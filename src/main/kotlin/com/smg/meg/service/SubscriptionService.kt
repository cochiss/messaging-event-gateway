package com.smg.meg.service

import com.smg.meg.controller.SubscriptionRequest
import com.smg.meg.controller.UpdateSubscriptionRequest
import com.smg.meg.model.document.PendingMessage
import com.smg.meg.model.document.Subscription
import com.smg.meg.repository.PendingMessageRepository
import com.smg.meg.repository.TopicRepository
import com.smg.meg.repository.SubscriptionRepository
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.QueueBuilder
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.time.Instant
import java.util.Locale
import java.util.UUID

@Service
class SubscriptionService(
    private val repository: SubscriptionRepository,
    private val topicRepository: TopicRepository,
    private val rabbitAdmin: RabbitAdmin,
    private val rabbitTemplate: RabbitTemplate,
    private val pendingMessageRepository: PendingMessageRepository,
    private val confResilienceService: ConfResilienceService,
    @Value("\${meg.pull.visibility-timeout-seconds:10}")
    private val visibilityTimeoutSeconds: Long,
    @Value("\${meg.pull.max-delivery-count-default:10}")
    private val maxDeliveryCountPullDefault: Int
) {
    fun createSubscription(topicId: String, request: SubscriptionRequest): Subscription {
        val normalizedTopicId = topicId.trim().lowercase(Locale.ROOT)
        val normalizedNameSub = request.nameSub.trim().lowercase(Locale.ROOT)
        val topic = topicRepository.findById(normalizedTopicId).orElseThrow {
            ResponseStatusException(HttpStatus.NOT_FOUND, "Topic '$normalizedTopicId' not found")
        }
        if (request.topicVersion != topic.version) {
            throw ResponseStatusException(
                HttpStatus.BAD_REQUEST,
                "Topic '$normalizedTopicId' current version is ${topic.version}, requested subscription version ${request.topicVersion}"
            )
        }

        val subId = "$normalizedTopicId-v${request.topicVersion}-$normalizedNameSub"
        if (repository.existsById(subId)) {
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Subscription '$normalizedNameSub' already exists for topic '$normalizedTopicId'"
            )
        }

        val mainQueueName = "q.$normalizedTopicId.v${request.topicVersion}.$normalizedNameSub"
        val dlqName = "$mainQueueName.dlq"
        val exchangeName = "ex.$normalizedTopicId"

        val dlq = QueueBuilder.durable(dlqName).build()
        val mainQueue = QueueBuilder.durable(mainQueueName)
            .withArgument("x-dead-letter-exchange", "")
            .withArgument("x-dead-letter-routing-key", dlqName)
            .build()

        rabbitAdmin.declareQueue(dlq)
        rabbitAdmin.declareQueue(mainQueue)
        val binding = BindingBuilder.bind(mainQueue).to(TopicExchange(exchangeName)).with("#")
        rabbitAdmin.declareBinding(binding)

        val subscription = Subscription(
            id = subId,
            topicId = normalizedTopicId,
            topicVersion = request.topicVersion,
            nameSub = normalizedNameSub,
            description = request.description,
            // Normalized: PushDispatchScheduler matches "PUSH" / "PULL" exactly.
            type = request.type.trim().uppercase(Locale.ROOT),
            token = UUID.randomUUID().toString(),
            urlRest = request.urlRest,
            maxRetries = request.maxRetries,
            maxDeliveryCountPull = (request.maxDeliveryCountPull ?: maxDeliveryCountPullDefault).coerceAtLeast(1),
            mainQueue = mainQueueName,
            dlq = dlqName
        )

        val created = repository.save(subscription)
        if (created.type.equals("PUSH", ignoreCase = true)) {
            confResilienceService.ensureForPushSubscription(created)
        }
        return created
    }

    fun listSubscriptions(topicId: String?, page: Int, size: Int): List<Subscription> {
        val normalizedTopicId = topicId?.trim()?.lowercase(Locale.ROOT)
        val source = if (normalizedTopicId.isNullOrBlank()) repository.findAll() else repository.findByTopicId(normalizedTopicId)
        return paginate(source, page, size)
    }

    fun updateSubscription(id: String, token: String, request: UpdateSubscriptionRequest): Subscription {
        val existing = requireSubscription(id, token)
        val normalizedStatus = request.status.trim().uppercase(Locale.ROOT)
        val normalizedType = existing.type.trim().uppercase(Locale.ROOT)
        val normalizedUrl = request.urlRest?.trim()
        if (normalizedType == "PUSH" && normalizedUrl.isNullOrBlank()) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "urlRest is required for PUSH subscriptions")
        }
        if (normalizedType == "PULL" && !normalizedUrl.isNullOrBlank()) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "urlRest must be null/blank for PULL subscriptions")
        }
        val updated = existing.copy(
            description = request.description,
            status = normalizedStatus,
            urlRest = normalizedUrl,
            maxDeliveryCountPull = (request.maxDeliveryCountPull ?: existing.maxDeliveryCountPull).coerceAtLeast(1)
        )
        return repository.save(updated)
    }

    fun pullMessages(id: String, page: Int, size: Int, token: String): List<Any> {
        val subscription = requireSubscription(id, token)
        val normalizedStatus = subscription.status.trim().uppercase(Locale.ROOT)
        if (normalizedStatus != "ACTIVE") {
            throw ResponseStatusException(HttpStatus.FORBIDDEN, "Subscription '$id' is $normalizedStatus")
        }
        if (!subscription.type.equals("PULL", ignoreCase = true)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Subscription '$id' is not configured as PULL")
        }
        val safePage = page.coerceAtLeast(0)
        val safeSize = size.coerceAtLeast(1)
        val needed = (safePage + 1) * safeSize
        val now = Instant.now()
        val visibilityUntil = now.plusSeconds(visibilityTimeoutSeconds)
        val pendingMain = pendingMessageRepository
            .findBySubscriptionIdAndBucketOrderByCreatedAtAsc(subscription.id, BUCKET_MAIN)
        val visiblePending = pendingMain.filter { isVisible(it, now) }
        val activePending = mutableListOf<PendingMessage>()
        visiblePending.forEach { pending ->
            if (pending.deliveryCount >= subscription.maxDeliveryCountPull.coerceAtLeast(1)) {
                rabbitTemplate.convertAndSend("", subscription.dlq, enrichPayloadWithDlqReason(pending.payload))
                pendingMessageRepository.deleteById(pending.id)
            } else {
                activePending.add(pending)
            }
        }
        val leasedFromActivePending = activePending
            .drop(safePage * safeSize)
            .take(safeSize)
            .map { it.copy(visibilityUntil = visibilityUntil, deliveryCount = it.deliveryCount + 1) }

        val messages = activePending.map { it.payload }.toMutableList()

        while (messages.size < needed) {
            val message = rabbitTemplate.receiveAndConvert(subscription.mainQueue) ?: break
            val messageId = resolveMessageId(message)
            savePending(subscription.id, messageId, BUCKET_MAIN, message, visibilityUntil, 1)
            messages.add(message)
        }

        if (leasedFromActivePending.isNotEmpty()) {
            pendingMessageRepository.saveAll(leasedFromActivePending)
        }

        return paginate(messages, safePage, safeSize)
    }

    fun processAck(id: String, messageId: String, token: String, action: String) {
        val subscription = requireSubscription(id, token)
        val pendingMessage = pendingMessageRepository.findBySubscriptionIdAndMessageIdAndBucket(
            subscription.id,
            messageId,
            BUCKET_MAIN
        ) ?: throw ResponseStatusException(
            HttpStatus.NOT_FOUND,
            "Message '$messageId' is not pending for subscription '$id'"
        )

        when (action.uppercase(Locale.getDefault())) {
            "PROCESSED" -> pendingMessageRepository.deleteById(pendingMessage.id)
            "REJECT" -> {
                rabbitTemplate.convertAndSend("", subscription.dlq, pendingMessage.payload)
                pendingMessageRepository.deleteById(pendingMessage.id)
            }
            else -> throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid action '$action'")
        }
    }

    fun getDeadLetters(id: String, page: Int, size: Int, token: String): List<Any> {
        val subscription = requireSubscription(id, token)
        val safePage = page.coerceAtLeast(0)
        val safeSize = size.coerceAtLeast(1)
        val needed = (safePage + 1) * safeSize
        val messages = pendingMessageRepository
            .findBySubscriptionIdAndBucketOrderByCreatedAtAsc(subscription.id, BUCKET_DLQ)
            .map { it.payload }
            .toMutableList()

        while (messages.size < needed) {
            val message = rabbitTemplate.receiveAndConvert(subscription.dlq) ?: break
            val messageId = resolveMessageId(message)
            savePending(subscription.id, messageId, BUCKET_DLQ, message)
            messages.add(message)
        }
        return paginate(messages, safePage, safeSize)
    }

    fun requeue(id: String, messageId: String, token: String) {
        val subscription = requireSubscription(id, token)
        val pendingDeadLetter = pendingMessageRepository.findBySubscriptionIdAndMessageIdAndBucket(
            subscription.id,
            messageId,
            BUCKET_DLQ
        ) ?: throw ResponseStatusException(
            HttpStatus.NOT_FOUND,
            "Dead letter '$messageId' is not pending for subscription '$id'"
        )

        rabbitTemplate.convertAndSend("", subscription.mainQueue, pendingDeadLetter.payload)
        pendingMessageRepository.deleteById(pendingDeadLetter.id)
    }

    private fun requireSubscription(id: String, token: String): Subscription =
        repository.findByIdAndToken(id, token)
            .orElseThrow { ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid subscription token") }

    private fun savePending(
        subscriptionId: String,
        messageId: String,
        bucket: String,
        payload: Any,
        visibilityUntil: Instant? = null,
        deliveryCount: Int = 0
    ) {
        val pendingId = "$subscriptionId:$bucket:$messageId"
        if (!pendingMessageRepository.existsById(pendingId)) {
            pendingMessageRepository.save(
                PendingMessage(
                    id = pendingId,
                    subscriptionId = subscriptionId,
                    messageId = messageId,
                    bucket = bucket,
                    payload = payload,
                    visibilityUntil = visibilityUntil,
                    deliveryCount = deliveryCount
                )
            )
        }
    }

    private fun isVisible(pendingMessage: PendingMessage, now: Instant): Boolean =
        pendingMessage.visibilityUntil == null || !pendingMessage.visibilityUntil.isAfter(now)

    private fun resolveMessageId(message: Any): String {
        val mapMessage = message as? Map<*, *> ?: return "msg_${UUID.randomUUID()}"
        val header = mapMessage["header"] as? Map<*, *>
        return (header?.get("messageId") as? String) ?: "msg_${UUID.randomUUID()}"
    }

    @Suppress("UNCHECKED_CAST")
    private fun enrichPayloadWithDlqReason(payload: Any): Any {
        val mapPayload = payload as? Map<String, Any?> ?: return payload
        val header = (mapPayload["header"] as? Map<String, Any?>)?.toMutableMap() ?: mutableMapOf()
        header["dlqReason"] = "max-delivery-exceeded"
        return mapPayload.toMutableMap().apply { put("header", header) }
    }

    private companion object {
        const val BUCKET_MAIN = "MAIN"
        const val BUCKET_DLQ = "DLQ"
    }

    private fun <T> paginate(items: List<T>, page: Int, size: Int): List<T> =
        items.drop(page * size).take(size)
}
