package com.smg.meg.service

import com.smg.meg.controller.SubscriptionRequest
import com.smg.meg.controller.UpdateSubscriptionRequest
import com.smg.meg.model.document.PendingMessage
import com.smg.meg.model.document.Topic as TopicDocument
import com.smg.meg.model.document.Subscription
import com.smg.meg.repository.PendingMessageRepository
import com.smg.meg.repository.TopicRepository
import com.smg.meg.repository.SubscriptionRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.Optional

@ExtendWith(MockitoExtension::class)
class SubscriptionServiceTest {

    @Mock
    private lateinit var subscriptionRepository: SubscriptionRepository

    @Mock
    private lateinit var pendingMessageRepository: PendingMessageRepository

    @Mock
    private lateinit var topicRepository: TopicRepository

    @Mock
    private lateinit var rabbitAdmin: RabbitAdmin

    @Mock
    private lateinit var rabbitTemplate: RabbitTemplate

    @Mock
    private lateinit var confResilienceService: ConfResilienceService

    private lateinit var subscriptionService: SubscriptionService

    @BeforeEach
    fun setUp() {
        subscriptionService = SubscriptionService(
            subscriptionRepository,
            topicRepository,
            rabbitAdmin,
            rabbitTemplate,
            pendingMessageRepository,
            confResilienceService,
            10L,
            10
        )
    }

    @Captor
    private lateinit var queueCaptor: ArgumentCaptor<Queue>

    @Captor
    private lateinit var bindingCaptor: ArgumentCaptor<Binding>

    @Test
    fun `createSubscription fails if same name already exists`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                TopicDocument(
                    id = "reintegros",
                    version = 2,
                    description = "Cola",
                    ownerApp = "finanzas",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )
        whenever(subscriptionRepository.existsById("reintegros-v2-procesador-v2")).thenReturn(true)
        val request = SubscriptionRequest("reintegros", 2, "procesador-v2", "Sub", "PULL", null, 10, null)

        val ex = assertThrows(ResponseStatusException::class.java) {
            subscriptionService.createSubscription("reintegros", request)
        }

        assertEquals(HttpStatus.CONFLICT, ex.statusCode)
        verify(subscriptionRepository, never()).save(Mockito.any(Subscription::class.java))
    }

    @Test
    fun `createSubscription uses deterministic id and declares queues`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                TopicDocument(
                    id = "reintegros",
                    version = 2,
                    description = "Cola",
                    ownerApp = "finanzas",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )
        val request = SubscriptionRequest("reintegros", 2, "procesador-v2", "Sub", "PULL", null, 10, null)
        whenever(subscriptionRepository.existsById("reintegros-v2-procesador-v2")).thenReturn(false)
        whenever(subscriptionRepository.save(Mockito.any(Subscription::class.java))).thenAnswer { it.arguments[0] }

        val result = subscriptionService.createSubscription("reintegros", request)

        assertEquals("reintegros-v2-procesador-v2", result.id)
        verify(rabbitAdmin, times(2)).declareQueue(queueCaptor.capture())
        verify(rabbitAdmin).declareBinding(bindingCaptor.capture())
    }

    @Test
    fun `ack processed removes only selected pending message`() {
        val subId = "reintegros-v2-procesador-v2"
        val token = "token-ok"
        val subscription = Subscription(
            id = subId,
            topicId = "reintegros",
            topicVersion = 2,
            nameSub = "procesador-v2",
            type = "PULL",
            token = token,
            mainQueue = "q.reintegros.v2.procesador-v2",
            dlq = "q.reintegros.v2.procesador-v2.dlq"
        )
        val pending = PendingMessage(
            id = "$subId:MAIN:msg_1",
            subscriptionId = subId,
            messageId = "msg_1",
            bucket = "MAIN",
            payload = mapOf("header" to mapOf("messageId" to "msg_1"))
        )
        whenever(subscriptionRepository.findByIdAndToken(subId, token)).thenReturn(Optional.of(subscription))
        whenever(
            pendingMessageRepository.findBySubscriptionIdAndMessageIdAndBucket(
                subId,
                "msg_1",
                "MAIN"
            )
        ).thenReturn(pending)

        subscriptionService.processAck(subId, "msg_1", token, "PROCESSED")

        verify(pendingMessageRepository).deleteById("$subId:MAIN:msg_1")
    }

    @Test
    fun `dead letter requeue returns message to main queue`() {
        val subId = "reintegros-v2-procesador-v2"
        val token = "token-ok"
        val subscription = Subscription(
            id = subId,
            topicId = "reintegros",
            topicVersion = 2,
            nameSub = "procesador-v2",
            type = "PULL",
            token = token,
            mainQueue = "q.reintegros.v2.procesador-v2",
            dlq = "q.reintegros.v2.procesador-v2.dlq"
        )
        val payload = mapOf("header" to mapOf("messageId" to "msg_1"), "payload" to mapOf("monto" to 100))
        val pendingDlq = PendingMessage(
            id = "$subId:DLQ:msg_1",
            subscriptionId = subId,
            messageId = "msg_1",
            bucket = "DLQ",
            payload = payload
        )
        whenever(subscriptionRepository.findByIdAndToken(subId, token)).thenReturn(Optional.of(subscription))
        whenever(
            pendingMessageRepository.findBySubscriptionIdAndMessageIdAndBucket(
                subId,
                "msg_1",
                "DLQ"
            )
        ).thenReturn(pendingDlq)

        subscriptionService.requeue(subId, "msg_1", token)

        verify(rabbitTemplate).convertAndSend("", subscription.mainQueue, payload)
        verify(pendingMessageRepository).deleteById("$subId:DLQ:msg_1")
        assertTrue(true)
    }

    @Test
    fun `updateSubscription allows description status and url for push`() {
        val existing = Subscription(
            id = "reintegros-v1-push",
            topicId = "reintegros",
            topicVersion = 1,
            nameSub = "push",
            description = "old",
            type = "PUSH",
            token = "token",
            urlRest = "http://old",
            mainQueue = "q.main",
            dlq = "q.dlq"
        )
        whenever(subscriptionRepository.findById(existing.id)).thenReturn(Optional.of(existing))
        whenever(subscriptionRepository.save(Mockito.any(Subscription::class.java))).thenAnswer { it.arguments[0] }

        val updated = subscriptionService.updateSubscription(
            existing.id,
            UpdateSubscriptionRequest(
                description = "new",
                status = "INACTIVE",
                urlRest = "http://new-webhook",
                maxDeliveryCountPull = 7
            )
        )

        assertEquals("new", updated.description)
        assertEquals("INACTIVE", updated.status)
        assertEquals("http://new-webhook", updated.urlRest)
        assertEquals(7, updated.maxDeliveryCountPull)
    }

    @Test
    fun `updateSubscription rejects urlRest for pull`() {
        val existing = Subscription(
            id = "reintegros-v1-pull",
            topicId = "reintegros",
            topicVersion = 1,
            nameSub = "pull",
            description = "old",
            type = "PULL",
            token = "token",
            urlRest = null,
            mainQueue = "q.main",
            dlq = "q.dlq"
        )
        whenever(subscriptionRepository.findById(existing.id)).thenReturn(Optional.of(existing))

        val ex = assertThrows(ResponseStatusException::class.java) {
            subscriptionService.updateSubscription(
                existing.id,
                UpdateSubscriptionRequest(
                    description = "new",
                    status = "ACTIVE",
                    urlRest = "http://should-not-be-set",
                    maxDeliveryCountPull = null
                )
            )
        }

        assertEquals(HttpStatus.BAD_REQUEST, ex.statusCode)
    }

    @Test
    fun `createSubscription normalizes topic and subscription names to lowercase`() {
        whenever(topicRepository.findById("solicitudes-reintegros")).thenReturn(
            Optional.of(
                TopicDocument(
                    id = "solicitudes-reintegros",
                    version = 1,
                    description = "Cola",
                    ownerApp = "finanzas",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.solicitudes-reintegros"
                )
            )
        )
        val request = SubscriptionRequest(
            "Solicitudes-Reintegros",
            1,
            "Pago-Reintegros-Grandes",
            "Sub",
            "PULL",
            null,
            10,
            null
        )
        whenever(subscriptionRepository.existsById("solicitudes-reintegros-v1-pago-reintegros-grandes")).thenReturn(false)
        whenever(subscriptionRepository.save(Mockito.any(Subscription::class.java))).thenAnswer { it.arguments[0] }

        val result = subscriptionService.createSubscription("Solicitudes-Reintegros", request)

        assertEquals("solicitudes-reintegros-v1-pago-reintegros-grandes", result.id)
        assertEquals("solicitudes-reintegros", result.topicId)
        assertEquals("pago-reintegros-grandes", result.nameSub)
        assertEquals("q.solicitudes-reintegros.v1.pago-reintegros-grandes", result.mainQueue)
    }

    @Test
    fun `createSubscription applies global maxDeliveryCountPull default when omitted`() {
        whenever(topicRepository.findById("reintegros")).thenReturn(
            Optional.of(
                TopicDocument(
                    id = "reintegros",
                    version = 1,
                    description = "Cola",
                    ownerApp = "finanzas",
                    maxBodyBytes = 65536,
                    rabbitExchange = "ex.reintegros"
                )
            )
        )
        val request = SubscriptionRequest("reintegros", 1, "procesador", "Sub", "PULL", null, 10, null)
        whenever(subscriptionRepository.existsById("reintegros-v1-procesador")).thenReturn(false)
        whenever(subscriptionRepository.save(Mockito.any(Subscription::class.java))).thenAnswer { it.arguments[0] }

        val result = subscriptionService.createSubscription("reintegros", request)

        assertEquals(10, result.maxDeliveryCountPull)
    }

    @Test
    fun `pullMessages moves message to dlq when maxDeliveryCountPull is reached`() {
        val subId = "reintegros-v1-procesador"
        val token = "token-ok"
        val subscription = Subscription(
            id = subId,
            topicId = "reintegros",
            topicVersion = 1,
            nameSub = "procesador",
            type = "PULL",
            token = token,
            maxDeliveryCountPull = 2,
            mainQueue = "q.reintegros.v1.procesador",
            dlq = "q.reintegros.v1.procesador.dlq"
        )
        val payload = mapOf("header" to mapOf("messageId" to "msg_1"), "payload" to mapOf("monto" to 100))
        val pending = PendingMessage(
            id = "$subId:MAIN:msg_1",
            subscriptionId = subId,
            messageId = "msg_1",
            bucket = "MAIN",
            payload = payload,
            deliveryCount = 2
        )
        whenever(subscriptionRepository.findByIdAndToken(subId, token)).thenReturn(Optional.of(subscription))
        whenever(pendingMessageRepository.findBySubscriptionIdAndBucketOrderByCreatedAtAsc(subId, "MAIN")).thenReturn(listOf(pending))
        whenever(rabbitTemplate.receiveAndConvert(subscription.mainQueue)).thenReturn(null)

        val result = subscriptionService.pullMessages(subId, 0, 10, token)

        assertTrue(result.isEmpty())
        verify(rabbitTemplate).convertAndSend(Mockito.eq(""), Mockito.eq(subscription.dlq), Mockito.any(Map::class.java))
        verify(pendingMessageRepository).deleteById(pending.id)
    }
}
