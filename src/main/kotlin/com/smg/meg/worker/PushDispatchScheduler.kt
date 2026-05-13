package com.smg.meg.worker

import com.smg.meg.model.document.ConfResilience
import com.smg.meg.model.document.Subscription
import com.smg.meg.repository.SubscriptionRepository
import com.smg.meg.service.ConfResilienceService
import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageBuilder
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.http.client.ClientHttpResponse
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.client.DefaultResponseErrorHandler
import org.springframework.web.client.RestTemplate
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.pow
import kotlin.random.Random

@Component
class PushDispatchScheduler(
    private val subscriptionRepository: SubscriptionRepository,
    private val rabbitTemplate: RabbitTemplate,
    private val confResilienceService: ConfResilienceService,
    private val meterRegistry: MeterRegistry,
    restTemplateBuilder: RestTemplateBuilder,
    @Value("\${meg.push.max-delivery-attempts:0}")
    private val maxDeliveryAttemptsConfig: Int
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val restTemplate: RestTemplate = restTemplateBuilder
        .errorHandler(
            object : DefaultResponseErrorHandler() {
                override fun hasError(response: ClientHttpResponse): Boolean = false
            }
        )
        .build()
    private val bulkheadRegistry = BulkheadRegistry.ofDefaults()
    private val circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
    private val ioExecutor = Executors.newCachedThreadPool()

    @Scheduled(fixedDelayString = "\${meg.push.dispatch-interval-ms:2000}")
    fun dispatchPushMessages() {
        val pushSubs = subscriptionRepository.findByTypeAndStatusIn("PUSH", listOf("ACTIVE", "PAUSED_BY_SYSTEM"))
        pushSubs.forEach { sub ->
            val targetUrl = sub.urlRest ?: return@forEach
            val conf = confResilienceService.getForSubscription(sub.id)
            if (!conf.enabled) return@forEach
            val maxAttempts = maxAttemptsFor(sub, conf)
            var delivered = 0
            while (true) {
                val raw = rabbitTemplate.receive(sub.mainQueue, RECEIVE_TIMEOUT_MS) ?: break
                val attempt = (raw.messageProperties.headers?.get(ATTEMPT_HEADER) as? Number)?.toInt() ?: 1
                val payload = runCatching { rabbitTemplate.messageConverter.fromMessage(raw) }.getOrNull()
                if (payload == null) {
                    handlePushFailure(
                        raw,
                        sub,
                        attempt,
                        maxAttempts,
                        IllegalStateException("Could not convert AMQP message body for push"),
                        targetUrl
                    )
                    continue
                }
                val result = runCatching {
                    dispatchWithResilience(sub, conf, targetUrl, payload)
                }
                val failure = result.exceptionOrNull()
                if (failure != null) {
                    when (failure) {
                        is PushPermanentException -> {
                            incrementCounter(METRIC_DLQ, sub.id, OUTCOME_4XX_DIRECT)
                            sendToDlq(sub, payload, failure.message ?: "4xx from push target")
                            logPushOutcome(sub.id, payload, attempt, OUTCOME_4XX_DIRECT, failure.message)
                        }
                        is CallNotPermittedException -> {
                            confResilienceService.markCircuitOpen(sub.id)
                            incrementCounter(METRIC_CIRCUIT_OPEN, sub.id)
                            handlePushFailure(raw, sub, attempt, maxAttempts, failure, targetUrl)
                        }
                        else -> {
                            handlePushFailure(raw, sub, attempt, maxAttempts, failure, targetUrl)
                        }
                    }
                    continue
                }
                val response = result.getOrThrow()
                if (response.statusCode.is2xxSuccessful) {
                    confResilienceService.markCircuitClosed(sub.id)
                    incrementCounter(METRIC_DELIVERIES_OK, sub.id)
                    logPushOutcome(sub.id, payload, attempt, OUTCOME_DELIVERED, "HTTP ${response.statusCode.value()}")
                    delivered++
                } else if (response.statusCode.is4xxClientError) {
                    incrementCounter(METRIC_DLQ, sub.id, OUTCOME_4XX_DIRECT)
                    sendToDlq(
                        sub,
                        payload,
                        "HTTP ${response.statusCode.value()} from push target (4xx: no retry)"
                    )
                    logPushOutcome(sub.id, payload, attempt, OUTCOME_4XX_DIRECT, "HTTP ${response.statusCode.value()}")
                } else {
                    handlePushFailure(
                        raw,
                        sub,
                        attempt,
                        maxAttempts,
                        RuntimeException("HTTP ${response.statusCode.value()} from push target"),
                        targetUrl
                    )
                }
            }
            if (delivered > 0) {
                log.info("Delivered {} push messages for subscription {}", delivered, sub.id)
            }
        }
    }

    private fun maxAttemptsFor(sub: Subscription, conf: ConfResilience): Int =
        if (maxDeliveryAttemptsConfig > 0) maxDeliveryAttemptsConfig.coerceAtLeast(1)
        else conf.retry.maxAttempts.coerceAtLeast(sub.maxRetries.coerceAtLeast(1))

    private fun dispatchWithResilience(
        sub: Subscription,
        conf: ConfResilience,
        targetUrl: String,
        payload: Any
    ) = retryTransient(conf) {
        val circuitBreaker = resolveCircuitBreaker(sub.id, conf)
        val bulkhead = resolveBulkhead(sub.id, conf)
        val guarded = Bulkhead.decorateSupplier(bulkhead) {
            CircuitBreaker.decorateSupplier(circuitBreaker) {
                val future = CompletableFuture.supplyAsync(
                    { restTemplate.postForEntity(targetUrl, payload, String::class.java) },
                    ioExecutor
                )
                future.get(conf.timeoutMs, TimeUnit.MILLISECONDS)
            }.get()
        }
        val response = guarded.get()
        if (response.statusCode.is4xxClientError) {
            throw PushPermanentException("HTTP ${response.statusCode.value()} from push target (4xx: no retry)")
        }
        if (!response.statusCode.is2xxSuccessful) {
            throw RuntimeException("HTTP ${response.statusCode.value()} from push target")
        }
        response
    }

    private fun <T> retryTransient(conf: ConfResilience, block: () -> T): T {
        var attempt = 1
        var last: Throwable? = null
        while (attempt <= conf.retry.maxAttempts) {
            try {
                return block()
            } catch (ex: Throwable) {
                if (ex is PushPermanentException) throw ex
                last = ex
                if (attempt >= conf.retry.maxAttempts) break
                Thread.sleep(backoffMs(conf, attempt))
                attempt++
            }
        }
        throw last ?: RuntimeException("push retry exhausted")
    }

    private fun backoffMs(conf: ConfResilience, attempt: Int): Long {
        val exp = conf.retry.initialBackoffMs * conf.retry.multiplier.pow((attempt - 1).toDouble())
        val capped = minOf(exp.toLong(), conf.retry.maxBackoffMs)
        val jitterSpan = (capped * conf.retry.jitterFactor).toLong().coerceAtLeast(1)
        return (capped - jitterSpan) + Random.nextLong(jitterSpan * 2)
    }

    private fun resolveCircuitBreaker(subscriptionId: String, conf: ConfResilience): CircuitBreaker {
        val cbConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(conf.circuitBreaker.failureRateThreshold)
            .slowCallRateThreshold(conf.circuitBreaker.slowCallRateThreshold)
            .slowCallDurationThreshold(Duration.ofMillis(conf.circuitBreaker.slowCallDurationMs))
            .minimumNumberOfCalls(conf.circuitBreaker.minimumNumberOfCalls)
            .slidingWindowSize(conf.circuitBreaker.slidingWindowSize)
            .waitDurationInOpenState(Duration.ofMillis(conf.circuitBreaker.waitDurationInOpenStateMs))
            .permittedNumberOfCallsInHalfOpenState(conf.circuitBreaker.permittedCallsInHalfOpenState)
            .build()
        return circuitBreakerRegistry.circuitBreaker("push-$subscriptionId", cbConfig)
    }

    private fun resolveBulkhead(subscriptionId: String, conf: ConfResilience): Bulkhead {
        val bulkheadConfig = BulkheadConfig.custom()
            .maxConcurrentCalls(conf.bulkhead.maxConcurrentCalls)
            .maxWaitDuration(Duration.ofMillis(conf.bulkhead.maxWaitDurationMs))
            .build()
        return bulkheadRegistry.bulkhead("push-$subscriptionId", bulkheadConfig)
    }

    private fun sendToDlq(sub: Subscription, payload: Any, reason: String) {
        rabbitTemplate.convertAndSend("", sub.dlq, payload)
        val ctx = extractMessageContext(payload)
        log.warn(
            "push_outcome subscriptionId={} messageId={} correlationId={} attempt={} outcome={} dlq={} reason={}",
            sub.id,
            ctx.messageId,
            ctx.correlationId,
            0,
            OUTCOME_DLQ,
            sub.dlq,
            reason
        )
    }

    private fun handlePushFailure(
        raw: Message,
        sub: Subscription,
        attempt: Int,
        maxAttempts: Int,
        ex: Throwable,
        targetUrl: String
    ) {
        if (attempt >= maxAttempts) {
            val payloadForDlq = runCatching { rabbitTemplate.messageConverter.fromMessage(raw) }.getOrNull()
            if (payloadForDlq != null) {
                rabbitTemplate.convertAndSend("", sub.dlq, payloadForDlq)
                logPushOutcome(sub.id, payloadForDlq, attempt, OUTCOME_DLQ, ex.message)
            } else {
                rabbitTemplate.send("", sub.dlq, raw)
                logPushOutcome(sub.id, null, attempt, OUTCOME_DLQ, ex.message)
            }
            incrementCounter(METRIC_DELIVERIES_FAILED, sub.id)
            incrementCounter(METRIC_DLQ, sub.id, OUTCOME_RETRY_EXHAUSTED)
            log.error(
                "Push failed after {} attempt(s) for subscription {} to {} — message sent to DLQ {}",
                maxAttempts,
                sub.id,
                targetUrl,
                sub.dlq,
                ex
            )
            return
        }
        val next = MessageBuilder.fromMessage(raw).setHeader(ATTEMPT_HEADER, attempt + 1).build()
        rabbitTemplate.send("", sub.mainQueue, next)
        incrementCounter(METRIC_RETRIES, sub.id)
        incrementCounter(METRIC_DELIVERIES_FAILED, sub.id)
        val messageContext = extractMessageContext(runCatching { rabbitTemplate.messageConverter.fromMessage(raw) }.getOrNull())
        log.warn(
            "push_outcome subscriptionId={} messageId={} correlationId={} attempt={} outcome={} maxAttempts={} targetUrl={} error={}",
            sub.id,
            messageContext.messageId,
            messageContext.correlationId,
            attempt,
            OUTCOME_RETRY_SCHEDULED,
            maxAttempts,
            targetUrl,
            ex.message
        )
    }

    private fun incrementCounter(metric: String, subscriptionId: String, outcome: String? = null) {
        val builder = io.micrometer.core.instrument.Counter.builder(metric)
            .tag("subscriptionId", subscriptionId)
        if (outcome != null) {
            builder.tag("outcome", outcome)
        }
        builder.register(meterRegistry).increment()
    }

    private fun logPushOutcome(
        subscriptionId: String,
        payload: Any?,
        attempt: Int,
        outcome: String,
        detail: String?
    ) {
        val ctx = extractMessageContext(payload)
        log.info(
            "push_outcome subscriptionId={} messageId={} correlationId={} attempt={} outcome={} detail={}",
            subscriptionId,
            ctx.messageId,
            ctx.correlationId,
            attempt,
            outcome,
            detail
        )
    }

    private fun extractMessageContext(payload: Any?): PushMessageContext {
        val mapPayload = payload as? Map<*, *> ?: return PushMessageContext()
        val header = mapPayload["header"] as? Map<*, *> ?: return PushMessageContext()
        return PushMessageContext(
            messageId = header["messageId"]?.toString(),
            correlationId = header["correlationId"]?.toString()
        )
    }

    companion object {
        private const val ATTEMPT_HEADER = "x-meg-push-attempt"
        private const val RECEIVE_TIMEOUT_MS = 200L
        // Nombres Micrometer (punto); en scrape Prometheus aparecen como meg_push_*_total
        private const val METRIC_DELIVERIES_OK = "meg.push.deliveries.ok"
        private const val METRIC_DELIVERIES_FAILED = "meg.push.deliveries.failed"
        private const val METRIC_RETRIES = "meg.push.retries"
        private const val METRIC_DLQ = "meg.push.dlq"
        private const val METRIC_CIRCUIT_OPEN = "meg.push.circuit.open"
        private const val OUTCOME_DELIVERED = "delivered"
        private const val OUTCOME_RETRY_SCHEDULED = "retry_scheduled"
        private const val OUTCOME_DLQ = "dlq"
        private const val OUTCOME_4XX_DIRECT = "4xx_direct_dlq"
        private const val OUTCOME_RETRY_EXHAUSTED = "retry_exhausted_dlq"
    }
}

private class PushPermanentException(message: String) : RuntimeException(message)

private data class PushMessageContext(
    val messageId: String? = null,
    val correlationId: String? = null
)
