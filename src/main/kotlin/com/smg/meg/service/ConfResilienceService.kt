package com.smg.meg.service

import com.smg.meg.model.document.BulkheadConf
import com.smg.meg.model.document.CircuitBreakerConf
import com.smg.meg.model.document.ConfResilience
import com.smg.meg.model.document.RetryConf
import com.smg.meg.model.document.Subscription
import com.smg.meg.repository.ConfResilienceRepository
import com.smg.meg.repository.SubscriptionRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@Service
class ConfResilienceService(
    private val confResilienceRepository: ConfResilienceRepository,
    private val subscriptionRepository: SubscriptionRepository,
    @Value("\${meg.push.conf-resilience.default.enabled:true}")
    private val defaultEnabled: Boolean,
    @Value("\${meg.push.conf-resilience.default.timeout-ms:2000}")
    private val defaultTimeoutMs: Long,
    @Value("\${meg.push.conf-resilience.default.retry.max-attempts:3}")
    private val defaultRetryMaxAttempts: Int,
    @Value("\${meg.push.conf-resilience.default.retry.initial-backoff-ms:500}")
    private val defaultRetryInitialBackoffMs: Long,
    @Value("\${meg.push.conf-resilience.default.retry.multiplier:2.0}")
    private val defaultRetryMultiplier: Double,
    @Value("\${meg.push.conf-resilience.default.retry.max-backoff-ms:5000}")
    private val defaultRetryMaxBackoffMs: Long,
    @Value("\${meg.push.conf-resilience.default.retry.jitter-factor:0.2}")
    private val defaultRetryJitterFactor: Double,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.failure-rate-threshold:50}")
    private val defaultCbFailureRateThreshold: Float,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.slow-call-rate-threshold:70}")
    private val defaultCbSlowCallRateThreshold: Float,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.slow-call-duration-ms:2000}")
    private val defaultCbSlowCallDurationMs: Long,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.minimum-number-of-calls:10}")
    private val defaultCbMinimumNumberOfCalls: Int,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.sliding-window-size:20}")
    private val defaultCbSlidingWindowSize: Int,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.wait-duration-in-open-state-ms:30000}")
    private val defaultCbWaitDurationOpenMs: Long,
    @Value("\${meg.push.conf-resilience.default.circuit-breaker.permitted-calls-in-half-open-state:3}")
    private val defaultCbPermittedHalfOpenCalls: Int,
    @Value("\${meg.push.conf-resilience.default.bulkhead.max-concurrent-calls:10}")
    private val defaultBulkheadMaxConcurrentCalls: Int,
    @Value("\${meg.push.conf-resilience.default.bulkhead.max-wait-duration-ms:0}")
    private val defaultBulkheadMaxWaitDurationMs: Long,
    @Value("\${meg.push.conf-resilience.status.pause-open-threshold-ms:30000}")
    private val pauseOpenThresholdMs: Long,
    @Value("\${meg.push.conf-resilience.status.recover-closed-threshold-ms:10000}")
    private val recoverClosedThresholdMs: Long
) {
    private val cache = ConcurrentHashMap<String, ConfResilience>()
    private val openSince = ConcurrentHashMap<String, Instant>()
    private val closedSince = ConcurrentHashMap<String, Instant>()

    fun ensureForPushSubscription(subscription: Subscription): ConfResilience {
        if (!subscription.type.equals("PUSH", ignoreCase = true)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "conf-resilience applies only to PUSH subscriptions")
        }
        val existing = confResilienceRepository.findBySubscriptionId(subscription.id).orElse(null)
        if (existing != null) {
            cache[subscription.id] = existing
            return existing
        }
        val created = confResilienceRepository.save(defaultFor(subscription.id, "system"))
        cache[subscription.id] = created
        return created
    }

    fun getForSubscription(subscriptionId: String): ConfResilience {
        val subscription = requirePushSubscription(subscriptionId)
        return cache[subscription.id] ?: ensureForPushSubscription(subscription)
    }

    fun updateForSubscription(subscriptionId: String, request: ConfResilienceRequest): ConfResilience {
        val subscription = requirePushSubscription(subscriptionId)
        validate(request)
        val current = getForSubscription(subscription.id)
        val updated = current.copy(
            enabled = request.enabled,
            timeoutMs = request.timeoutMs,
            retry = RetryConf(
                maxAttempts = request.retry.maxAttempts,
                initialBackoffMs = request.retry.initialBackoffMs,
                multiplier = request.retry.multiplier,
                maxBackoffMs = request.retry.maxBackoffMs,
                jitterFactor = request.retry.jitterFactor
            ),
            circuitBreaker = CircuitBreakerConf(
                failureRateThreshold = request.circuitBreaker.failureRateThreshold,
                slowCallRateThreshold = request.circuitBreaker.slowCallRateThreshold,
                slowCallDurationMs = request.circuitBreaker.slowCallDurationMs,
                minimumNumberOfCalls = request.circuitBreaker.minimumNumberOfCalls,
                slidingWindowSize = request.circuitBreaker.slidingWindowSize,
                waitDurationInOpenStateMs = request.circuitBreaker.waitDurationInOpenStateMs,
                permittedCallsInHalfOpenState = request.circuitBreaker.permittedCallsInHalfOpenState
            ),
            bulkhead = BulkheadConf(
                maxConcurrentCalls = request.bulkhead.maxConcurrentCalls,
                maxWaitDurationMs = request.bulkhead.maxWaitDurationMs
            ),
            updatedAt = Instant.now(),
            updatedBy = request.updatedBy ?: "api"
        )
        val persisted = confResilienceRepository.save(updated)
        cache[subscription.id] = persisted
        return persisted
    }

    fun resetForSubscription(subscriptionId: String): ConfResilience {
        val subscription = requirePushSubscription(subscriptionId)
        val current = getForSubscription(subscription.id)
        val reset = defaultFor(subscription.id, "api-reset").copy(id = current.id, createdAt = current.createdAt)
        val persisted = confResilienceRepository.save(reset)
        cache[subscription.id] = persisted
        return persisted
    }

    fun markCircuitOpen(subscriptionId: String) {
        val now = Instant.now()
        openSince.putIfAbsent(subscriptionId, now)
        closedSince.remove(subscriptionId)
        val openAt = openSince[subscriptionId] ?: return
        if (now.toEpochMilli() - openAt.toEpochMilli() < pauseOpenThresholdMs) return
        val subscription = subscriptionRepository.findById(subscriptionId).orElse(null) ?: return
        if (subscription.status == "PAUSED_BY_SYSTEM") return
        subscriptionRepository.save(subscription.copy(status = "PAUSED_BY_SYSTEM"))
    }

    fun markCircuitClosed(subscriptionId: String) {
        val now = Instant.now()
        closedSince.putIfAbsent(subscriptionId, now)
        openSince.remove(subscriptionId)
        val closedAt = closedSince[subscriptionId] ?: return
        if (now.toEpochMilli() - closedAt.toEpochMilli() < recoverClosedThresholdMs) return
        val subscription = subscriptionRepository.findById(subscriptionId).orElse(null) ?: return
        if (subscription.status != "PAUSED_BY_SYSTEM") return
        subscriptionRepository.save(subscription.copy(status = "ACTIVE"))
    }

    private fun defaultFor(subscriptionId: String, updatedBy: String): ConfResilience =
        ConfResilience(
            id = subscriptionId,
            subscriptionId = subscriptionId,
            enabled = defaultEnabled,
            timeoutMs = defaultTimeoutMs,
            retry = RetryConf(
                maxAttempts = defaultRetryMaxAttempts,
                initialBackoffMs = defaultRetryInitialBackoffMs,
                multiplier = defaultRetryMultiplier,
                maxBackoffMs = defaultRetryMaxBackoffMs,
                jitterFactor = defaultRetryJitterFactor
            ),
            circuitBreaker = CircuitBreakerConf(
                failureRateThreshold = defaultCbFailureRateThreshold,
                slowCallRateThreshold = defaultCbSlowCallRateThreshold,
                slowCallDurationMs = defaultCbSlowCallDurationMs,
                minimumNumberOfCalls = defaultCbMinimumNumberOfCalls,
                slidingWindowSize = defaultCbSlidingWindowSize,
                waitDurationInOpenStateMs = defaultCbWaitDurationOpenMs,
                permittedCallsInHalfOpenState = defaultCbPermittedHalfOpenCalls
            ),
            bulkhead = BulkheadConf(
                maxConcurrentCalls = defaultBulkheadMaxConcurrentCalls,
                maxWaitDurationMs = defaultBulkheadMaxWaitDurationMs
            ),
            updatedBy = updatedBy
        )

    private fun requirePushSubscription(subscriptionId: String): Subscription {
        val subscription = subscriptionRepository.findById(subscriptionId).orElseThrow {
            ResponseStatusException(HttpStatus.NOT_FOUND, "Subscription '$subscriptionId' not found")
        }
        if (!subscription.type.equals("PUSH", ignoreCase = true)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Subscription '$subscriptionId' is not PUSH")
        }
        return subscription
    }

    private fun validate(request: ConfResilienceRequest) {
        if (request.timeoutMs <= 0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "timeoutMs must be > 0")
        if (request.retry.maxAttempts < 1) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "retry.maxAttempts must be >= 1")
        if (request.retry.multiplier < 1.0) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "retry.multiplier must be >= 1.0")
        if (request.circuitBreaker.failureRateThreshold !in 1f..100f) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "circuitBreaker.failureRateThreshold must be between 1 and 100")
        if (request.circuitBreaker.slowCallRateThreshold !in 1f..100f) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "circuitBreaker.slowCallRateThreshold must be between 1 and 100")
        if (request.circuitBreaker.minimumNumberOfCalls > request.circuitBreaker.slidingWindowSize) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "circuitBreaker.minimumNumberOfCalls must be <= slidingWindowSize")
        }
        if (request.bulkhead.maxConcurrentCalls < 1) throw ResponseStatusException(HttpStatus.BAD_REQUEST, "bulkhead.maxConcurrentCalls must be >= 1")
    }
}

data class ConfResilienceRequest(
    val enabled: Boolean,
    val timeoutMs: Long,
    val retry: RetryRequest,
    val circuitBreaker: CircuitBreakerRequest,
    val bulkhead: BulkheadRequest,
    val updatedBy: String? = null
)

data class RetryRequest(
    val maxAttempts: Int,
    val initialBackoffMs: Long,
    val multiplier: Double,
    val maxBackoffMs: Long,
    val jitterFactor: Double
)

data class CircuitBreakerRequest(
    val failureRateThreshold: Float,
    val slowCallRateThreshold: Float,
    val slowCallDurationMs: Long,
    val minimumNumberOfCalls: Int,
    val slidingWindowSize: Int,
    val waitDurationInOpenStateMs: Long,
    val permittedCallsInHalfOpenState: Int
)

data class BulkheadRequest(
    val maxConcurrentCalls: Int,
    val maxWaitDurationMs: Long
)

