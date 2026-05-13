package com.smg.meg.model.document

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document("conf_resilience")
data class ConfResilience(
    @Id
    val id: String,
    val subscriptionId: String,
    val enabled: Boolean = true,
    val timeoutMs: Long,
    val retry: RetryConf,
    val circuitBreaker: CircuitBreakerConf,
    val bulkhead: BulkheadConf,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now(),
    val updatedBy: String = "system"
)

data class RetryConf(
    val maxAttempts: Int,
    val initialBackoffMs: Long,
    val multiplier: Double,
    val maxBackoffMs: Long,
    val jitterFactor: Double
)

data class CircuitBreakerConf(
    val failureRateThreshold: Float,
    val slowCallRateThreshold: Float,
    val slowCallDurationMs: Long,
    val minimumNumberOfCalls: Int,
    val slidingWindowSize: Int,
    val waitDurationInOpenStateMs: Long,
    val permittedCallsInHalfOpenState: Int
)

data class BulkheadConf(
    val maxConcurrentCalls: Int,
    val maxWaitDurationMs: Long
)

