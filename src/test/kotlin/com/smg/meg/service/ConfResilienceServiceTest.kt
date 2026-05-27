package com.smg.meg.service

import com.smg.meg.repository.ConfResilienceRepository
import com.smg.meg.repository.SubscriptionRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.whenever
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.util.Optional

@ExtendWith(MockitoExtension::class)
class ConfResilienceServiceTest {

    @Mock
    private lateinit var confResilienceRepository: ConfResilienceRepository

    @Mock
    private lateinit var subscriptionRepository: SubscriptionRepository

    @Test
    fun `updateForSubscription rejects invalid token`() {
        val subscriptionId = "reintegros-v1-push"
        whenever(subscriptionRepository.findByIdAndToken(subscriptionId, "wrong")).thenReturn(Optional.empty())

        val ex = assertThrows(ResponseStatusException::class.java) {
            service().updateForSubscription(subscriptionId, "wrong", sampleRequest())
        }

        assertEquals(HttpStatus.UNAUTHORIZED, ex.statusCode)
    }

    @Test
    fun `resetForSubscription rejects invalid token`() {
        val subscriptionId = "reintegros-v1-push"
        whenever(subscriptionRepository.findByIdAndToken(subscriptionId, "wrong")).thenReturn(Optional.empty())

        val ex = assertThrows(ResponseStatusException::class.java) {
            service().resetForSubscription(subscriptionId, "wrong")
        }

        assertEquals(HttpStatus.UNAUTHORIZED, ex.statusCode)
    }

    private fun service(): ConfResilienceService =
        ConfResilienceService(
            confResilienceRepository = confResilienceRepository,
            subscriptionRepository = subscriptionRepository,
            defaultEnabled = true,
            defaultTimeoutMs = 2000,
            defaultRetryMaxAttempts = 3,
            defaultRetryInitialBackoffMs = 500,
            defaultRetryMultiplier = 2.0,
            defaultRetryMaxBackoffMs = 5000,
            defaultRetryJitterFactor = 0.2,
            defaultCbFailureRateThreshold = 50f,
            defaultCbSlowCallRateThreshold = 70f,
            defaultCbSlowCallDurationMs = 2000,
            defaultCbMinimumNumberOfCalls = 10,
            defaultCbSlidingWindowSize = 20,
            defaultCbWaitDurationOpenMs = 30000,
            defaultCbPermittedHalfOpenCalls = 3,
            defaultBulkheadMaxConcurrentCalls = 10,
            defaultBulkheadMaxWaitDurationMs = 0,
            pauseOpenThresholdMs = 30000,
            recoverClosedThresholdMs = 10000
        )

    private fun sampleRequest() =
        ConfResilienceRequest(
            enabled = true,
            timeoutMs = 2000,
            retry = RetryRequest(3, 500, 2.0, 5000, 0.2),
            circuitBreaker = CircuitBreakerRequest(50f, 70f, 2000, 10, 20, 30000, 3),
            bulkhead = BulkheadRequest(10, 0),
            updatedBy = "test"
        )
}
