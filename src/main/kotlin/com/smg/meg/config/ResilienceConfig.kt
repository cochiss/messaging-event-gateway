package com.smg.meg.config

import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.retry.RetryRegistry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class ResilienceConfig {

    @Bean
    fun retryRegistry(): RetryRegistry {
        val retryConfig = RetryConfig.custom<Any>()
            .maxAttempts(10)
            .intervalFunction { attempt ->
                (500 * Math.pow(2.0, (attempt - 1).toDouble())).toLong()
            }
            .build()
        return RetryRegistry.of(retryConfig)
    }
}
