package ru.quipy.payments.config

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class RateLimiterConfig {
    @Bean
    fun webClientRateLimiter(): RateLimiter {
        return RateLimiter.of("webclient", RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofMillis(1000))
            .limitForPeriod(1100)
            .build()
        )
    }
}