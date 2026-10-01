package com.trading.events.infrastructure.config;

import com.trading.events.domain.exception.TransientProcessingException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Instancias de Resilience4j creadas por código (sin anotaciones ni AOP) para que el orden de
 * los decoradores sea explícito: Retry( CircuitBreaker( Bulkhead( llamada ) ) ).
 */
@Configuration(proxyBeanMethods = false)
public class ResilienceConfig {

    @Bean
    public CircuitBreaker marketCircuitBreaker(GatewayProperties properties) {
        GatewayProperties.Breaker breaker = properties.breaker();
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(breaker.failureRateThreshold())
                .slidingWindowSize(breaker.slidingWindowSize())
                .minimumNumberOfCalls(breaker.minimumNumberOfCalls())
                .waitDurationInOpenState(breaker.waitDurationInOpen())
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                // Solo fallos de infraestructura abren el circuito; los errores de negocio no cuentan.
                .recordExceptions(TransientProcessingException.class, TimeoutException.class)
                .build();
        return CircuitBreaker.of("market", config);
    }

    @Bean
    public Bulkhead marketBulkhead(GatewayProperties properties) {
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(properties.bulkheadMaxCalls())
                .maxWaitDuration(Duration.ZERO)
                .build();
        return Bulkhead.of("market", config);
    }
}
