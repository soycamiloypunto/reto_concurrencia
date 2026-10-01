package com.trading.events.infrastructure.gateway;

import com.trading.events.domain.exception.GatewayUnavailableException;
import com.trading.events.domain.exception.OverloadedException;
import com.trading.events.domain.exception.TransientProcessingException;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.infrastructure.config.GatewayProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.util.concurrent.TimeoutException;

/**
 * Decorador de resiliencia. Orden (de fuera hacia dentro):
 * {@code Retry( CircuitBreaker( Bulkhead( timeout( llamada ) ) ) )}.
 * <ul>
 *   <li>Bulkhead: limita las llamadas simultáneas hacia el mercado.</li>
 *   <li>CircuitBreaker: corta si el mercado falla de forma sostenida.</li>
 *   <li>Retry: SOLO reintenta fallos transitorios, con backoff exponencial y jitter.</li>
 * </ul>
 */
public class ResilientMarketGateway implements MarketGateway {

    private static final Logger log = LoggerFactory.getLogger(ResilientMarketGateway.class);
    private static final double JITTER = 0.5;

    private final MarketGateway delegate;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final GatewayProperties properties;

    public ResilientMarketGateway(MarketGateway delegate, CircuitBreaker circuitBreaker,
                                  Bulkhead bulkhead, GatewayProperties properties) {
        this.delegate = delegate;
        this.circuitBreaker = circuitBreaker;
        this.bulkhead = bulkhead;
        this.properties = properties;
    }

    @Override
    public Mono<Void> execute(Event event) {
        return Mono.defer(() -> delegate.execute(event))
                .timeout(properties.timeout())
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .onErrorMap(BulkheadFullException.class,
                        e -> new OverloadedException(event.accountId(), "Market bulkhead is full"))
                .onErrorMap(CallNotPermittedException.class,
                        e -> new GatewayUnavailableException(event.eventId(), event.accountId(),
                                "Market circuit breaker is open", e))
                .onErrorMap(TimeoutException.class,
                        e -> new TransientProcessingException(event.eventId(), event.accountId(),
                                "Market call timed out", e))
                .retryWhen(retrySpec(event));
    }

    private Retry retrySpec(Event event) {
        return Retry.backoff(properties.retry().maxAttempts(), properties.retry().backoff())
                .jitter(JITTER)
                .filter(TransientProcessingException.class::isInstance)
                .doBeforeRetry(signal -> log.debug("Retrying event {} (attempt {})",
                        event.eventId(), signal.totalRetries() + 1))
                .onRetryExhaustedThrow((spec, signal) -> new GatewayUnavailableException(
                        event.eventId(), event.accountId(),
                        "Market unavailable after " + (signal.totalRetries() + 1) + " attempts",
                        signal.failure()));
    }
}
