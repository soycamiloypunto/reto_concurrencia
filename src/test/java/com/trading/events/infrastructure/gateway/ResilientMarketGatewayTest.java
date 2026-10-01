package com.trading.events.infrastructure.gateway;

import com.trading.events.domain.exception.GatewayUnavailableException;
import com.trading.events.domain.exception.InvalidEventException;
import com.trading.events.domain.exception.OverloadedException;
import com.trading.events.domain.exception.TransientProcessingException;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.infrastructure.config.GatewayProperties;
import com.trading.events.infrastructure.config.ResilienceConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ResilientMarketGateway: Retry(CircuitBreaker(Bulkhead(timeout(llamada))))")
class ResilientMarketGatewayTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private final Event event = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");
    private final AtomicInteger calls = new AtomicInteger();

    private static GatewayProperties props(int retries, int bulkhead, Duration timeout, int window) {
        return new GatewayProperties(Duration.ofMillis(1), Duration.ofMillis(1), 0.0, timeout, bulkhead,
                new GatewayProperties.Breaker(50, window, window, Duration.ofSeconds(30)),
                new GatewayProperties.Retry(retries, Duration.ofMillis(1)));
    }

    private MarketGateway gateway(MarketGateway delegate, GatewayProperties props) {
        ResilienceConfig config = new ResilienceConfig();
        return new ResilientMarketGateway(delegate, config.marketCircuitBreaker(props),
                config.marketBulkhead(props), props);
    }

    private MarketGateway gatewayWithBreaker(MarketGateway delegate, GatewayProperties props, CircuitBreaker[] out) {
        ResilienceConfig config = new ResilienceConfig();
        out[0] = config.marketCircuitBreaker(props);
        return new ResilientMarketGateway(delegate, out[0], config.marketBulkhead(props), props);
    }

    @Test
    @DisplayName("reintenta fallos transitorios hasta tener éxito")
    void retriesTransientFailures() {
        MarketGateway flaky = e -> calls.incrementAndGet() < 3
                ? Mono.error(new TransientProcessingException(e.eventId(), e.accountId(), "flaky"))
                : Mono.empty();

        StepVerifier.create(gateway(flaky, props(2, 10, Duration.ofSeconds(1), 100)).execute(event))
                .verifyComplete();
        assertThat(calls).hasValue(3);
    }

    @Test
    @DisplayName("NO reintenta errores no transitorios (se propagan tal cual, 1 sola llamada)")
    void doesNotRetryNonTransient() {
        MarketGateway invalid = e -> {
            calls.incrementAndGet();
            return Mono.error(InvalidEventException.because(e.eventId(), e.accountId(), "bad"));
        };

        StepVerifier.create(gateway(invalid, props(3, 10, Duration.ofSeconds(1), 100)).execute(event))
                .expectError(InvalidEventException.class).verify(WAIT);
        assertThat(calls).hasValue(1);
    }

    @Test
    @DisplayName("agotados los reintentos se informa GatewayUnavailableException")
    void exhaustedRetriesBecomeUnavailable() {
        MarketGateway down = e -> {
            calls.incrementAndGet();
            return Mono.error(new TransientProcessingException(e.eventId(), e.accountId(), "down"));
        };

        StepVerifier.create(gateway(down, props(2, 10, Duration.ofSeconds(1), 100)).execute(event))
                .expectError(GatewayUnavailableException.class).verify(WAIT);
        assertThat(calls).hasValue(3); // 1 intento + 2 reintentos
    }

    @Test
    @DisplayName("el circuit breaker abre tras fallos sostenidos y deja de llamar al mercado")
    void circuitBreakerOpens() {
        MarketGateway down = e -> {
            calls.incrementAndGet();
            return Mono.error(new TransientProcessingException(e.eventId(), e.accountId(), "down"));
        };
        CircuitBreaker[] breaker = new CircuitBreaker[1];
        MarketGateway gateway = gatewayWithBreaker(down, props(0, 10, Duration.ofSeconds(1), 4), breaker);

        for (int i = 0; i < 4; i++) {
            StepVerifier.create(gateway.execute(event)).expectError(GatewayUnavailableException.class).verify(WAIT);
        }
        assertThat(breaker[0].getState()).isEqualTo(CircuitBreaker.State.OPEN);

        int callsBefore = calls.get();
        StepVerifier.create(gateway.execute(event))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(GatewayUnavailableException.class)
                        .hasMessageContaining("circuit breaker"))
                .verify(WAIT);
        assertThat(calls).hasValue(callsBefore); // en circuito abierto ni siquiera se invoca el delegado
    }

    @Test
    @DisplayName("el bulkhead rechaza llamadas por encima del máximo simultáneo")
    void bulkheadRejectsExcess() {
        Sinks.Empty<Void> gate = Sinks.empty();
        MarketGateway slow = e -> gate.asMono();
        MarketGateway gateway = gateway(slow, props(0, 1, Duration.ofSeconds(30), 100));

        gateway.execute(event).subscribe();   // ocupa el único permiso
        StepVerifier.create(gateway.execute(event)).expectError(OverloadedException.class).verify(WAIT);
        gate.tryEmitEmpty();
    }

    @Test
    @DisplayName("el timeout se trata como fallo transitorio")
    void timeoutBecomesUnavailable() {
        MarketGateway hang = e -> Mono.never();

        StepVerifier.create(gateway(hang, props(0, 10, Duration.ofMillis(50), 100)).execute(event))
                .expectErrorSatisfies(e -> assertThat(e).isInstanceOf(GatewayUnavailableException.class)
                        .hasCauseInstanceOf(TransientProcessingException.class))
                .verify(WAIT);
    }
}
