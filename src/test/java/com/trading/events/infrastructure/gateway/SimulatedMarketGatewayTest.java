package com.trading.events.infrastructure.gateway;

import com.trading.events.domain.exception.TransientProcessingException;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import com.trading.events.infrastructure.config.GatewayProperties;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedMarketGatewayTest {

    private final Event event = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");

    private static GatewayProperties props(long min, long max, double failureRate) {
        return new GatewayProperties(Duration.ofMillis(min), Duration.ofMillis(max), failureRate,
                Duration.ofSeconds(1), 10, new GatewayProperties.Breaker(50, 10, 5, Duration.ofSeconds(1)),
                new GatewayProperties.Retry(0, Duration.ofMillis(1)));
    }

    @Test
    void appliesLatencyWithinRangeOnAVirtualThread() {
        AtomicBoolean virtual = new AtomicBoolean();
        Scheduler scheduler = Schedulers.fromExecutorService(Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().factory()));
        SimulatedMarketGateway gateway = new SimulatedMarketGateway(props(30, 40, 0), scheduler);

        long start = System.nanoTime();
        StepVerifier.create(gateway.execute(event).doOnSubscribe(s -> { }).doFinally(s -> { })
                        .doOnTerminate(() -> virtual.set(Thread.currentThread().isVirtual())))
                .verifyComplete();
        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertThat(elapsedMs).isGreaterThanOrEqualTo(30);
        assertThat(virtual).isTrue(); // la espera bloqueante corre en un hilo virtual
        scheduler.dispose();
    }

    @Test
    void injectsTransientFailureWhenConfigured() {
        SimulatedMarketGateway gateway = new SimulatedMarketGateway(props(0, 0, 1.0), Schedulers.immediate());
        StepVerifier.create(gateway.execute(event)).expectError(TransientProcessingException.class).verify();
    }

    @Test
    void neverFailsWithZeroRate() {
        SimulatedMarketGateway gateway = new SimulatedMarketGateway(props(0, 0, 0.0), Schedulers.immediate());
        for (int i = 0; i < 50; i++) {
            StepVerifier.create(gateway.execute(event)).verifyComplete();
        }
    }
}
