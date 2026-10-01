package com.trading.events.infrastructure.gateway;

import com.trading.events.domain.exception.TransientProcessingException;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.infrastructure.config.GatewayProperties;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mercado simulado con latencia configurable. Imita un cliente BLOQUEANTE (Thread.sleep), por eso
 * se ejecuta en el scheduler de hilos virtuales: la espera es barata y no ocupa el event loop.
 * La inyección de fallos solo se activa por configuración ({@code app.gateway.failure-rate}).
 */
public class SimulatedMarketGateway implements MarketGateway {

    private final GatewayProperties properties;
    private final Scheduler blockingScheduler;

    public SimulatedMarketGateway(GatewayProperties properties, Scheduler blockingScheduler) {
        this.properties = properties;
        this.blockingScheduler = blockingScheduler;
    }

    @Override
    public Mono<Void> execute(Event event) {
        return Mono.<Void>fromCallable(() -> {
            simulateLatency(event);
            injectFailure(event);
            return null;
        }).subscribeOn(blockingScheduler);
    }

    private void simulateLatency(Event event) {
        long min = properties.minLatency().toMillis();
        long max = Math.max(min, properties.maxLatency().toMillis());
        // Generador no criptográfico: solo simula variación de latencia.
        long millis = min == max ? min : ThreadLocalRandom.current().nextLong(min, max + 1); // NOSONAR
        try {
            Thread.sleep(Duration.ofMillis(millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientProcessingException(event.eventId(), event.accountId(),
                    "Market call interrupted", e);
        }
    }

    private void injectFailure(Event event) {
        double rate = properties.failureRate();
        if (rate > 0 && ThreadLocalRandom.current().nextDouble() < rate) { // NOSONAR
            throw new TransientProcessingException(event.eventId(), event.accountId(),
                    "Simulated transient market failure");
        }
    }
}
