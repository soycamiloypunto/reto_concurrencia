package com.trading.events.infrastructure.metrics;

import com.trading.events.domain.port.out.ProcessingMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Adaptador de métricas basado en Micrometer (expuestas en /actuator/prometheus).
 */
@Component
public class MicrometerProcessingMetrics implements ProcessingMetrics {

    private final MeterRegistry registry;
    private final Counter received;
    private final Counter rejected;
    private final Counter duplicated;
    private final Timer processingTimer;

    public MicrometerProcessingMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.received = Counter.builder("events.received").description("Eventos recibidos").register(registry);
        this.rejected = Counter.builder("events.rejected").description("Eventos rechazados por saturación").register(registry);
        this.duplicated = Counter.builder("events.duplicated").description("Eventos duplicados (idempotencia)").register(registry);
        this.processingTimer = Timer.builder("events.processing.duration")
                .description("Duración del procesamiento dentro de la lane")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    @Override
    public void received() {
        received.increment();
    }

    @Override
    public void completed(Duration elapsed) {
        processingTimer.record(elapsed);
        registry.counter("events.completed").increment();
    }

    @Override
    public void failed(String reason) {
        registry.counter("events.failed", "reason", reason).increment();
    }

    @Override
    public void rejected() {
        rejected.increment();
    }

    @Override
    public void duplicated() {
        duplicated.increment();
    }
}
