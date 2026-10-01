package com.trading.events.infrastructure.metrics;

import com.trading.events.application.concurrency.AccountLaneDispatcher;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

/**
 * Expone la presión de las lanes: tareas en cola y la lane más profunda (cuenta caliente).
 */
@Component
public class LaneMetricsBinder implements MeterBinder {

    private final AccountLaneDispatcher dispatcher;

    public LaneMetricsBinder(AccountLaneDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("events.lanes.queued", dispatcher, AccountLaneDispatcher::queuedTasks)
                .description("Tareas en cola en todas las lanes").register(registry);
        Gauge.builder("events.lanes.max.depth", dispatcher, AccountLaneDispatcher::maxLaneDepth)
                .description("Profundidad de la lane más cargada").register(registry);
        Gauge.builder("events.lanes.count", dispatcher, AccountLaneDispatcher::laneCount)
                .description("Número de lanes").register(registry);
    }
}
