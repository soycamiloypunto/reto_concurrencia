package com.trading.events.infrastructure.messaging;

import com.trading.events.domain.model.Event;
import com.trading.events.domain.port.out.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Publicador pub/sub sobre un {@link Sinks.Many} multicast. Un Sink exige emisiones
 * SERIALIZADAS: si varias lanes publican a la vez, {@code tryEmitNext} devuelve
 * {@code FAIL_NON_SERIALIZED}; aquí se reintenta (spin) hasta que el hilo concurrente termine,
 * de modo que no se pierde ningún evento. {@code directBestEffort} descarta para el suscriptor
 * lento en lugar de frenar a todos (un consumidor lento no bloquea el procesamiento).
 */
@Component
public class SinkEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(SinkEventPublisher.class);

    private final Sinks.Many<Event> sink = Sinks.many().multicast().directBestEffort();

    @Override
    public Mono<Void> publish(Event event) {
        return Mono.fromRunnable(() -> emit(event));
    }

    @Override
    public Flux<Event> stream() {
        return sink.asFlux();
    }

    private void emit(Event event) {
        Sinks.EmitResult result = sink.tryEmitNext(event);
        while (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            Thread.onSpinWait();
            result = sink.tryEmitNext(event);
        }
        if (result != Sinks.EmitResult.OK && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            log.warn("Event {} was not delivered to the stream: {}", event.eventId(), result);
        }
    }
}
