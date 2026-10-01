package com.trading.events.domain.port.out;

import com.trading.events.domain.model.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Puerto de publicación de eventos procesados hacia suscriptores (pub/sub).
 */
public interface EventPublisher {

    /**
     * Publica el evento. Nunca falla por problemas de los suscriptores.
     */
    Mono<Void> publish(Event event);

    Flux<Event> stream();
}
