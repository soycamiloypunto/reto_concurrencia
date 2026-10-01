package com.trading.events.domain.port.out;

import com.trading.events.domain.model.Event;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Puerto de persistencia de eventos; sostiene la idempotencia por {@code eventId}.
 */
public interface EventRepository {

    /**
     * Registra el evento solo si su id no existía (operación atómica).
     *
     * @return true si es la primera vez que se ve el id; false si es un duplicado
     */
    Mono<Boolean> registerIfAbsent(Event event);

    Mono<Event> save(Event event);

    Mono<Event> findById(UUID eventId);

    /**
     * Elimina el registro (se usa cuando un evento se rechazó sin procesarse, para permitir reenviarlo).
     */
    Mono<Void> remove(UUID eventId);
}
