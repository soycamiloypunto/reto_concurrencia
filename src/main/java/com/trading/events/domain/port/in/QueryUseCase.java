package com.trading.events.domain.port.in;

import com.trading.events.domain.model.Account;
import com.trading.events.domain.model.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Consultas de solo lectura sobre eventos y cuentas.
 */
public interface QueryUseCase {

    Mono<Event> getEvent(UUID eventId);

    Mono<Account> getAccount(String accountId);

    /**
     * @return flujo en vivo de eventos procesados
     */
    Flux<Event> eventStream();
}
