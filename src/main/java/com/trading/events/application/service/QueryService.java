package com.trading.events.application.service;

import com.trading.events.domain.exception.ResourceNotFoundException;
import com.trading.events.domain.model.Account;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.port.in.QueryUseCase;
import com.trading.events.domain.port.out.AccountRepository;
import com.trading.events.domain.port.out.EventPublisher;
import com.trading.events.domain.port.out.EventRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Consultas de solo lectura; no participa en las lanes porque no modifica estado.
 */
@Service
public class QueryService implements QueryUseCase {

    private final EventRepository events;
    private final AccountRepository accounts;
    private final EventPublisher publisher;

    public QueryService(EventRepository events, AccountRepository accounts, EventPublisher publisher) {
        this.events = events;
        this.accounts = accounts;
        this.publisher = publisher;
    }

    @Override
    public Mono<Event> getEvent(UUID eventId) {
        return events.findById(eventId)
                .switchIfEmpty(Mono.error(() -> new ResourceNotFoundException("Event not found: " + eventId)));
    }

    @Override
    public Mono<Account> getAccount(String accountId) {
        return accounts.find(accountId)
                .switchIfEmpty(Mono.error(() -> new ResourceNotFoundException("Account not found: " + accountId)));
    }

    @Override
    public Flux<Event> eventStream() {
        return publisher.stream();
    }
}
