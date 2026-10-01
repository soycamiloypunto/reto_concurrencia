package com.trading.events.application.service;

import com.trading.events.application.concurrency.AccountLaneDispatcher;
import com.trading.events.domain.exception.EventProcessingException;
import com.trading.events.domain.exception.OverloadedException;
import com.trading.events.domain.model.Account;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.ProcessingResult;
import com.trading.events.domain.port.in.ProcessEventCommand;
import com.trading.events.domain.port.in.ProcessEventUseCase;
import com.trading.events.domain.port.out.AccountRepository;
import com.trading.events.domain.port.out.EventPublisher;
import com.trading.events.domain.port.out.EventRepository;
import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.domain.port.out.ProcessingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Orquesta el procesamiento: validar, garantizar idempotencia, serializar por cuenta (lane),
 * confirmar en el mercado, actualizar el balance de forma atómica y publicar el resultado.
 */
@Service
public class ProcessEventService implements ProcessEventUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessEventService.class);

    private final AccountLaneDispatcher dispatcher;
    private final AccountRepository accounts;
    private final EventRepository events;
    private final MarketGateway gateway;
    private final EventPublisher publisher;
    private final ProcessingMetrics metrics;
    private final int maxBatchConcurrency;

    public ProcessEventService(AccountLaneDispatcher dispatcher,
                               AccountRepository accounts,
                               EventRepository events,
                               MarketGateway gateway,
                               EventPublisher publisher,
                               ProcessingMetrics metrics,
                               ProcessingLimits limits) {
        this.dispatcher = dispatcher;
        this.accounts = accounts;
        this.events = events;
        this.gateway = gateway;
        this.publisher = publisher;
        this.metrics = metrics;
        this.maxBatchConcurrency = limits.maxBatchConcurrency();
    }

    @Override
    public Mono<Event> process(ProcessEventCommand command) {
        return Mono.defer(() -> {
            Event event = toEvent(command);
            event.validate();
            metrics.received();
            return events.registerIfAbsent(event)
                    .flatMap(isNew -> Boolean.TRUE.equals(isNew) ? enqueue(event) : returnExisting(event));
        });
    }

    @Override
    public Flux<ProcessingResult> processBatch(List<ProcessEventCommand> commands, int maxConcurrency) {
        int concurrency = Math.clamp(maxConcurrency, 1, maxBatchConcurrency);
        return Flux.fromIterable(commands)
                .flatMap(command -> process(command)
                        .map(ProcessingResult::completed)
                        .onErrorResume(EventProcessingException.class, e ->
                                Mono.just(ProcessingResult.failed(
                                        e.getEventId() != null ? e.getEventId() : command.eventId(),
                                        command.accountId(),
                                        e.getMessage()))), concurrency);
    }

    private Mono<Event> enqueue(Event event) {
        return dispatcher.submit(event.accountId(), () -> executeInLane(event))
                .doOnError(OverloadedException.class, e -> {
                    metrics.rejected();
                    log.warn("Event {} rejected: {}", event.eventId(), e.getMessage());
                })
                .onErrorResume(OverloadedException.class, e -> discardRejected(event).then(Mono.error(e)));
    }

    /** Un evento rechazado por saturación no se procesó: se libera su id para que pueda reenviarse. */
    private Mono<Void> discardRejected(Event event) {
        return events.remove(event.eventId());
    }

    private Mono<Event> returnExisting(Event event) {
        metrics.duplicated();
        log.debug("Duplicate event {} ignored (idempotent)", event.eventId());
        return events.findById(event.eventId());
    }

    /**
     * Se ejecuta DENTRO de la lane de la cuenta: nadie más modifica esta cuenta mientras corre.
     */
    private Mono<Event> executeInLane(Event received) {
        long startNanos = System.nanoTime();
        Event processing = received.markAsProcessing();
        return events.save(processing)
                // defer: cada paso se evalúa al suscribirse (en orden), no al armar el pipeline.
                .then(Mono.defer(() -> checkFunds(processing)))
                .then(Mono.defer(() -> gateway.execute(processing)))
                .then(Mono.defer(() -> accounts.update(processing.accountId(), account -> account.apply(processing))))
                .thenReturn(processing.markAsCompleted())
                .flatMap(events::save)
                .flatMap(this::publishQuietly)
                .doOnSuccess(done -> metrics.completed(Duration.ofNanos(System.nanoTime() - startNanos)))
                .onErrorResume(error -> recordFailure(processing, error));
    }

    /** Verifica fondos antes de llamar al mercado (seguro: la lane serializa la cuenta). */
    private Mono<Account> checkFunds(Event event) {
        return accounts.find(event.accountId())
                .defaultIfEmpty(Account.open(event.accountId()))
                .doOnNext(account -> account.apply(event));
    }

    private Mono<Event> recordFailure(Event processing, Throwable error) {
        metrics.failed(error.getClass().getSimpleName());
        log.warn("Event {} failed: {}", processing.eventId(), error.getMessage());
        return events.save(processing.markAsFailed(error.getMessage()))
                .then(Mono.error(error));
    }

    private Mono<Event> publishQuietly(Event completed) {
        return publisher.publish(completed)
                .onErrorResume(e -> {
                    log.warn("Publishing event {} failed: {}", completed.eventId(), e.getMessage());
                    return Mono.empty();
                })
                .thenReturn(completed);
    }

    private static Event toEvent(ProcessEventCommand command) {
        return Event.received(command.eventId(), command.accountId(), command.eventType(),
                command.amount(), command.marketSource());
    }
}
