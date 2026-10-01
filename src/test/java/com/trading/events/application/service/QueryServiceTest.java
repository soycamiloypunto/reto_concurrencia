package com.trading.events.application.service;

import com.trading.events.domain.exception.ResourceNotFoundException;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import com.trading.events.infrastructure.config.StoreProperties;
import com.trading.events.infrastructure.messaging.SinkEventPublisher;
import com.trading.events.infrastructure.persistence.InMemoryAccountRepository;
import com.trading.events.infrastructure.persistence.InMemoryEventRepository;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

class QueryServiceTest {

    private final InMemoryEventRepository events = new InMemoryEventRepository(new StoreProperties(10, Duration.ofMinutes(1)));
    private final InMemoryAccountRepository accounts = new InMemoryAccountRepository();
    private final SinkEventPublisher publisher = new SinkEventPublisher();
    private final QueryService service = new QueryService(events, accounts, publisher);

    @Test
    void returnsExistingEventAndAccount() {
        Event event = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");
        events.save(event).block();
        accounts.update("A", a -> a).block();

        StepVerifier.create(service.getEvent(event.eventId())).expectNext(event).verifyComplete();
        StepVerifier.create(service.getAccount("A")).expectNextCount(1).verifyComplete();
    }

    @Test
    void failsWithNotFound() {
        StepVerifier.create(service.getEvent(java.util.UUID.randomUUID()))
                .expectError(ResourceNotFoundException.class).verify();
        StepVerifier.create(service.getAccount("missing"))
                .expectError(ResourceNotFoundException.class).verify();
    }

    @Test
    void streamDeliversPublishedEvents() {
        Event event = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");
        StepVerifier.create(service.eventStream().take(1))
                .then(() -> publisher.publish(event).block())
                .expectNext(event).verifyComplete();
    }
}
