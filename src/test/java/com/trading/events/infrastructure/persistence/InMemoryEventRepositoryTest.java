package com.trading.events.infrastructure.persistence;

import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import com.trading.events.infrastructure.config.StoreProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryEventRepositoryTest {

    private final InMemoryEventRepository repository =
            new InMemoryEventRepository(new StoreProperties(1000, Duration.ofMinutes(1)));

    private static Event event() {
        return Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");
    }

    @Test
    @DisplayName("registerIfAbsent es atómico: con 200 hilos y el mismo id solo uno gana")
    void onlyOneRegistrationWins() {
        Event event = event();
        AtomicInteger winners = new AtomicInteger();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, 200).forEach(i -> pool.submit(() -> {
                if (Boolean.TRUE.equals(repository.registerIfAbsent(event).block())) {
                    winners.incrementAndGet();
                }
            }));
        }

        assertThat(winners).hasValue(1);
    }

    @Test
    void saveFindAndRemove() {
        Event event = event();
        repository.save(event).block();
        assertThat(repository.findById(event.eventId()).block()).isEqualTo(event);

        repository.remove(event.eventId()).block();
        assertThat(repository.findById(event.eventId()).block()).isNull();
    }
}
