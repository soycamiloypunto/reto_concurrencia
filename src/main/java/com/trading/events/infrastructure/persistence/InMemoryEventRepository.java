package com.trading.events.infrastructure.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.port.out.EventRepository;
import com.trading.events.infrastructure.config.StoreProperties;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Historial de eventos en memoria con retención ACOTADA (tamaño máximo + TTL) mediante Caffeine,
 * en lugar de un {@code ConcurrentHashMap} que crece sin límite. {@code putIfAbsent} es atómico.
 */
@Repository
public class InMemoryEventRepository implements EventRepository {

    private final Cache<UUID, Event> cache;

    public InMemoryEventRepository(StoreProperties properties) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(properties.maxEvents())
                .expireAfterWrite(properties.ttl())
                .build();
    }

    @Override
    public Mono<Boolean> registerIfAbsent(Event event) {
        return Mono.fromSupplier(() -> cache.asMap().putIfAbsent(event.eventId(), event) == null);
    }

    @Override
    public Mono<Event> save(Event event) {
        return Mono.fromSupplier(() -> {
            cache.put(event.eventId(), event);
            return event;
        });
    }

    @Override
    public Mono<Event> findById(UUID eventId) {
        return Mono.fromSupplier(() -> cache.getIfPresent(eventId));
    }

    @Override
    public Mono<Void> remove(UUID eventId) {
        return Mono.fromRunnable(() -> cache.invalidate(eventId));
    }
}
