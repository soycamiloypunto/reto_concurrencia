package com.trading.events.infrastructure.messaging;

import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

@DisplayName("SinkEventPublisher: emisión serializada bajo publicación concurrente")
class SinkEventPublisherTest {

    private static Event event() {
        return Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X");
    }

    @Test
    @DisplayName("publicar desde 32 hilos a la vez no pierde ningún evento (sin FAIL_NON_SERIALIZED)")
    void concurrentPublishersLoseNothing() {
        SinkEventPublisher publisher = new SinkEventPublisher();
        ConcurrentLinkedQueue<Event> received = new ConcurrentLinkedQueue<>();
        publisher.stream().subscribe(received::add);

        try (ExecutorService pool = Executors.newFixedThreadPool(32)) {
            IntStream.range(0, 10_000).forEach(i -> pool.submit(() -> publisher.publish(event()).block()));
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).hasSize(10_000));
    }

    @Test
    @DisplayName("publicar sin suscriptores no falla")
    void publishWithoutSubscribersIsHarmless() {
        SinkEventPublisher publisher = new SinkEventPublisher();
        assertThatCode(() -> publisher.publish(event()).block()).doesNotThrowAnyException();
    }
}
