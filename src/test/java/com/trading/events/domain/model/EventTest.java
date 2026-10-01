package com.trading.events.domain.model;

import com.trading.events.domain.exception.InvalidEventException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Event: transiciones de estado y validación")
class EventTest {

    private static Event event() {
        return Event.received(null, "ACC-1", EventType.DEPOSIT, new BigDecimal("10.50"), "NYSE");
    }

    @Test
    @DisplayName("received genera id cuando no se provee y respeta el provisto")
    void receivedGeneratesOrKeepsId() {
        UUID id = UUID.randomUUID();
        assertThat(event().eventId()).isNotNull();
        assertThat(Event.received(id, "A", EventType.DEPOSIT, BigDecimal.ONE, "X").eventId()).isEqualTo(id);
        assertThat(event().status()).isEqualTo(EventStatus.RECEIVED);
    }

    @Test
    @DisplayName("recorre RECEIVED -> PROCESSING -> COMPLETED")
    void happyPathTransitions() {
        Event completed = event().markAsProcessing().markAsCompleted();
        assertThat(completed.status()).isEqualTo(EventStatus.COMPLETED);
    }

    @Test
    @DisplayName("no permite saltarse estados")
    void rejectsInvalidTransitions() {
        Event received = event();
        assertThatThrownBy(received::markAsCompleted).isInstanceOf(IllegalStateException.class);
        Event processing = received.markAsProcessing();
        assertThatThrownBy(processing::markAsProcessing).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("FAILED es válido desde RECEIVED y PROCESSING, pero no desde un estado terminal")
    void failedTransitions() {
        assertThat(event().markAsFailed("x").failureReason()).isEqualTo("x");
        assertThat(event().markAsProcessing().markAsFailed("y").status()).isEqualTo(EventStatus.FAILED);
        Event completed = event().markAsProcessing().markAsCompleted();
        assertThatThrownBy(() -> completed.markAsFailed("z")).isInstanceOf(IllegalStateException.class);
        Event failed = event().markAsFailed("a");
        assertThatThrownBy(() -> failed.markAsFailed("b")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("un evento válido pasa la validación")
    void validEvent() {
        assertThatCode(event()::validate).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  "})
    @DisplayName("rechaza cuenta o mercado vacíos")
    void rejectsBlankFields(String blank) {
        Event noAccount = Event.received(null, blank, EventType.DEPOSIT, BigDecimal.ONE, "X");
        Event noMarket = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, blank);
        assertThatThrownBy(noAccount::validate).isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(noMarket::validate).isInstanceOf(InvalidEventException.class);
    }

    @Test
    @DisplayName("rechaza tipo nulo, montos no positivos y con demasiados decimales")
    void rejectsBadTypeAndAmounts() {
        assertThatThrownBy(() -> Event.received(null, "A", null, BigDecimal.ONE, "X").validate())
                .isInstanceOf(InvalidEventException.class);
        for (BigDecimal bad : new BigDecimal[]{null, BigDecimal.ZERO, new BigDecimal("-1"), new BigDecimal("1.00001")}) {
            Event e = Event.received(null, "A", EventType.DEPOSIT, bad, "X");
            assertThatThrownBy(e::validate).isInstanceOf(InvalidEventException.class);
        }
    }

    @Test
    @DisplayName("acepta 4 decimales aunque tenga ceros a la derecha")
    void acceptsFourDecimals() {
        Event e = Event.received(null, "A", EventType.DEPOSIT, new BigDecimal("1.230000"), "X");
        assertThatCode(e::validate).doesNotThrowAnyException();
    }
}
