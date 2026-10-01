package com.trading.events.domain.model;

import com.trading.events.domain.exception.InvalidEventException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Evento de trading inmutable. Cada transición de estado devuelve una nueva instancia,
 * por lo que es seguro compartirlo entre hilos sin sincronización.
 */
public record Event(
        UUID eventId,
        String accountId,
        EventType eventType,
        BigDecimal amount,
        Instant timestamp,
        EventStatus status,
        String marketSource,
        String failureReason) {

    /** Máximo de decimales permitidos en un monto (coincide con la escala de {@link Account}). */
    public static final int MAX_AMOUNT_SCALE = 4;

    /**
     * Crea un evento en estado RECEIVED. Si {@code eventId} es nulo se genera uno nuevo;
     * un id provisto por el cliente habilita la idempotencia.
     */
    public static Event received(UUID eventId, String accountId, EventType eventType,
                                 BigDecimal amount, String marketSource) {
        return new Event(
                eventId != null ? eventId : UUID.randomUUID(),
                accountId,
                eventType,
                amount,
                Instant.now(),
                EventStatus.RECEIVED,
                marketSource,
                null);
    }

    public Event markAsProcessing() {
        requireStatus(EventStatus.RECEIVED, EventStatus.PROCESSING);
        return withStatus(EventStatus.PROCESSING, null);
    }

    public Event markAsCompleted() {
        requireStatus(EventStatus.PROCESSING, EventStatus.COMPLETED);
        return withStatus(EventStatus.COMPLETED, null);
    }

    /**
     * Marca el evento como fallido. Se permite desde RECEIVED (rechazo previo al procesamiento)
     * y desde PROCESSING.
     */
    public Event markAsFailed(String reason) {
        if (status == EventStatus.COMPLETED || status == EventStatus.FAILED) {
            throw new IllegalStateException("Event cannot transition to FAILED from " + status);
        }
        return withStatus(EventStatus.FAILED, reason);
    }

    /**
     * Valida la consistencia de los datos del evento.
     *
     * @throws InvalidEventException si algún campo es inválido
     */
    public void validate() {
        if (isBlank(accountId)) {
            throw invalid("Account ID cannot be null or empty");
        }
        if (eventType == null) {
            throw invalid("Event type cannot be null");
        }
        if (isBlank(marketSource)) {
            throw invalid("Market source cannot be null or empty");
        }
        validateAmount();
    }

    private void validateAmount() {
        if (amount == null || amount.signum() <= 0) {
            throw invalid("Amount must be positive");
        }
        if (amount.stripTrailingZeros().scale() > MAX_AMOUNT_SCALE) {
            throw invalid("Amount cannot have more than " + MAX_AMOUNT_SCALE + " decimals");
        }
    }

    private void requireStatus(EventStatus expected, EventStatus target) {
        if (status != expected) {
            throw new IllegalStateException("Event cannot transition to " + target + " from " + status);
        }
    }

    private Event withStatus(EventStatus newStatus, String reason) {
        return new Event(eventId, accountId, eventType, amount, timestamp, newStatus, marketSource, reason);
    }

    private InvalidEventException invalid(String reason) {
        return InvalidEventException.because(eventId, accountId, reason);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
