package com.trading.events.domain.exception;

import java.util.UUID;

/**
 * Excepción base para errores durante el procesamiento de eventos.
 */
public class EventProcessingException extends RuntimeException {

    private final UUID eventId;
    private final String accountId;

    public EventProcessingException(UUID eventId, String accountId, String message) {
        super(message);
        this.eventId = eventId;
        this.accountId = accountId;
    }

    public EventProcessingException(UUID eventId, String accountId, String message, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
        this.accountId = accountId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getAccountId() {
        return accountId;
    }
}
