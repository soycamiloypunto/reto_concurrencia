package com.trading.events.domain.exception;

import java.util.UUID;

/**
 * El evento no cumple las reglas de validación. No es reintentable.
 */
public class InvalidEventException extends EventProcessingException {

    public InvalidEventException(UUID eventId, String accountId, String message) {
        super(eventId, accountId, message);
    }

    public static InvalidEventException because(UUID eventId, String accountId, String reason) {
        return new InvalidEventException(eventId, accountId,
                "Event " + eventId + " for account " + accountId + " is invalid: " + reason);
    }
}
