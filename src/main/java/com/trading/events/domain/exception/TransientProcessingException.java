package com.trading.events.domain.exception;

import java.util.UUID;

/**
 * Fallo transitorio (p. ej. error momentáneo del mercado). Es el ÚNICO tipo de error
 * que se reintenta.
 */
public class TransientProcessingException extends EventProcessingException {

    public TransientProcessingException(UUID eventId, String accountId, String message) {
        super(eventId, accountId, message);
    }

    public TransientProcessingException(UUID eventId, String accountId, String message, Throwable cause) {
        super(eventId, accountId, message, cause);
    }
}
