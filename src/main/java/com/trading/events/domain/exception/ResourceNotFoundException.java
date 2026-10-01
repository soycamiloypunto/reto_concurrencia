package com.trading.events.domain.exception;

/**
 * El recurso solicitado (evento o cuenta) no existe.
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
