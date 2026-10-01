package com.trading.events.domain.exception;

import java.util.UUID;

/**
 * El mercado no está disponible (circuit breaker abierto o fallos agotados).
 */
public class GatewayUnavailableException extends EventProcessingException {

    public GatewayUnavailableException(UUID eventId, String accountId, String message, Throwable cause) {
        super(eventId, accountId, message, cause);
    }
}
