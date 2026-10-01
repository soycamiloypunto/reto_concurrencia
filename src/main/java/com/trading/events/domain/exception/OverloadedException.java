package com.trading.events.domain.exception;

/**
 * El sistema está saturado (cola de la lane o bulkhead llenos). Se rechaza rápido en lugar
 * de acumular trabajo sin límite (backpressure explícito).
 */
public class OverloadedException extends EventProcessingException {

    public OverloadedException(String accountId, String message) {
        super(null, accountId, message);
    }
}
