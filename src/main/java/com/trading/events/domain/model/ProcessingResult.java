package com.trading.events.domain.model;

import java.util.UUID;

/**
 * Resultado individual de procesar un evento dentro de un lote.
 */
public record ProcessingResult(UUID eventId, String accountId, EventStatus status, String reason) {

    public static ProcessingResult completed(Event event) {
        return new ProcessingResult(event.eventId(), event.accountId(), event.status(), event.failureReason());
    }

    public static ProcessingResult failed(UUID eventId, String accountId, String reason) {
        return new ProcessingResult(eventId, accountId, EventStatus.FAILED, reason);
    }

    public boolean isSuccess() {
        return status == EventStatus.COMPLETED;
    }
}
