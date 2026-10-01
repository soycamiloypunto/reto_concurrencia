package com.trading.events.domain.port.in;

import com.trading.events.domain.model.EventType;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Comando de entrada para procesar un evento. {@code eventId} es opcional: si el cliente lo
 * envía, el procesamiento es idempotente.
 */
public record ProcessEventCommand(
        UUID eventId,
        String accountId,
        EventType eventType,
        BigDecimal amount,
        String marketSource) {
}
