package com.trading.events.infrastructure.web.dto;

import com.trading.events.domain.model.EventType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Evento entrante. {@code eventId} es opcional: si se envía, el reenvío es idempotente.
 */
public record EventRequest(
        UUID eventId,
        @NotBlank @Size(max = 64) String accountId,
        @NotNull EventType eventType,
        @NotNull @Positive @Digits(integer = 15, fraction = 4) @DecimalMax("999999999999999.9999") BigDecimal amount,
        @NotBlank @Size(max = 64) String marketSource) {
}
