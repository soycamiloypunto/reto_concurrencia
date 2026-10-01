package com.trading.events.infrastructure.web.dto;

import com.trading.events.domain.model.EventStatus;
import com.trading.events.domain.model.EventType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record EventResponse(
        UUID eventId,
        String accountId,
        EventType eventType,
        BigDecimal amount,
        EventStatus status,
        Instant timestamp,
        String marketSource,
        String failureReason) {
}
