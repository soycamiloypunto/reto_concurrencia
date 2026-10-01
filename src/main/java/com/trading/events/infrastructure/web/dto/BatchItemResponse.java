package com.trading.events.infrastructure.web.dto;

import com.trading.events.domain.model.EventStatus;

import java.util.UUID;

public record BatchItemResponse(UUID eventId, String accountId, EventStatus status, String reason) {
}
