package com.trading.events.infrastructure.web.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record AccountResponse(String accountId, BigDecimal balance, long version, Instant updatedAt) {
}
