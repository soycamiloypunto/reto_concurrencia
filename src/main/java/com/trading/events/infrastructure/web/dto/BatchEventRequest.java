package com.trading.events.infrastructure.web.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Lote de eventos. {@code maxConcurrency} es opcional; el servidor lo acota a su límite configurado.
 */
public record BatchEventRequest(
        @NotEmpty List<@Valid EventRequest> events,
        @Min(1) Integer maxConcurrency) {
}
