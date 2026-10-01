package com.trading.events.infrastructure.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Retención acotada del historial de eventos (prefijo {@code app.store}); evita fugas de memoria.
 *
 * @param maxEvents máximo de eventos retenidos
 * @param ttl       tiempo de vida de un evento retenido (ventana de idempotencia)
 */
@Validated
@ConfigurationProperties(prefix = "app.store")
public record StoreProperties(
        @DefaultValue("100000") @Min(1) long maxEvents,
        @DefaultValue("1h") @NotNull Duration ttl) {
}
