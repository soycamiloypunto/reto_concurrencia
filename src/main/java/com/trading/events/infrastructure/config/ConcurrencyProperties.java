package com.trading.events.infrastructure.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Parámetros de concurrencia (prefijo {@code app.concurrency}).
 *
 * @param lanes               lanes por cuenta = grado máximo de paralelismo entre cuentas
 * @param laneQueueCapacity   tareas en espera por lane antes de rechazar con 429
 * @param maxBatchConcurrency máximo de eventos simultáneos dentro de un lote
 * @param maxBatchSize        máximo de eventos aceptados por lote
 */
@Validated
@ConfigurationProperties(prefix = "app.concurrency")
public record ConcurrencyProperties(
        @DefaultValue("64") @Min(1) int lanes,
        @DefaultValue("1000") @Min(1) int laneQueueCapacity,
        @DefaultValue("64") @Min(1) int maxBatchConcurrency,
        @DefaultValue("1000") @Min(1) int maxBatchSize) {
}
