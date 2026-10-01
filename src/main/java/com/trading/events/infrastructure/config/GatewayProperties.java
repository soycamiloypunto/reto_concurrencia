package com.trading.events.infrastructure.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Parámetros del mercado simulado y de sus decoradores de resiliencia (prefijo {@code app.gateway}).
 *
 * @param minLatency       latencia mínima simulada de una llamada al mercado
 * @param maxLatency       latencia máxima simulada
 * @param failureRate      probabilidad [0,1] de inyectar un fallo transitorio (0 en producción)
 * @param timeout          tiempo máximo por llamada
 * @param bulkheadMaxCalls llamadas simultáneas permitidas hacia el mercado
 * @param breaker          configuración del circuit breaker
 * @param retry            configuración del reintento
 */
@Validated
@ConfigurationProperties(prefix = "app.gateway")
public record GatewayProperties(
        @DefaultValue("5ms") @NotNull Duration minLatency,
        @DefaultValue("20ms") @NotNull Duration maxLatency,
        @DefaultValue("0.0") @DecimalMin("0.0") @DecimalMax("1.0") double failureRate,
        @DefaultValue("2s") @NotNull Duration timeout,
        @DefaultValue("512") @Min(1) int bulkheadMaxCalls,
        @DefaultValue Breaker breaker,
        @DefaultValue Retry retry) {

    /**
     * @param failureRateThreshold  % de fallos que abre el circuito
     * @param slidingWindowSize     llamadas que componen la ventana
     * @param minimumNumberOfCalls  llamadas mínimas antes de evaluar
     * @param waitDurationInOpen    tiempo en abierto antes de probar (half-open)
     */
    public record Breaker(
            @DefaultValue("50") @Min(1) int failureRateThreshold,
            @DefaultValue("20") @Min(1) int slidingWindowSize,
            @DefaultValue("10") @Min(1) int minimumNumberOfCalls,
            @DefaultValue("10s") @NotNull Duration waitDurationInOpen) {
    }

    /**
     * @param maxAttempts intentos adicionales tras el primer fallo transitorio
     * @param backoff     espera inicial (exponencial con jitter)
     */
    public record Retry(
            @DefaultValue("2") @Min(0) int maxAttempts,
            @DefaultValue("50ms") @NotNull Duration backoff) {
    }
}
