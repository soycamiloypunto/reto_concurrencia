package com.trading.events.domain.port.out;

import java.time.Duration;

/**
 * Puerto de métricas: mantiene el dominio y la aplicación libres de librerías de monitoreo.
 */
public interface ProcessingMetrics {

    void received();

    void completed(Duration elapsed);

    void failed(String reason);

    void rejected();

    void duplicated();
}
