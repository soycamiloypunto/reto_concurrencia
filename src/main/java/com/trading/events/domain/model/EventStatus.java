package com.trading.events.domain.model;

/**
 * Estados de un evento durante su ciclo de vida.
 */
public enum EventStatus {
    RECEIVED,
    PROCESSING,
    COMPLETED,
    FAILED
}
