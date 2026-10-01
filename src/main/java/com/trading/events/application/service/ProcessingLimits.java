package com.trading.events.application.service;

/**
 * Límites operativos que la capa de aplicación necesita conocer (sin depender de Spring Boot).
 */
public record ProcessingLimits(int maxBatchConcurrency) {
}
