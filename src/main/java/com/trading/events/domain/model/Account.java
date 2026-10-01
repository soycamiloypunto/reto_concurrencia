package com.trading.events.domain.model;

import com.trading.events.domain.exception.InsufficientFundsException;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Cuenta de trading inmutable. {@link #apply(Event)} es una función pura: calcula el nuevo
 * estado sin mutar el actual, lo que permite aplicarla de forma atómica desde el repositorio
 * (read-modify-write sin condiciones de carrera).
 *
 * @param version contador monotónico de actualizaciones (detecta y audita la secuencia de cambios)
 */
public record Account(String accountId, BigDecimal balance, long version, Instant updatedAt) {

    private static final int SCALE = Event.MAX_AMOUNT_SCALE;

    public static Account open(String accountId) {
        return new Account(accountId, BigDecimal.ZERO.setScale(SCALE), 0L, Instant.now());
    }

    /**
     * Aplica el efecto del evento sobre el balance.
     *
     * @throws InsufficientFundsException si un débito dejaría el balance negativo
     */
    public Account apply(Event event) {
        BigDecimal delta = event.eventType().isCredit() ? event.amount() : event.amount().negate();
        BigDecimal newBalance = balance.add(delta).setScale(SCALE);
        if (newBalance.signum() < 0) {
            throw InsufficientFundsException.of(event.eventId(), accountId, balance, event.amount());
        }
        return new Account(accountId, newBalance, version + 1, Instant.now());
    }
}
