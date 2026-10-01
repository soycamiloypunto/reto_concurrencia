package com.trading.events.domain.exception;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Un débito dejaría el balance en negativo. Error de negocio: no es reintentable.
 */
public class InsufficientFundsException extends EventProcessingException {

    public InsufficientFundsException(UUID eventId, String accountId, String message) {
        super(eventId, accountId, message);
    }

    public static InsufficientFundsException of(UUID eventId, String accountId,
                                                BigDecimal balance, BigDecimal requested) {
        return new InsufficientFundsException(eventId, accountId,
                "Insufficient funds in account " + accountId + ": balance=" + balance + ", requested=" + requested);
    }
}
