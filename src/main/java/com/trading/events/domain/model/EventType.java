package com.trading.events.domain.model;

/**
 * Tipos de eventos soportados. Cada tipo define si acredita o debita el balance de la cuenta.
 */
public enum EventType {
    DEPOSIT(true),
    INTEREST_CALCULATION(true),
    WITHDRAWAL(false),
    FEE_APPLIED(false),
    TRADE_EXECUTION(false);

    private final boolean credit;

    EventType(boolean credit) {
        this.credit = credit;
    }

    /**
     * @return true si el evento suma al balance, false si lo resta.
     */
    public boolean isCredit() {
        return credit;
    }
}
