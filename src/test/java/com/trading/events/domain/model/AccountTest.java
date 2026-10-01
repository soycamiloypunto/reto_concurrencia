package com.trading.events.domain.model;

import com.trading.events.domain.exception.InsufficientFundsException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Account: aplicación pura de eventos al balance")
class AccountTest {

    private static Event of(EventType type, String amount) {
        return Event.received(null, "ACC-1", type, new BigDecimal(amount), "NYSE");
    }

    @Test
    @DisplayName("una cuenta nueva parte en cero y versión 0")
    void openAccount() {
        Account account = Account.open("ACC-1");
        assertThat(account.balance()).isEqualByComparingTo("0");
        assertThat(account.version()).isZero();
    }

    @Test
    @DisplayName("créditos suman, débitos restan, y cada cambio incrementa la versión")
    void appliesCreditsAndDebits() {
        Account account = Account.open("ACC-1")
                .apply(of(EventType.DEPOSIT, "100"))
                .apply(of(EventType.INTEREST_CALCULATION, "5.5"))
                .apply(of(EventType.WITHDRAWAL, "10"))
                .apply(of(EventType.FEE_APPLIED, "0.5"))
                .apply(of(EventType.TRADE_EXECUTION, "25"));
        assertThat(account.balance()).isEqualByComparingTo("70");
        assertThat(account.version()).isEqualTo(5);
    }

    @Test
    @DisplayName("no muta la cuenta original (inmutabilidad)")
    void isImmutable() {
        Account original = Account.open("ACC-1");
        original.apply(of(EventType.DEPOSIT, "100"));
        assertThat(original.balance()).isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("rechaza un débito que dejaría el balance negativo")
    void rejectsOverdraft() {
        Account account = Account.open("ACC-1").apply(of(EventType.DEPOSIT, "10"));
        assertThatThrownBy(() -> account.apply(of(EventType.WITHDRAWAL, "10.0001")))
                .isInstanceOf(InsufficientFundsException.class);
    }

    @Test
    @DisplayName("permite dejar el balance exactamente en cero")
    void allowsExactZero() {
        Account account = Account.open("ACC-1").apply(of(EventType.DEPOSIT, "10"))
                .apply(of(EventType.WITHDRAWAL, "10"));
        assertThat(account.balance().signum()).isZero();
    }
}
