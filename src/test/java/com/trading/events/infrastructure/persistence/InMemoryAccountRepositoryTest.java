package com.trading.events.infrastructure.persistence;

import com.trading.events.domain.exception.InsufficientFundsException;
import com.trading.events.domain.model.Account;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryAccountRepository: atomicidad del read-modify-write")
class InMemoryAccountRepositoryTest {

    private static Event deposit(String amount) {
        return Event.received(null, "ACC", EventType.DEPOSIT, new BigDecimal(amount), "X");
    }

    @Test
    @DisplayName("DEMO del bug: get + put NO atómico pierde actualizaciones (lost update)")
    void naiveReadModifyWriteLosesUpdates() throws Exception {
        ConcurrentHashMap<String, Integer> naive = new ConcurrentHashMap<>();
        naive.put("ACC", 0);
        CyclicBarrier bothHaveRead = new CyclicBarrier(2);

        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                futures.add(pool.submit(() -> {
                    int read = naive.get("ACC");          // 1) leen el mismo valor (0)
                    bothHaveRead.await();                  // 2) ninguno ha escrito todavía
                    naive.put("ACC", read + 1);            // 3) ambos escriben 1
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }

        // Dos incrementos, pero el resultado es 1: se perdió una actualización.
        assertThat(naive.get("ACC")).isEqualTo(1);
    }

    @Test
    @DisplayName("update() atómico: 16 hilos x 1000 depósitos suman exactamente 16000")
    void atomicUpdateNeverLosesWrites() throws Exception {
        InMemoryAccountRepository repository = new InMemoryAccountRepository();
        Event deposit = deposit("1");

        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < 1000; i++) {
                        repository.update("ACC", account -> account.apply(deposit)).block();
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }

        Account account = repository.find("ACC").block();
        assertThat(account.balance()).isEqualByComparingTo("16000");
        assertThat(account.version()).isEqualTo(16_000);
    }

    @Test
    @DisplayName("si la función de cambio lanza, la cuenta queda intacta")
    void failedUpdateLeavesAccountUntouched() {
        InMemoryAccountRepository repository = new InMemoryAccountRepository();
        repository.update("ACC", a -> a.apply(deposit("5"))).block();
        Event withdrawal = Event.received(null, "ACC", EventType.WITHDRAWAL, new BigDecimal("50"), "X");

        assertThatThrownBy(() -> repository.update("ACC", a -> a.apply(withdrawal)).block())
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(repository.find("ACC").block().balance()).isEqualByComparingTo("5");
    }

    @Test
    @DisplayName("find de una cuenta inexistente devuelve vacío")
    void findMissingIsEmpty() {
        assertThat(new InMemoryAccountRepository().find("none").block()).isNull();
    }
}
