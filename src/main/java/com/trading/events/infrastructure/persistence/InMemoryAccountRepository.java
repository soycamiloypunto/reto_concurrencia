package com.trading.events.infrastructure.persistence;

import com.trading.events.domain.model.Account;
import com.trading.events.domain.port.out.AccountRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * Cuentas en memoria. La atomicidad por cuenta la da {@link ConcurrentHashMap#compute}: la
 * función de cambio se ejecuta con la entrada bloqueada, por lo que dos actualizaciones de la
 * misma cuenta jamás se intercalan (segunda línea de defensa tras las lanes).
 */
@Repository
public class InMemoryAccountRepository implements AccountRepository {

    private final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();

    @Override
    public Mono<Account> find(String accountId) {
        return Mono.fromSupplier(() -> accounts.get(accountId));
    }

    @Override
    public Mono<Account> update(String accountId, UnaryOperator<Account> change) {
        return Mono.fromSupplier(() -> accounts.compute(accountId,
                (id, current) -> change.apply(current != null ? current : Account.open(id))));
    }
}
