package com.trading.events.domain.port.out;

import com.trading.events.domain.model.Account;
import reactor.core.publisher.Mono;

import java.util.function.UnaryOperator;

/**
 * Puerto de persistencia de cuentas.
 */
public interface AccountRepository {

    Mono<Account> find(String accountId);

    /**
     * Actualiza la cuenta de forma ATÓMICA por clave: la función se aplica sobre el valor vigente
     * sin que otro hilo pueda intercalar una escritura (se abre la cuenta si no existe). Si la
     * función lanza una excepción, la cuenta queda intacta.
     */
    Mono<Account> update(String accountId, UnaryOperator<Account> change);
}
