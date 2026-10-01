package com.trading.events.domain.port.out;

import com.trading.events.domain.model.Event;
import reactor.core.publisher.Mono;

/**
 * Puerto hacia el mercado externo que confirma/ejecuta la operación.
 */
public interface MarketGateway {

    Mono<Void> execute(Event event);
}
