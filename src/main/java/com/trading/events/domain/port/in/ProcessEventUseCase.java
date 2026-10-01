package com.trading.events.domain.port.in;

import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.ProcessingResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Caso de uso principal: procesar eventos de trading de forma concurrente y segura.
 */
public interface ProcessEventUseCase {

    /**
     * Procesa un evento. Los eventos de una misma cuenta se ejecutan en orden y de uno en uno;
     * los de cuentas distintas, en paralelo.
     */
    Mono<Event> process(ProcessEventCommand command);

    /**
     * Procesa un lote con un máximo de eventos en vuelo. Los fallos individuales no abortan el lote.
     *
     * @param maxConcurrency máximo solicitado de eventos simultáneos (se acota al límite configurado)
     */
    Flux<ProcessingResult> processBatch(List<ProcessEventCommand> commands, int maxConcurrency);
}
