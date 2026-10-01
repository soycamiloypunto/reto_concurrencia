package com.trading.events.infrastructure.web.controller;

import com.trading.events.domain.model.ProcessingResult;
import com.trading.events.domain.port.in.ProcessEventCommand;
import com.trading.events.domain.port.in.ProcessEventUseCase;
import com.trading.events.domain.port.in.QueryUseCase;
import com.trading.events.infrastructure.config.ConcurrencyProperties;
import com.trading.events.infrastructure.web.dto.BatchEventRequest;
import com.trading.events.infrastructure.web.dto.BatchResponse;
import com.trading.events.infrastructure.web.dto.EventRequest;
import com.trading.events.infrastructure.web.dto.EventResponse;
import com.trading.events.infrastructure.web.mapper.EventWebMapper;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

/**
 * Adaptador de entrada HTTP. No contiene lógica de negocio: valida, mapea y delega en los casos de uso.
 */
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final ProcessEventUseCase processEvent;
    private final QueryUseCase queries;
    private final EventWebMapper mapper;
    private final ConcurrencyProperties properties;

    public EventController(ProcessEventUseCase processEvent, QueryUseCase queries,
                           EventWebMapper mapper, ConcurrencyProperties properties) {
        this.processEvent = processEvent;
        this.queries = queries;
        this.mapper = mapper;
        this.properties = properties;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<EventResponse> receive(@Valid @RequestBody EventRequest request) {
        return processEvent.process(mapper.toCommand(request)).map(mapper::toResponse);
    }

    @PostMapping(value = "/batch", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<BatchResponse> receiveBatch(@Valid @RequestBody BatchEventRequest request) {
        if (request.events().size() > properties.maxBatchSize()) {
            return Mono.error(new ServerWebInputException(
                    "Batch too large: max " + properties.maxBatchSize() + " events"));
        }
        List<ProcessEventCommand> commands = request.events().stream().map(mapper::toCommand).toList();
        int concurrency = request.maxConcurrency() != null ? request.maxConcurrency() : properties.maxBatchConcurrency();
        return processEvent.processBatch(commands, concurrency)
                .collectList()
                .map(EventController::summarize)
                .map(summary -> new BatchResponse(summary.total(), summary.completed(), summary.failed(),
                        summary.results().stream().map(mapper::toItemResponse).toList()));
    }

    @GetMapping(value = "/{eventId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<EventResponse> get(@PathVariable UUID eventId) {
        return queries.getEvent(eventId).map(mapper::toResponse);
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<EventResponse> stream() {
        return queries.eventStream().map(mapper::toResponse);
    }

    private static Summary summarize(List<ProcessingResult> results) {
        long completed = results.stream().filter(ProcessingResult::isSuccess).count();
        return new Summary(results.size(), completed, results.size() - completed, results);
    }

    private record Summary(int total, long completed, long failed, List<ProcessingResult> results) {
    }
}
