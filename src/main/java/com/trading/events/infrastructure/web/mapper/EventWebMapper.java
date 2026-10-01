package com.trading.events.infrastructure.web.mapper;

import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.ProcessingResult;
import com.trading.events.domain.port.in.ProcessEventCommand;
import com.trading.events.infrastructure.web.dto.BatchItemResponse;
import com.trading.events.infrastructure.web.dto.EventRequest;
import com.trading.events.infrastructure.web.dto.EventResponse;
import org.mapstruct.Mapper;

/**
 * Traducciones DTO <-> dominio (el dominio nunca se expone directamente en la API).
 */
@Mapper
public interface EventWebMapper {

    ProcessEventCommand toCommand(EventRequest request);

    EventResponse toResponse(Event event);

    BatchItemResponse toItemResponse(ProcessingResult result);
}
