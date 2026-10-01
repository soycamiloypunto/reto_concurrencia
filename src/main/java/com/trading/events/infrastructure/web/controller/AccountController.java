package com.trading.events.infrastructure.web.controller;

import com.trading.events.domain.port.in.QueryUseCase;
import com.trading.events.infrastructure.web.dto.AccountResponse;
import com.trading.events.infrastructure.web.mapper.AccountWebMapper;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {

    private final QueryUseCase queries;
    private final AccountWebMapper mapper;

    public AccountController(QueryUseCase queries, AccountWebMapper mapper) {
        this.queries = queries;
        this.mapper = mapper;
    }

    @GetMapping(value = "/{accountId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<AccountResponse> get(@PathVariable String accountId) {
        return queries.getAccount(accountId).map(mapper::toResponse);
    }
}
