package com.trading.events.infrastructure.web.mapper;

import com.trading.events.domain.model.Account;
import com.trading.events.infrastructure.web.dto.AccountResponse;
import org.mapstruct.Mapper;

@Mapper
public interface AccountWebMapper {

    AccountResponse toResponse(Account account);
}
