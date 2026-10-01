package com.trading.events.infrastructure.web.dto;

import java.util.List;

public record BatchResponse(int total, long completed, long failed, List<BatchItemResponse> results) {
}
