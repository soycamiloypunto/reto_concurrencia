package com.trading.events.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingResultTest {

    @Test
    void completedAndFailedResults() {
        Event done = Event.received(null, "A", EventType.DEPOSIT, BigDecimal.ONE, "X")
                .markAsProcessing().markAsCompleted();
        ProcessingResult ok = ProcessingResult.completed(done);
        ProcessingResult ko = ProcessingResult.failed(UUID.randomUUID(), "A", "boom");

        assertThat(ok.isSuccess()).isTrue();
        assertThat(ko.isSuccess()).isFalse();
        assertThat(ko.reason()).isEqualTo("boom");
    }
}
