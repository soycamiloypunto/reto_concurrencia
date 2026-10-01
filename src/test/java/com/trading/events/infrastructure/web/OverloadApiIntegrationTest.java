package com.trading.events.infrastructure.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.concurrency.lanes=1",
        "app.concurrency.lane-queue-capacity=3",
        "app.gateway.min-latency=200ms",
        "app.gateway.max-latency=200ms"
}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "60000")
@DisplayName("Backpressure end-to-end: saturación => 429 + Retry-After, sin caída ni pérdida de los aceptados")
class OverloadApiIntegrationTest {

    @Autowired
    private WebTestClient client;

    @LocalServerPort
    private int port;

    private record Reply(int status, HttpHeaders headers) {
    }

    /** Dispara N peticiones realmente simultáneas (cliente no bloqueante). */
    private List<Reply> fire(String account, int count) {
        WebClient web = WebClient.create("http://localhost:" + port);
        Map<String, Object> deposit = Map.of("accountId", account, "eventType", "DEPOSIT",
                "amount", "1", "marketSource", "X");
        return Flux.range(0, count)
                .flatMap(i -> web.post().uri("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(deposit)
                        .exchangeToMono(r -> r.releaseBody()
                                .thenReturn(new Reply(r.statusCode().value(), r.headers().asHttpHeaders()))), count)
                .collectList().block(Duration.ofSeconds(60));
    }

    @Test
    void rejectsExcessWith429AndKeepsAcceptedConsistent() {
        List<Reply> replies = fire("HOT", 20);

        long ok = replies.stream().filter(r -> r.status() == 200).count();
        long rejected = replies.stream().filter(r -> r.status() == 429).count();

        assertThat(ok + rejected).isEqualTo(20);
        assertThat(rejected).as("alguna petición debió rechazarse por saturación").isPositive();
        assertThat(ok).as("las aceptadas se procesan").isPositive();

        client.get().uri("/api/v1/accounts/HOT").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.balance").value(v ->
                        assertThat(new java.math.BigDecimal(v.toString())).isEqualByComparingTo(String.valueOf(ok)));
    }

    @Test
    void rejectionCarriesRetryAfterHeader() {
        List<HttpHeaders> rejectedHeaders = fire("HDR", 20).stream()
                .filter(r -> r.status() == 429).map(Reply::headers).toList();

        assertThat(rejectedHeaders).isNotEmpty().allMatch(h -> "1".equals(h.getFirst(HttpHeaders.RETRY_AFTER)));
    }
}
