package com.trading.events.infrastructure.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.gateway.min-latency=1ms",
        "app.gateway.max-latency=2ms"
}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "60000")
@AutoConfigureObservability
@DisplayName("API REST de eventos (contexto Spring completo)")
class EventApiIntegrationTest {

    @Autowired
    private WebTestClient client;

    @LocalServerPort
    private int port;

    /** Cliente NO bloqueante: WebTestClient.exchange() bloquea y serializaría las "peticiones concurrentes". */
    private WebClient webClient() {
        return WebClient.create("http://localhost:" + port);
    }

    private static String account() {
        return "ACC-" + UUID.randomUUID();
    }

    private static Map<String, Object> body(String account, String type, String amount) {
        return Map.of("accountId", account, "eventType", type, "amount", amount, "marketSource", "NYSE");
    }

    private WebTestClient.ResponseSpec post(Map<String, Object> body) {
        return client.post().uri("/api/v1/events").contentType(MediaType.APPLICATION_JSON).bodyValue(body).exchange();
    }

    private void assertBalance(String account, String expected) {
        client.get().uri("/api/v1/accounts/{id}", account).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.balance").value(v -> assertThat(new java.math.BigDecimal(v.toString()))
                        .isEqualByComparingTo(expected));
    }

    @Test
    @DisplayName("POST evento válido => 200 COMPLETED y el balance queda actualizado")
    void processesEvent() {
        String acc = account();
        post(body(acc, "DEPOSIT", "250.75")).expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("COMPLETED")
                .jsonPath("$.accountId").isEqualTo(acc);
        assertBalance(acc, "250.75");
    }

    @Test
    @DisplayName("GET /events/{id} devuelve el evento procesado y 404 si no existe")
    void getEventById() {
        String acc = account();
        String id = UUID.randomUUID().toString();
        post(Map.of("eventId", id, "accountId", acc, "eventType", "DEPOSIT", "amount", "1", "marketSource", "X"))
                .expectStatus().isOk();

        client.get().uri("/api/v1/events/{id}", id).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.eventId").isEqualTo(id);
        client.get().uri("/api/v1/events/{id}", UUID.randomUUID()).exchange().expectStatus().isNotFound();
    }

    @Test
    @DisplayName("validación: monto negativo y campos faltantes => 400 con ProblemDetail y lista de errores")
    void validationErrors() {
        post(body(account(), "DEPOSIT", "-5")).expectStatus().isBadRequest()
                .expectBody().jsonPath("$.errors").isArray();
        post(Map.of("accountId", "")).expectStatus().isBadRequest()
                .expectBody().jsonPath("$.title").isEqualTo("Invalid request");
    }

    @Test
    @DisplayName("tipo de evento desconocido => 400")
    void unknownEventType() {
        post(body(account(), "FOO", "5")).expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("más de 4 decimales => 400 (evento inválido de dominio o de validación)")
    void tooManyDecimals() {
        post(body(account(), "DEPOSIT", "1.123456")).expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("retiro sin fondos => 422 y el balance no se crea")
    void insufficientFunds() {
        String acc = account();
        post(body(acc, "WITHDRAWAL", "10")).expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.title").isEqualTo("Insufficient funds");
        client.get().uri("/api/v1/accounts/{id}", acc).exchange().expectStatus().isNotFound();
    }

    @Test
    @DisplayName("idempotencia: reenviar el mismo eventId no vuelve a aplicar el evento")
    void idempotentRetry() {
        String acc = account();
        Map<String, Object> request = Map.of("eventId", UUID.randomUUID().toString(), "accountId", acc,
                "eventType", "DEPOSIT", "amount", "40", "marketSource", "X");
        post(request).expectStatus().isOk();
        post(request).expectStatus().isOk();
        post(request).expectStatus().isOk();
        assertBalance(acc, "40");
    }

    @Test
    @DisplayName("lote: reporta completados y fallidos por evento")
    void batch() {
        String acc = account();
        Map<String, Object> batch = Map.of("maxConcurrency", 4, "events", java.util.List.of(
                body(acc, "DEPOSIT", "10"),
                body(acc, "DEPOSIT", "10"),
                body(account(), "WITHDRAWAL", "99")));

        client.post().uri("/api/v1/events/batch").contentType(MediaType.APPLICATION_JSON).bodyValue(batch)
                .exchange().expectStatus().isOk()
                .expectBody()
                .jsonPath("$.total").isEqualTo(3)
                .jsonPath("$.completed").isEqualTo(2)
                .jsonPath("$.failed").isEqualTo(1);
        assertBalance(acc, "20");
    }

    @Test
    @DisplayName("lote vacío o más grande que el máximo => 400")
    void batchLimits() {
        client.post().uri("/api/v1/events/batch").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("events", java.util.List.of())).exchange().expectStatus().isBadRequest();

        java.util.List<Map<String, Object>> tooMany = java.util.stream.IntStream.range(0, 1001)
                .mapToObj(i -> body("X", "DEPOSIT", "1")).toList();
        client.post().uri("/api/v1/events/batch").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("events", tooMany)).exchange().expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("500 depósitos HTTP concurrentes a la misma cuenta => balance exacto de 500")
    void concurrentRequestsOnSameAccount() {
        String acc = account();

        WebClient web = webClient();
        Long ok = Flux.range(0, 500)
                .flatMap(i -> web.post().uri("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(body(acc, "DEPOSIT", "1"))
                        .exchangeToMono(r -> r.releaseBody().thenReturn(r.statusCode().value())), 100)
                .filter(status -> status == 200)
                .count().block(Duration.ofSeconds(60));

        assertThat(ok).isEqualTo(500);
        assertBalance(acc, "500");
    }

    @Test
    @DisplayName("el stream SSE entrega los eventos procesados")
    void streamDeliversEvents() {
        String acc = account();
        Flux<String> sse = webClient().get().uri("/api/v1/events/stream").accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve().bodyToFlux(String.class)
                .filter(line -> line.contains(acc)).take(1);

        // Se publica repetidamente hasta que el suscriptor SSE (que se conecta de forma asíncrona) lo recibe.
        Disposable trigger = Flux.interval(Duration.ofMillis(200))
                .flatMap(i -> Mono.fromRunnable(() -> post(body(acc, "DEPOSIT", "3")).expectStatus().isOk())
                        .subscribeOn(Schedulers.boundedElastic()))
                .subscribe();
        try {
            StepVerifier.create(sse).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(20));
        } finally {
            trigger.dispose();
        }
    }

    @Test
    @DisplayName("expone métricas de Prometheus del procesamiento y de las lanes")
    void exposesMetrics() {
        post(body(account(), "DEPOSIT", "1")).expectStatus().isOk();
        client.get().uri("/actuator/prometheus").exchange().expectStatus().isOk()
                .expectBody(String.class).value(text -> assertThat(text)
                        .contains("events_received_total")
                        .contains("events_processing_duration_seconds")
                        .contains("events_lanes_queued")
                        .contains("events_lanes_count"));
    }

    @Test
    @DisplayName("health está UP")
    void health() {
        client.get().uri("/actuator/health").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
    }
}
