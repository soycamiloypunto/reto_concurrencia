package com.trading.events.load;

import com.trading.events.application.concurrency.AccountLaneDispatcher;
import com.trading.events.application.service.ProcessEventService;
import com.trading.events.application.service.ProcessingLimits;
import com.trading.events.domain.model.EventType;
import com.trading.events.domain.port.in.ProcessEventCommand;
import com.trading.events.domain.port.out.ProcessingMetrics;
import com.trading.events.infrastructure.config.GatewayProperties;
import com.trading.events.infrastructure.config.StoreProperties;
import com.trading.events.infrastructure.gateway.SimulatedMarketGateway;
import com.trading.events.infrastructure.messaging.SinkEventPublisher;
import com.trading.events.infrastructure.persistence.InMemoryAccountRepository;
import com.trading.events.infrastructure.persistence.InMemoryEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prueba de carga/escalabilidad. Excluida del build normal; se ejecuta con:
 * {@code mvn test -Pload}. Escribe una tabla Markdown en {@code target/load-report.md}.
 *
 * <p>Compara cuántos eventos/segundo procesa el sistema variando el número de lanes y la
 * distribución de cuentas, con un mercado simulado de latencia fija (bloqueante, en hilos virtuales).
 * {@code lanes = 1} equivale a un lock global (todo serializado) y es la LÍNEA BASE.
 */
@Tag("load")
@DisplayName("Carga: throughput y latencia según lanes y distribución de cuentas")
class ThroughputLoadTest {

    private static final int EVENTS = 2_000;
    private static final long MARKET_LATENCY_MS = 10;
    private static final int CLIENT_CONCURRENCY = 1_000;

    private record Scenario(String name, int lanes, int accounts) {
    }

    private record Result(Scenario scenario, double throughput, long p50, long p95, long p99, long elapsedMs) {
    }

    @Test
    void measureScalability() throws IOException {
        List<Scenario> scenarios = List.of(
                new Scenario("Línea base: lock global (1 lane)", 1, 1000),
                new Scenario("16 lanes", 16, 1000),
                new Scenario("64 lanes", 64, 1000),
                new Scenario("256 lanes", 256, 1000),
                new Scenario("64 lanes, 100 cuentas", 64, 100),
                new Scenario("Cuenta caliente (1 cuenta)", 64, 1));

        List<Result> results = new ArrayList<>();
        for (Scenario scenario : scenarios) {
            results.add(run(scenario));
        }

        String report = render(results);
        System.out.println(report);
        Files.writeString(Path.of("target", "load-report.md"), report);

        double baseline = results.get(0).throughput();
        double scaled = results.get(2).throughput();
        assertThat(scaled).as("64 lanes debe escalar claramente frente a la línea base").isGreaterThan(baseline * 5);
    }

    private Result run(Scenario scenario) {
        Scheduler scheduler = Schedulers.fromExecutorService(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()), "load");
        try {
            InMemoryAccountRepository accounts = new InMemoryAccountRepository();
            GatewayProperties gatewayProps = new GatewayProperties(Duration.ofMillis(MARKET_LATENCY_MS),
                    Duration.ofMillis(MARKET_LATENCY_MS), 0.0, Duration.ofSeconds(5), 10_000,
                    new GatewayProperties.Breaker(50, 20, 10, Duration.ofSeconds(10)),
                    new GatewayProperties.Retry(0, Duration.ofMillis(1)));
            ProcessEventService service = new ProcessEventService(
                    new AccountLaneDispatcher(scenario.lanes(), EVENTS + 1, scheduler), accounts,
                    new InMemoryEventRepository(new StoreProperties(1_000_000, Duration.ofMinutes(10))),
                    new SimulatedMarketGateway(gatewayProps, scheduler), new SinkEventPublisher(),
                    noopMetrics(), new ProcessingLimits(64));

            List<Long> latencies = Collections.synchronizedList(new ArrayList<>(EVENTS));
            long start = System.nanoTime();
            Long done = Flux.range(0, EVENTS)
                    .flatMap(i -> {
                        long t0 = System.nanoTime();
                        return service.process(new ProcessEventCommand(null, "ACC-" + (i % scenario.accounts()),
                                        EventType.DEPOSIT, BigDecimal.ONE, "NYSE"))
                                .doOnSuccess(e -> latencies.add(System.nanoTime() - t0))
                                .onErrorResume(e -> Mono.empty());
                    }, CLIENT_CONCURRENCY)
                    .count().block(Duration.ofMinutes(5));
            long elapsedNanos = System.nanoTime() - start;

            assertThat(done).isEqualTo(EVENTS);
            // Corrección bajo carga: la suma de todos los balances == eventos procesados.
            BigDecimal total = BigDecimal.ZERO;
            for (int a = 0; a < scenario.accounts(); a++) {
                total = total.add(accounts.find("ACC-" + a).block().balance());
            }
            assertThat(total).isEqualByComparingTo(String.valueOf(EVENTS));

            List<Long> sorted = new ArrayList<>(latencies);
            Collections.sort(sorted);
            return new Result(scenario, EVENTS / (elapsedNanos / 1e9),
                    percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99),
                    Duration.ofNanos(elapsedNanos).toMillis());
        } finally {
            scheduler.dispose();
        }
    }

    private static long percentile(List<Long> sorted, double p) {
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1);
        return Duration.ofNanos(sorted.get(Math.max(0, index))).toMillis();
    }

    private static ProcessingMetrics noopMetrics() {
        return new ProcessingMetrics() {
            @Override public void received() { /* sin métricas en la prueba */ }
            @Override public void completed(Duration elapsed) { /* sin métricas en la prueba */ }
            @Override public void failed(String reason) { /* sin métricas en la prueba */ }
            @Override public void rejected() { /* sin métricas en la prueba */ }
            @Override public void duplicated() { /* sin métricas en la prueba */ }
        };
    }

    private static String render(List<Result> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("| Escenario | Lanes | Cuentas | Eventos | Tiempo (ms) | Throughput (ev/s) | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        sb.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Result r : results) {
            sb.append(String.format("| %s | %d | %d | %d | %d | %.0f | %d | %d | %d |%n",
                    r.scenario().name(), r.scenario().lanes(), r.scenario().accounts(), EVENTS,
                    r.elapsedMs(), r.throughput(), r.p50(), r.p95(), r.p99()));
        }
        return sb.toString();
    }
}
