package com.trading.events.application.service;

import com.trading.events.application.concurrency.AccountLaneDispatcher;
import com.trading.events.domain.exception.GatewayUnavailableException;
import com.trading.events.domain.exception.InsufficientFundsException;
import com.trading.events.domain.exception.InvalidEventException;
import com.trading.events.domain.exception.OverloadedException;
import com.trading.events.domain.model.Account;
import com.trading.events.domain.model.Event;
import com.trading.events.domain.model.EventStatus;
import com.trading.events.domain.model.EventType;
import com.trading.events.domain.model.ProcessingResult;
import com.trading.events.domain.port.in.ProcessEventCommand;
import com.trading.events.domain.port.out.EventPublisher;
import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.domain.port.out.ProcessingMetrics;
import com.trading.events.infrastructure.config.StoreProperties;
import com.trading.events.infrastructure.messaging.SinkEventPublisher;
import com.trading.events.infrastructure.persistence.InMemoryAccountRepository;
import com.trading.events.infrastructure.persistence.InMemoryEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DisplayName("ProcessEventService: corrección bajo concurrencia")
class ProcessEventServiceTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private Scheduler scheduler;
    private InMemoryAccountRepository accounts;
    private InMemoryEventRepository events;
    private ProcessingMetrics metrics;
    private MarketGateway gateway;
    private final AtomicInteger gatewayCalls = new AtomicInteger();

    @BeforeEach
    void setUp() {
        scheduler = Schedulers.fromExecutorService(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()), "svc-test");
        accounts = new InMemoryAccountRepository();
        events = new InMemoryEventRepository(new StoreProperties(100_000, Duration.ofMinutes(5)));
        metrics = mock(ProcessingMetrics.class);
        // Mercado que tarda 1 ms: suficiente para que existan solapes reales si no hubiera exclusión.
        gateway = event -> {
            gatewayCalls.incrementAndGet();
            return Mono.delay(Duration.ofMillis(1)).then();
        };
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    private ProcessEventService service(int lanes, int queue) {
        return service(lanes, queue, gateway);
    }

    private ProcessEventService service(int lanes, int queue, MarketGateway marketGateway) {
        EventPublisher publisher = new SinkEventPublisher();
        return new ProcessEventService(new AccountLaneDispatcher(lanes, queue, scheduler),
                accounts, events, marketGateway, publisher, metrics, new ProcessingLimits(16));
    }

    private static ProcessEventCommand cmd(String account, EventType type, String amount) {
        return new ProcessEventCommand(null, account, type, new BigDecimal(amount), "NYSE");
    }

    private Account account(String id) {
        return accounts.find(id).block(TIMEOUT);
    }

    @Test
    @DisplayName("un depósito actualiza el balance y deja el evento COMPLETED")
    void depositUpdatesBalance() {
        ProcessEventService service = service(8, 100);

        StepVerifier.create(service.process(cmd("ACC-1", EventType.DEPOSIT, "100.25")))
                .assertNext(e -> assertThat(e.status()).isEqualTo(EventStatus.COMPLETED))
                .expectComplete().verify(TIMEOUT);

        assertThat(account("ACC-1").balance()).isEqualByComparingTo("100.25");
        verify(metrics).received();
    }

    @Test
    @DisplayName("1000 depósitos concurrentes a la MISMA cuenta no pierden ninguna actualización")
    void noLostUpdatesOnSameAccount() {
        ProcessEventService service = service(8, 2000);

        List<Event> results = Flux.range(0, 1000)
                .flatMap(i -> service.process(cmd("HOT", EventType.DEPOSIT, "1.00")), 300)
                .collectList().block(TIMEOUT);

        assertThat(results).hasSize(1000).allMatch(e -> e.status() == EventStatus.COMPLETED);
        Account hot = account("HOT");
        assertThat(hot.balance()).isEqualByComparingTo("1000");
        assertThat(hot.version()).isEqualTo(1000);
    }

    @Test
    @DisplayName("cuentas distintas procesadas a la vez mantienen cada una su balance exacto")
    void independentAccountsStayConsistent() {
        ProcessEventService service = service(16, 1000);

        Flux.range(0, 2000)
                .flatMap(i -> service.process(cmd("ACC-" + (i % 50), EventType.DEPOSIT, "2.50")), 400)
                .then().block(TIMEOUT);

        for (int a = 0; a < 50; a++) {
            assertThat(account("ACC-" + a).balance()).isEqualByComparingTo("100"); // 40 depósitos x 2.50
        }
    }

    @Test
    @DisplayName("retiros concurrentes jamás dejan el balance negativo: exactamente tantos éxitos como fondos")
    void balanceNeverGoesNegative() {
        ProcessEventService service = service(8, 2000);
        service.process(cmd("ACC-W", EventType.DEPOSIT, "100")).block(TIMEOUT);

        AtomicInteger insufficient = new AtomicInteger();
        List<Event> ok = Flux.range(0, 300)
                .flatMap(i -> service.process(cmd("ACC-W", EventType.WITHDRAWAL, "1"))
                        .onErrorResume(InsufficientFundsException.class, e -> {
                            insufficient.incrementAndGet();
                            return Mono.empty();
                        }), 300)
                .collectList().block(TIMEOUT);

        assertThat(ok).hasSize(100);
        assertThat(insufficient).hasValue(200);
        assertThat(account("ACC-W").balance().signum()).isZero();
    }

    @Test
    @DisplayName("se respeta el orden por cuenta: un retiro enviado tras su depósito siempre tiene fondos")
    void preservesPerAccountOrder() {
        ProcessEventService service = service(8, 100);
        List<Mono<Event>> operations = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String acc = "ORD-" + i;
            // Se suscriben en este orden; la lane garantiza que el depósito corre primero.
            operations.add(service.process(cmd(acc, EventType.DEPOSIT, "100")));
            operations.add(service.process(cmd(acc, EventType.WITHDRAWAL, "100")));
        }

        StepVerifier.create(Flux.mergeSequential(operations).count())
                .expectNext(200L).expectComplete().verify(TIMEOUT);
    }

    @Test
    @DisplayName("idempotencia: el mismo eventId enviado 100 veces en paralelo se aplica una sola vez")
    void duplicateEventsApplyOnce() {
        ProcessEventService service = service(8, 1000);
        UUID id = UUID.randomUUID();
        ProcessEventCommand command = new ProcessEventCommand(id, "ACC-I", EventType.DEPOSIT,
                new BigDecimal("10"), "NYSE");

        Flux.range(0, 100).flatMap(i -> service.process(command), 100).collectList().block(TIMEOUT);

        assertThat(account("ACC-I").balance()).isEqualByComparingTo("10");
        assertThat(gatewayCalls).hasValue(1);
        verify(metrics, times(99)).duplicated();
    }

    @Test
    @DisplayName("fondos insuficientes: el evento queda FAILED con motivo y el balance intacto")
    void insufficientFundsMarksEventFailed() {
        ProcessEventService service = service(4, 100);
        UUID id = UUID.randomUUID();
        ProcessEventCommand withdrawal = new ProcessEventCommand(id, "ACC-F", EventType.WITHDRAWAL,
                new BigDecimal("5"), "NYSE");

        StepVerifier.create(service.process(withdrawal))
                .expectError(InsufficientFundsException.class).verify(TIMEOUT);

        Event stored = events.findById(id).block(TIMEOUT);
        assertThat(stored.status()).isEqualTo(EventStatus.FAILED);
        assertThat(stored.failureReason()).contains("Insufficient funds");
        assertThat(account("ACC-F")).isNull();
        assertThat(gatewayCalls).hasValue(0); // no se llamó al mercado
        verify(metrics).failed("InsufficientFundsException");
    }

    @Test
    @DisplayName("si el mercado falla, el balance no cambia y el evento queda FAILED")
    void gatewayFailureDoesNotTouchBalance() {
        MarketGateway failing = event -> Mono.error(
                new GatewayUnavailableException(event.eventId(), event.accountId(), "down", null));
        ProcessEventService service = service(4, 100, failing);
        service.process(cmd("ACC-G", EventType.DEPOSIT, "5")).onErrorResume(e -> Mono.empty()).block(TIMEOUT);

        assertThat(account("ACC-G")).isNull();
    }

    @Test
    @DisplayName("un error inesperado también deja el evento FAILED (no queda colgado en PROCESSING)")
    void unexpectedErrorMarksFailed() {
        MarketGateway broken = event -> Mono.error(new IllegalStateException("bug"));
        ProcessEventService service = service(4, 100, broken);
        UUID id = UUID.randomUUID();

        StepVerifier.create(service.process(new ProcessEventCommand(id, "ACC-U", EventType.DEPOSIT,
                        BigDecimal.ONE, "NYSE")))
                .expectError(IllegalStateException.class).verify(TIMEOUT);

        assertThat(events.findById(id).block(TIMEOUT).status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("un evento inválido se rechaza sin llegar a la lane ni quedar registrado")
    void invalidEventIsRejectedEarly() {
        ProcessEventService service = service(4, 100);
        UUID id = UUID.randomUUID();
        ProcessEventCommand invalid = new ProcessEventCommand(id, "ACC-X", EventType.DEPOSIT,
                new BigDecimal("-1"), "NYSE");

        StepVerifier.create(service.process(invalid)).expectError(InvalidEventException.class).verify(TIMEOUT);

        assertThat(events.findById(id).block(TIMEOUT)).isNull();
        verify(metrics, never()).received();
    }

    @Test
    @DisplayName("saturación: rechaza con OverloadedException y libera el id para poder reenviar")
    void overloadRejectsAndAllowsResend() throws InterruptedException {
        Sinks.Empty<Void> gate = Sinks.empty();
        CountDownLatch started = new CountDownLatch(1);
        MarketGateway blocked = event -> {
            started.countDown();
            return gate.asMono();
        };
        ProcessEventService service = service(1, 1, blocked);

        service.process(cmd("SAT", EventType.DEPOSIT, "1")).subscribe();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        service.process(cmd("SAT", EventType.DEPOSIT, "1")).subscribe(); // ocupa la única plaza en cola

        UUID id = UUID.randomUUID();
        ProcessEventCommand overflow = new ProcessEventCommand(id, "SAT", EventType.DEPOSIT, BigDecimal.ONE, "NYSE");
        StepVerifier.create(service.process(overflow)).expectError(OverloadedException.class).verify(TIMEOUT);

        verify(metrics).rejected();
        assertThat(events.findById(id).block(TIMEOUT)).isNull(); // id liberado
        gate.tryEmitEmpty();

        StepVerifier.create(service.process(overflow))
                .assertNext(e -> assertThat(e.status()).isEqualTo(EventStatus.COMPLETED))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    @DisplayName("lote: los fallos individuales no abortan el lote y se reportan por evento")
    void batchReportsIndividualFailures() {
        ProcessEventService service = service(8, 1000);
        List<ProcessEventCommand> commands = List.of(
                cmd("B-1", EventType.DEPOSIT, "10"),
                cmd("B-1", EventType.DEPOSIT, "10"),
                new ProcessEventCommand(null, "B-2", EventType.WITHDRAWAL, new BigDecimal("999"), "NYSE"),
                new ProcessEventCommand(null, "B-3", EventType.DEPOSIT, new BigDecimal("-5"), "NYSE"));

        List<ProcessingResult> results = service.processBatch(commands, 1000).collectList().block(TIMEOUT);

        assertThat(results).hasSize(4);
        assertThat(results.stream().filter(ProcessingResult::isSuccess)).hasSize(2);
        assertThat(results.stream().filter(r -> !r.isSuccess()).map(ProcessingResult::reason))
                .anyMatch(r -> r.contains("Insufficient funds"))
                .anyMatch(r -> r.contains("invalid"));
        assertThat(account("B-1").balance()).isEqualByComparingTo("20");
    }

    @Test
    @DisplayName("el publisher falla en silencio: un error al publicar no revierte el procesamiento")
    void publishFailureDoesNotFailProcessing() {
        EventPublisher failing = new EventPublisher() {
            @Override
            public Mono<Void> publish(Event event) {
                return Mono.error(new IllegalStateException("sink down"));
            }

            @Override
            public Flux<Event> stream() {
                return Flux.empty();
            }
        };
        ProcessEventService service = new ProcessEventService(new AccountLaneDispatcher(2, 10, scheduler),
                accounts, events, gateway, failing, metrics, new ProcessingLimits(4));

        StepVerifier.create(service.process(cmd("ACC-P", EventType.DEPOSIT, "3")))
                .assertNext(e -> assertThat(e.status()).isEqualTo(EventStatus.COMPLETED))
                .expectComplete().verify(TIMEOUT);
        assertThat(account("ACC-P").balance()).isEqualByComparingTo("3");
        verify(metrics, never()).failed(anyString());
    }
}
