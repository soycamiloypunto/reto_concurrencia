package com.trading.events.application.concurrency;

import com.trading.events.domain.exception.OverloadedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AccountLaneDispatcher: exclusión mutua por clave, orden FIFO y backpressure")
class AccountLaneDispatcherTest {

    private Scheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = Schedulers.fromExecutorService(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()), "test-vt");
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @RepeatedTest(5)
    @DisplayName("tareas de la misma clave nunca se solapan y conservan el orden de envío")
    void sameKeyIsSerializedAndOrdered() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(8, 1000, scheduler);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<Integer> executionOrder = Collections.synchronizedList(new ArrayList<>());

        // Se envían en orden desde un solo hilo: el orden de ejecución debe ser el mismo.
        List<Mono<Integer>> submitted = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            int n = i;
            submitted.add(dispatcher.submit("ACC-1", () -> Mono.fromCallable(() -> {
                maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                executionOrder.add(n);
                Thread.sleep(1);
                running.decrementAndGet();
                return n;
            }).subscribeOn(scheduler)));
        }

        StepVerifier.create(Flux.merge(submitted).then()).expectComplete()
                .verify(Duration.ofSeconds(30));

        assertThat(maxRunning.get()).isEqualTo(1);
        assertThat(executionOrder).hasSize(200).isSorted();
    }

    @Test
    @DisplayName("claves en lanes distintas se ejecutan en paralelo")
    void differentLanesRunInParallel() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(16, 100, scheduler);
        List<String> keys = keysInDistinctLanes(dispatcher, 4);
        long start = System.nanoTime();

        StepVerifier.create(Flux.fromIterable(keys)
                        .flatMap(k -> dispatcher.submit(k, () -> Mono.delay(Duration.ofMillis(300)).then(Mono.just(k))))
                        .then())
                .expectComplete().verify(Duration.ofSeconds(10));

        long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
        // En serie serían >= 1200 ms; en paralelo ~300 ms.
        assertThat(elapsedMs).isLessThan(900);
    }

    @Test
    @DisplayName("una clave siempre cae en la misma lane y el índice siempre es válido")
    void laneIndexIsStableAndInRange() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(7, 10, scheduler);
        for (int i = 0; i < 10_000; i++) {
            String key = "ACC-" + i + "-" + Integer.toHexString(i * 31);
            int index = dispatcher.laneIndex(key);
            assertThat(index).isBetween(0, 6).isEqualTo(dispatcher.laneIndex(key));
        }
    }

    @Test
    @DisplayName("reparte claves en todas las lanes (no se concentra en pocas)")
    void spreadsKeys() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(64, 10, scheduler);
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            used.add(dispatcher.laneIndex("ACC-" + i));
        }
        assertThat(used).hasSize(64);
    }

    @Test
    @DisplayName("rechaza con OverloadedException cuando la cola de la lane está llena")
    void rejectsWhenLaneIsFull() throws InterruptedException {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(1, 2, scheduler);
        Sinks.Empty<Void> gate = Sinks.empty();
        CountDownLatch started = new CountDownLatch(1);

        Disposable running = dispatcher.submit("A", () -> {
            started.countDown();
            return gate.asMono().thenReturn("first");
        }).subscribe();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        // La primera ya está ejecutando: caben 2 en cola y la 3ª se rechaza.
        dispatcher.submit("A", () -> Mono.just("q1")).subscribe();
        dispatcher.submit("A", () -> Mono.just("q2")).subscribe();
        assertThat(dispatcher.queuedTasks()).isEqualTo(2);
        assertThat(dispatcher.maxLaneDepth()).isEqualTo(2);

        StepVerifier.create(dispatcher.submit("A", () -> Mono.just("overflow")))
                .expectError(OverloadedException.class).verify(Duration.ofSeconds(2));

        gate.tryEmitEmpty();
        running.dispose();
    }

    @Test
    @DisplayName("cancelar una tarea en cola evita que se ejecute")
    void cancelledQueuedTaskIsSkipped() throws InterruptedException {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(1, 10, scheduler);
        Sinks.Empty<Void> gate = Sinks.empty();
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger executed = new AtomicInteger();

        dispatcher.submit("A", () -> {
            started.countDown();
            return gate.asMono();
        }).subscribe();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        Disposable queued = dispatcher.submit("A", () -> {
            executed.incrementAndGet();
            return Mono.just("never");
        }).subscribe();
        queued.dispose();

        CountDownLatch next = new CountDownLatch(1);
        dispatcher.submit("A", () -> {
            next.countDown();
            return Mono.just("after");
        }).subscribe();

        gate.tryEmitEmpty();
        assertThat(next.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executed).hasValue(0);
    }

    @Test
    @DisplayName("cancelar una tarea en vuelo cancela su ejecución y libera la lane")
    void cancelInFlightReleasesLane() throws InterruptedException {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(1, 10, scheduler);
        CountDownLatch started = new CountDownLatch(1);
        Disposable inFlight = dispatcher.submit("A", () -> Mono.never().doOnSubscribe(s -> started.countDown()))
                .subscribe();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        inFlight.dispose();

        StepVerifier.create(dispatcher.submit("A", () -> Mono.just("ok")))
                .expectNext("ok").expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("un error en una tarea no bloquea ni contamina las siguientes")
    void failureDoesNotBlockTheLane() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(1, 10, scheduler);

        Mono<String> failing = dispatcher.submit("A", () -> Mono.error(new IllegalStateException("boom")));
        Mono<String> throwing = dispatcher.submit("A", () -> {
            throw new IllegalArgumentException("sync");
        });
        Mono<String> fine = dispatcher.submit("A", () -> Mono.just("ok"));

        StepVerifier.create(failing).expectError(IllegalStateException.class).verify(Duration.ofSeconds(5));
        StepVerifier.create(throwing).expectError(IllegalArgumentException.class).verify(Duration.ofSeconds(5));
        StepVerifier.create(fine).expectNext("ok").expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("una tarea que completa vacía también libera la lane")
    void emptyResultReleasesLane() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(1, 10, scheduler);
        StepVerifier.create(dispatcher.<String>submit("A", Mono::empty)).expectComplete()
                .verify(Duration.ofSeconds(5));
        StepVerifier.create(dispatcher.submit("A", () -> Mono.just("x"))).expectNext("x").expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("tras close() rechaza nuevas tareas")
    void closedDispatcherRejects() {
        AccountLaneDispatcher dispatcher = new AccountLaneDispatcher(2, 10, scheduler);
        dispatcher.close();
        StepVerifier.create(dispatcher.submit("A", () -> Mono.just("x")))
                .expectError(OverloadedException.class).verify(Duration.ofSeconds(2));
        assertThat(dispatcher.laneCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("valida los parámetros del constructor")
    void validatesArguments() {
        assertThatThrownBy(() -> new AccountLaneDispatcher(0, 10, scheduler)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AccountLaneDispatcher(1, 0, scheduler)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<String> keysInDistinctLanes(AccountLaneDispatcher dispatcher, int count) {
        Set<Integer> lanes = new HashSet<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; keys.size() < count; i++) {
            String key = "K-" + i;
            if (lanes.add(dispatcher.laneIndex(key))) {
                keys.add(key);
            }
        }
        return keys;
    }
}
