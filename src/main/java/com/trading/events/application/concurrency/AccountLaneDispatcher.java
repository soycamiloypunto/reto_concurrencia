package com.trading.events.application.concurrency;

import com.trading.events.domain.exception.OverloadedException;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Despachador por "lanes": serializa el trabajo de una misma clave (cuenta) y paraleliza el de
 * claves distintas, sin locks globales ni hilos bloqueados esperando.
 *
 * <p>Cada clave se asigna siempre a la misma lane ({@code floorMod(hash(key), N)}). Una lane
 * ejecuta UNA tarea a la vez, en orden FIFO (patrón "serial executor" / actor). Su cola está
 * acotada: si se llena, la tarea se rechaza con {@link OverloadedException} (backpressure
 * explícito en vez de crecimiento ilimitado de memoria).
 *
 * <p>Propiedades:
 * <ul>
 *   <li>Exclusión mutua por cuenta: dos tareas de la misma lane nunca se solapan.</li>
 *   <li>Orden FIFO por cuenta.</li>
 *   <li>Paralelismo entre lanes distintas.</li>
 *   <li>Sin deadlocks: cada tarea toca una sola clave y no se toman varios locks.</li>
 * </ul>
 */
public final class AccountLaneDispatcher implements AutoCloseable {

    private final Lane[] lanes;
    private final Scheduler scheduler;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param laneCount     número de lanes (grado máximo de paralelismo entre cuentas)
     * @param queueCapacity tareas en espera por lane antes de rechazar
     * @param scheduler     scheduler donde corren las tareas (p. ej. hilos virtuales)
     */
    public AccountLaneDispatcher(int laneCount, int queueCapacity, Scheduler scheduler) {
        if (laneCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("laneCount and queueCapacity must be positive");
        }
        this.scheduler = scheduler;
        this.lanes = new Lane[laneCount];
        for (int i = 0; i < laneCount; i++) {
            lanes[i] = new Lane(queueCapacity);
        }
    }

    /**
     * Encola una tarea en la lane de {@code key}. La tarea se ejecuta cuando todas las anteriores
     * de esa lane terminaron. Cancelar la suscripción cancela la tarea (en cola o en vuelo).
     *
     * @return Mono con el resultado de la tarea, o error {@link OverloadedException} si la lane
     *         está llena o el despachador cerrado
     */
    public <T> Mono<T> submit(String key, Supplier<Mono<T>> task) {
        return Mono.create(sink -> {
            if (closed.get()) {
                sink.error(new OverloadedException(key, "Dispatcher is shut down"));
                return;
            }
            Job<T> job = new Job<>(task, sink);
            sink.onDispose(job::cancel);
            if (!laneFor(key).offer(job)) {
                sink.error(new OverloadedException(key, "Lane queue is full for account " + key));
            }
        });
    }

    public int laneCount() {
        return lanes.length;
    }

    /**
     * @return total de tareas en cola esperando (no incluye las que están ejecutándose)
     */
    public int queuedTasks() {
        int total = 0;
        for (Lane lane : lanes) {
            total += lane.queue.size();
        }
        return total;
    }

    /**
     * @return la mayor profundidad de cola entre las lanes (detecta cuentas calientes)
     */
    public int maxLaneDepth() {
        int max = 0;
        for (Lane lane : lanes) {
            max = Math.max(max, lane.queue.size());
        }
        return max;
    }

    int laneIndex(String key) {
        int hash = key.hashCode();
        // Mezcla los bits altos para repartir mejor claves con hashes parecidos.
        return Math.floorMod(hash ^ (hash >>> 16), lanes.length);
    }

    private Lane laneFor(String key) {
        return lanes[laneIndex(key)];
    }

    @Override
    public void close() {
        closed.set(true);
    }

    /** Tarea encolada junto con el sink que entrega su resultado al llamador. */
    private static final class Job<T> {
        private final Supplier<Mono<T>> task;
        private final MonoSink<T> sink;
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile Disposable inFlight;

        Job(Supplier<Mono<T>> task, MonoSink<T> sink) {
            this.task = task;
            this.sink = sink;
        }

        void fail(Throwable error) {
            sink.error(error);
        }

        void cancel() {
            cancelled.set(true);
            Disposable running = inFlight;
            if (running != null) {
                running.dispose();
            }
        }

        /** Ejecuta la tarea y garantiza que {@code done} se invoca exactamente una vez. */
        void start(Runnable done) {
            if (cancelled.get()) {
                done.run();
                return;
            }
            Mono<T> work;
            try {
                work = task.get();
            } catch (RuntimeException e) {
                sink.error(e);
                done.run();
                return;
            }
            inFlight = work
                    .doFinally(signal -> done.run())
                    .subscribe(sink::success, sink::error, sink::success);
            if (cancelled.get()) {
                inFlight.dispose();
            }
        }
    }

    /** Cola FIFO acotada con un único ejecutor activo a la vez. */
    private final class Lane {
        private final Queue<Job<?>> queue;
        /** Tareas aceptadas y aún no terminadas (en cola + la que corre). */
        private final AtomicInteger pending = new AtomicInteger();

        Lane(int capacity) {
            this.queue = new ArrayBlockingQueue<>(capacity);
        }

        boolean offer(Job<?> job) {
            if (!queue.offer(job)) {
                return false;
            }
            if (pending.getAndIncrement() == 0) {
                scheduleNext();
            }
            return true;
        }

        private void runNext() {
            Job<?> job = queue.poll();
            if (job == null) {
                onJobDone();
                return;
            }
            job.start(this::onJobDone);
        }

        private void onJobDone() {
            if (pending.decrementAndGet() != 0) {
                scheduleNext();
            }
        }

        private void scheduleNext() {
            try {
                scheduler.schedule(this::runNext);
            } catch (RejectedExecutionException e) {
                failPending(e);
            }
        }

        /** El scheduler ya no acepta trabajo (apagado): se falla lo que quedó en cola en vez de colgarlo. */
        private void failPending(RejectedExecutionException cause) {
            Job<?> job;
            while ((job = queue.poll()) != null) {
                job.fail(new OverloadedException(null, "Scheduler is shut down: " + cause.getMessage()));
            }
            pending.set(0);
        }
    }
}
