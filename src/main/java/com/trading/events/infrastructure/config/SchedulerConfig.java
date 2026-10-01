package com.trading.events.infrastructure.config;

import com.trading.events.application.concurrency.AccountLaneDispatcher;
import com.trading.events.application.service.ProcessingLimits;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

/**
 * Wiring de la infraestructura de concurrencia: hilos virtuales + despachador por lanes.
 */
@Configuration(proxyBeanMethods = false)
public class SchedulerConfig {

    /**
     * Scheduler de hilos virtuales: un hilo por tarea, barato incluso si bloquea (sleep / I/O
     * bloqueante). Nunca se usa para trabajo CPU intensivo ni sustituye al event loop de Netty.
     */
    @Bean(destroyMethod = "dispose")
    public Scheduler virtualThreadScheduler() {
        return Schedulers.fromExecutorService(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("event-vt-", 0).factory()),
                "event-virtual");
    }

    @Bean(destroyMethod = "close")
    public AccountLaneDispatcher accountLaneDispatcher(ConcurrencyProperties properties,
                                                       Scheduler virtualThreadScheduler) {
        return new AccountLaneDispatcher(properties.lanes(), properties.laneQueueCapacity(),
                virtualThreadScheduler);
    }

    @Bean
    public ProcessingLimits processingLimits(ConcurrencyProperties properties) {
        return new ProcessingLimits(properties.maxBatchConcurrency());
    }
}
