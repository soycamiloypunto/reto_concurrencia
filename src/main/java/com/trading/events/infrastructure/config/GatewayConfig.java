package com.trading.events.infrastructure.config;

import com.trading.events.domain.port.out.MarketGateway;
import com.trading.events.infrastructure.gateway.ResilientMarketGateway;
import com.trading.events.infrastructure.gateway.SimulatedMarketGateway;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;

/**
 * Compone el gateway: mercado simulado envuelto por el decorador de resiliencia.
 */
@Configuration(proxyBeanMethods = false)
public class GatewayConfig {

    @Bean
    public MarketGateway marketGateway(GatewayProperties properties, Scheduler virtualThreadScheduler,
                                       CircuitBreaker marketCircuitBreaker, Bulkhead marketBulkhead) {
        MarketGateway raw = new SimulatedMarketGateway(properties, virtualThreadScheduler);
        return new ResilientMarketGateway(raw, marketCircuitBreaker, marketBulkhead, properties);
    }
}
