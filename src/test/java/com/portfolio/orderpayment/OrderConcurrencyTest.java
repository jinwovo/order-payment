package com.portfolio.orderpayment;

import com.portfolio.orderpayment.catalog.ProductRepository;
import com.portfolio.orderpayment.ordering.OrderResponse;
import com.portfolio.orderpayment.saga.OrderLine;
import com.portfolio.orderpayment.saga.OrderSagaOrchestrator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves the atomic conditional stock decrement prevents oversell: many concurrent orders for the
 * last few units of one product result in exactly {@code stock}-many confirmations and never drive
 * stock below zero.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderConcurrencyTest {

    @Autowired
    private OrderSagaOrchestrator orchestrator;

    @Autowired
    private ProductRepository products;

    @Test
    void concurrent_orders_never_oversell() throws Exception {
        int stock = products.findBySku("SKU-DESK").orElseThrow().getStock(); // seeded at 5
        int attempts = stock * 6;                                            // far more orders than stock

        ExecutorService pool = Executors.newFixedThreadPool(attempts);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger confirmed = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < attempts; i++) {
            final int n = i;
            futures.add(pool.submit(() -> {
                try {
                    startGate.await();
                    OrderResponse result = orchestrator.place("concurrency-" + n,
                            List.of(new OrderLine("SKU-DESK", 1)));
                    (result.status().equals("CONFIRMED") ? confirmed : rejected).incrementAndGet();
                } catch (Exception e) {
                    rejected.incrementAndGet();
                }
            }));
        }

        startGate.countDown(); // release all threads at once
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        int remaining = products.findBySku("SKU-DESK").orElseThrow().getStock();
        assertEquals(stock, confirmed.get(), "exactly stock-many orders should confirm");
        assertEquals(attempts - stock, rejected.get(), "the rest should be rejected, none lost");
        assertEquals(0, remaining, "stock must never go negative (no oversell)");
    }
}
