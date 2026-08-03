package com.portfolio.orderpayment;

import com.portfolio.orderpayment.catalog.ProductRepository;
import com.portfolio.orderpayment.chaos.ChaosContext;
import com.portfolio.orderpayment.ordering.OrderResponse;
import com.portfolio.orderpayment.payment.SimulatedPaymentGateway;
import com.portfolio.orderpayment.saga.OrderLine;
import com.portfolio.orderpayment.saga.OrderSagaOrchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hybrid recovery under injected faults (ADR-0004): transient PSP outages are retried and the order
 * still confirms; an exhausted retry budget compensates the reservation; a crash after a successful
 * authorization compensates BOTH ways — void at the PSP plus stock release. Stock is asserted
 * restored in every failure path, because that's the invariant compensation exists to protect.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        // No broker in this suite: park the relay so it doesn't error-log every second.
        "outbox.relay-interval-ms=600000",
        // Deterministic, fast backoff for tests.
        "payment.retry.base-backoff-ms=5",
        "payment.retry.max-backoff-ms=10"
})
class SagaRecoveryTest {

    @Autowired
    private OrderSagaOrchestrator orchestrator;
    @Autowired
    private ProductRepository products;
    @Autowired
    private SimulatedPaymentGateway psp;

    @AfterEach
    void clearChaos() {
        ChaosContext.close();
    }

    @Test
    void transientPaymentOutage_isRetried_andTheOrderStillConfirms() {
        ChaosContext.open("payment-transient:2");
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 1)));

        assertEquals("CONFIRMED", order.status());
        assertEquals(3, order.paymentAttempts(), "two injected failures + the successful attempt");
    }

    @Test
    void exhaustedRetryBudget_compensatesTheReservation() {
        int stockBefore = stockOf("SKU-MOUSE");
        ChaosContext.open("payment-down");
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 2)));

        assertEquals("REJECTED", order.status());
        assertTrue(order.failureReason().startsWith("PAYMENT_UNAVAILABLE"),
                "unavailability is not a decline: " + order.failureReason());
        assertEquals(stockBefore, stockOf("SKU-MOUSE"), "reserved stock must be released");
    }

    @Test
    void crashAfterAuthorization_voidsThePaymentAndReleasesTheStock() {
        int stockBefore = stockOf("SKU-MOUSE");
        int voidsBefore = psp.voidedRefs().size();
        ChaosContext.open("confirm-crash");
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 1)));

        assertEquals("REJECTED", order.status());
        assertTrue(order.failureReason().startsWith("CONFIRM_FAILED"), order.failureReason());
        assertEquals(stockBefore, stockOf("SKU-MOUSE"), "reserved stock must be released");
        assertEquals(voidsBefore + 1, psp.voidedRefs().size(),
                "the authorization at the PSP must be voided — a DB rollback can't undo it");
    }

    @Test
    void aDecline_isNeverRetried() {
        // SKU-LAPTOP x4 = 600000 cents > the 500000 decline limit.
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-LAPTOP", 4)));

        assertEquals("REJECTED", order.status());
        assertTrue(order.failureReason().startsWith("PAYMENT_DECLINED"), order.failureReason());
        assertFalse(order.paymentAttempts() > 1, "a business 'no' must not be retried");
    }

    private int stockOf(String sku) {
        return products.findBySkuIn(List.of(sku)).get(0).getStock();
    }

    private static String key() {
        return "recovery-" + UUID.randomUUID();
    }
}
