package com.portfolio.orderpayment.payment;

import com.portfolio.orderpayment.chaos.ChaosContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Stand-in for a real PSP. Approves unless the amount exceeds a configurable limit
 * ({@code payment.decline-above-cents}) — a deterministic way to exercise the declined/compensation
 * path — and throws {@link PaymentUnavailableException} when the chaos context injects a transient
 * outage, which is what the retry layer above ({@link RetryingPaymentGateway}) exists to absorb.
 */
@Slf4j
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    @Value("${payment.decline-above-cents:500000}")
    private long declineAboveCents;

    // Test observability: a simulated PSP can simply record the voids it received.
    private final Queue<String> voidedRefs = new ConcurrentLinkedQueue<>();

    @Override
    public PaymentResult authorize(UUID orderId, long amountCents) {
        if (ChaosContext.consumePaymentFailure()) {
            log.warn("payment UNAVAILABLE order={} (injected transient outage)", orderId);
            throw new PaymentUnavailableException("simulated PSP timeout");
        }
        if (amountCents > declineAboveCents) {
            log.info("payment DECLINED order={} amount={} (over limit {})", orderId, amountCents, declineAboveCents);
            return PaymentResult.declined("amount exceeds limit of " + declineAboveCents);
        }
        String pspRef = "psp_" + UUID.randomUUID().toString().substring(0, 12);
        log.info("payment APPROVED order={} amount={} ref={}", orderId, amountCents, pspRef);
        return PaymentResult.approved(pspRef);
    }

    @Override
    public void voidAuthorization(String pspRef) {
        voidedRefs.add(pspRef);
        log.info("payment VOID ref={} (compensation)", pspRef);
    }

    public Queue<String> voidedRefs() {
        return voidedRefs;
    }
}
