package com.portfolio.orderpayment.payment;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Stand-in for a real PSP. Approves unless the amount exceeds a configurable limit
 * ({@code payment.decline-above-cents}) — a deterministic way to exercise the declined/compensation
 * path in tests and the demo without random behaviour.
 */
@Slf4j
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    @Value("${payment.decline-above-cents:500000}")
    private long declineAboveCents;

    @Override
    public PaymentResult authorize(UUID orderId, long amountCents) {
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
        log.info("payment VOID ref={} (compensation)", pspRef);
    }
}
