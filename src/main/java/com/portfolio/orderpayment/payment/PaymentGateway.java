package com.portfolio.orderpayment.payment;

import java.util.UUID;

/**
 * Abstraction over an external payment provider. The authorization is a call to a system outside our
 * database transaction — which is exactly why a successful charge followed by a later failure must be
 * undone by a <em>compensating</em> action ({@link #voidAuthorization}) rather than a DB rollback.
 *
 * <p>Two failure shapes, treated differently (ADR-0004): a <strong>decline</strong> is a business
 * answer (never retried); {@link PaymentUnavailableException} is infrastructure (retried with
 * backoff + jitter before the saga compensates).
 */
public interface PaymentGateway {

    PaymentResult authorize(UUID orderId, long amountCents);

    void voidAuthorization(String pspRef);

    record PaymentResult(boolean approved, String pspRef, String declineReason, int attempts) {
        public static PaymentResult approved(String pspRef) {
            return new PaymentResult(true, pspRef, null, 1);
        }

        public static PaymentResult declined(String reason) {
            return new PaymentResult(false, null, reason, 1);
        }

        public PaymentResult withAttempts(int attempts) {
            return new PaymentResult(approved, pspRef, declineReason, attempts);
        }
    }
}
