package com.portfolio.orderpayment.payment;

import java.util.UUID;

/**
 * Abstraction over an external payment provider. The authorization is a call to a system outside our
 * database transaction — which is exactly why a successful charge followed by a later failure must be
 * undone by a <em>compensating</em> action ({@link #voidAuthorization}) rather than a DB rollback.
 */
public interface PaymentGateway {

    PaymentResult authorize(UUID orderId, long amountCents);

    void voidAuthorization(String pspRef);

    record PaymentResult(boolean approved, String pspRef, String declineReason) {
        public static PaymentResult approved(String pspRef) {
            return new PaymentResult(true, pspRef, null);
        }

        public static PaymentResult declined(String reason) {
            return new PaymentResult(false, null, reason);
        }
    }
}
