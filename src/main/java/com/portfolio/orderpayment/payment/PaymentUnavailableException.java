package com.portfolio.orderpayment.payment;

/**
 * The PSP could not be reached or timed out — a <em>transient</em> failure, distinct from a
 * decline. Transient failures are retried with backoff before the saga gives up and compensates
 * (hybrid recovery, ADR-0004); declines are a business answer and are never retried.
 */
public class PaymentUnavailableException extends RuntimeException {

    public PaymentUnavailableException(String message) {
        super(message);
    }

    public PaymentUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
