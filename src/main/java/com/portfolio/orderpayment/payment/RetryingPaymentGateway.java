package com.portfolio.orderpayment.payment;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry decorator for the PSP: transient failures get a bounded number of attempts with
 * exponential backoff and <em>full jitter</em> before the saga falls back to compensation
 * (hybrid recovery, ADR-0004).
 *
 * <p>Full jitter — {@code sleep = random(0, min(cap, base·2^attempt))} — is deliberate: synchronized
 * retries from many clients are how transient blips turn into metastable retry storms; randomizing
 * the whole window de-correlates them. Declines pass straight through: retrying a business "no"
 * risks double-charging — it is the unavailability case that earns retries.
 */
@Slf4j
@Primary
@Component
public class RetryingPaymentGateway implements PaymentGateway {

    private final SimulatedPaymentGateway delegate;
    private final MeterRegistry metrics;

    @Value("${payment.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${payment.retry.base-backoff-ms:100}")
    private long baseBackoffMs;

    @Value("${payment.retry.max-backoff-ms:1000}")
    private long maxBackoffMs;

    public RetryingPaymentGateway(SimulatedPaymentGateway delegate, MeterRegistry metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    public PaymentResult authorize(UUID orderId, long amountCents) {
        for (int attempt = 1; ; attempt++) {
            try {
                return delegate.authorize(orderId, amountCents).withAttempts(attempt);
            } catch (PaymentUnavailableException e) {
                metrics.counter("orders.payment.retries").increment();
                if (attempt >= maxAttempts) {
                    throw new PaymentUnavailableException(
                            "PSP unavailable after " + attempt + " attempts", e);
                }
                long backoff = fullJitterBackoff(attempt);
                log.warn("payment attempt {}/{} failed for order {} — retrying in {}ms",
                        attempt, maxAttempts, orderId, backoff);
                sleep(backoff);
            }
        }
    }

    @Override
    public void voidAuthorization(String pspRef) {
        delegate.voidAuthorization(pspRef);
    }

    private long fullJitterBackoff(int attempt) {
        long ceiling = Math.min(maxBackoffMs, baseBackoffMs * (1L << (attempt - 1)));
        return ThreadLocalRandom.current().nextLong(0, ceiling + 1);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PaymentUnavailableException("interrupted during payment retry backoff", e);
        }
    }
}
