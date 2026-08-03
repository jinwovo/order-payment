# ADR-0004 — Hybrid recovery: bounded retries with jitter, then compensation

- **Status:** Accepted — implemented (Milestone 2)
- **Date:** 2026-07

## Context

The saga had exactly one answer to a payment problem: compensate. That conflates two different
failures:

- a **decline** is a business answer — retrying it is wrong (at best pointless, at worst a double
  charge);
- an **outage/timeout** is infrastructure — compensating on the first blip rejects orders that a
  200ms retry would have confirmed.

Compensation-only turns every transient blip into a lost order; retry-only (no compensation) turns
every real outage into a stuck saga. Production systems do both, in that order.

## Decision

- `PaymentGateway` distinguishes the two shapes: declines stay a `PaymentResult`, transports throw
  `PaymentUnavailableException`.
- A decorator (`RetryingPaymentGateway`, `@Primary`) retries only the transient shape:
  `payment.retry.max-attempts` (3) with exponential backoff and **full jitter** —
  `sleep = random(0, min(cap, base·2^attempt))`. Jitter is not a nicety: synchronized retries from
  many clients are the classic amplifier that turns a blip into a metastable retry storm, and
  randomizing the whole window de-correlates them (the AWS Builders' Library backoff guidance; the
  "metastable failures" literature is about exactly this feedback loop).
- Only an **exhausted budget** compensates (`PAYMENT_UNAVAILABLE` → release the reservation).
- A failure **after** a successful authorization (the confirm step) compensates **both ways**:
  `voidAuthorization` at the PSP *and* release the stock — a DB rollback cannot reach money that
  moved in an external system. This makes the previously-dead `voidAuthorization` a real, tested
  code path.
- The saga's answer records how hard it worked: `paymentAttempts` on the order (>1 = the retry
  layer earned its keep), and the failure reason distinguishes `PAYMENT_DECLINED` /
  `PAYMENT_UNAVAILABLE` / `CONFIRM_FAILED`.
- Fault injection is an explicit API surface (`X-Chaos` header → `ChaosContext`, gated by
  `chaos.enabled`): the recovery tests and the demo UI drive the same failure paths as real
  traffic, so every advertised failure mode is reproducible with one curl.

## Consequences

- The payment step's latency is now bounded by the retry budget (~worst case
  `Σ jitter windows ≈ 1.1s` at defaults) — acceptable for an interactive demo, tunable by config.
- Retries assume the PSP treats `authorize(orderId, …)` idempotently per order; a real integration
  would send a PSP-level idempotency key, which this simulation models by scoping chaos per order.
- `ChaosContext` is a ThreadLocal because the saga is synchronous on the request thread; if steps
  ever go async, the directives must move onto the request payload.

## References

- Garcia-Molina & Salem, *Sagas* (SIGMOD 1987) — the compensation model this service implements.
- AWS Builders' Library, *Timeouts, retries, and backoff with jitter* — the full-jitter formula.
- Bronson et al., *Metastable Failures in Distributed Systems* (HotOS 2021) — why retries without
  jitter and bounds are a feedback loop, not a fix.
