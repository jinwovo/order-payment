# ADR-0005 — Poison events park on a dead-letter topic

- **Status:** Accepted — implemented (Milestone 2)
- **Date:** 2026-07

## Context

The consumer had no defined destination for a record that *always* fails (malformed payload, an
event shape the code can't handle). With the default container error handling, a poison record is
retried and then dropped or blocks the partition — for a service whose thesis is "reliable
eventing", "undefined" is the worst possible answer. The outbox guarantees the event *arrives*;
nothing guaranteed what happens when it can't be *processed*.

## Decision

- A `DefaultErrorHandler` with **bounded** exponential backoff (3 attempts, 200ms → 2s) whose
  recoverer republishes the exhausted record to `order-events.DLT` **explicitly**: original headers
  copied, the failure reason and source topic attached as headers, and the send awaited — so a
  failed parking attempt surfaces as a loud error, never a silent drop. (The stock
  `DeadLetterPublishingRecoverer` was tried first; its fire-and-forget internals swallowed a
  publication failure with no log line, which is exactly the failure mode a DLT exists to avoid.)
  Bounding the retries matters as much as having them — unbounded redelivery of a
  permanently-broken record is a tiny metastable failure loop on the consumer.
- A DLT consumer drains the topic into a queryable table (`dead_letter_event`: event-id, payload,
  failure reason) exposed at `GET /orders/dead-letters` — parking is only useful if a human can
  see what parked and why. The demo UI renders it live.
- Parking is **terminal** by design: no automatic re-drive. A poison record failed deterministic
  processing three times; only a code fix or a human decision makes it processable.
- The metric `orders.events.dlt` counts parked events — the alarm-worthy number.
- Chaos directive `poison-event` corrupts the outbox payload *after* the order committed cleanly,
  which demonstrates the key property: the order stays `CONFIRMED`, its projection simply never
  appears, and the DLT explains why — a poison event cannot half-apply to the read model.

## Consequences

- The consumer's idempotency story is preserved: the `event-id` header rides along to the DLT, so
  even a re-driven record (manual) would dedup.
- `order-events.DLT` is declared alongside the main topic (same partition count — the recoverer
  routes to the same partition).
- Retries multiply processing latency for genuinely-transient consumer errors before succeeding;
  at 3 attempts the worst case adds ~1.4s, which is the right trade against data loss.
