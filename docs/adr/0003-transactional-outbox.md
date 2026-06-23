# ADR-0003 — Transactional outbox

- **Status:** Accepted
- **Date:** 2026-06

## Context

When an order is confirmed or rejected, downstream systems (fulfilment, notifications, analytics)
need to know. Publishing to a broker directly from the request path is unsafe: if the DB commit
succeeds but the publish fails (or vice-versa), state and events diverge — a lost or phantom event.

## Decision

Use the **transactional outbox**. The event is written to an `outbox_event` row in the **same
transaction** as the state change it describes. A separate scheduled **relay** polls unpublished
rows, publishes them, and marks them published. State and event therefore commit atomically; delivery
is decoupled from the request.

## Rationale

- Atomicity without distributed transactions: one local DB commit covers both the order change and
  the intent to publish.
- The relay gives **at-least-once** delivery — a crash after publish but before marking published
  re-publishes, so consumers must dedupe by event id.

## Consequences

- Verified: every order outcome wrote an outbox row, and the scheduled relay published all of them
  (`OrderConfirmed`, `OrderRejected` ×2).
- The relay publishes to a **Kafka** topic (`order-events`), marking each row published only after
  the broker acknowledges. Events written while Kafka was down stay unpublished and are delivered on
  recovery — verified end-to-end (broker killed, orders placed, then delivered when it came back).
- The downstream `OrderEventConsumer` processes the stream **idempotently**: a `processed_event`
  ledger skips redelivered event ids, turning at-least-once delivery into effectively-once processing
  (verified by replaying a duplicate). The outbox row id rides along as the `event-id` Kafka header.
- Polling adds latency (≤ the relay interval) and load. Fine at this scale; a CDC-based outbox
  (Debezium) would remove polling if throughput demands it.

## Alternatives considered

- **Publish directly after commit** — the classic dual-write bug; rejected.
- **Listen-to-yourself / CDC (Debezium)** — stronger at high throughput but adds operational
  machinery; the polling relay is the right first step and upgrades cleanly.
