# ADR-0006 — One trace across the async boundary (traceparent through the outbox)

- **Status:** Accepted — implemented (Milestone 2)
- **Date:** 2026-07

## Context

The pipeline's whole point is the async hop: request → outbox (same tx) → relay → Kafka →
idempotent consumer → read model. That is also exactly where observability died — the request's
trace ended at the HTTP response, and the consumer's work belonged to no trace at all. The demo UI
even drew the pipeline by hand because nothing else could show it.

The standard answer is W3C Trace Context (`traceparent`) propagation, but the naive setup breaks
here: the relay publishes from a scheduler thread long after the request thread is gone, so
"current span" at publish time is the wrong trace. The trace has to be **persisted with the
event**.

## Decision

- **Capture at append.** `OutboxAppender` runs on the request thread inside the state-change
  transaction, so it snapshots the current span as a W3C `traceparent` string into the outbox row
  (`trace_parent` column). The trace commits atomically with the event that carries it.
- **Restore at relay.** The relay copies the stored `traceparent` onto the Kafka record as a
  header — deliberately *not* letting a producer-side observation overwrite it with the
  scheduler's own (unrelated) context.
- **Join at consume.** `spring.kafka.listener.observation-enabled` makes the consumer span a child
  of the header's trace (visible in Zipkin as one trace: HTTP server span → consumer span), and
  the consumer records the trace id onto the projection row. `GET /orders/{id}/fulfillment`
  returns it, and the demo UI links straight into Zipkin.
- Micrometer Tracing (Brave bridge) + Zipkin (`op-zipkin`, compose) with 100% sampling — a demo
  wants every trace; production would sample.

## Consequences

- The relay's own send is not a span inside the propagated trace (its observation is off by
  design); the trace shows request → consumer, with the outbox row as the carrier. Named
  limitation, right trade for correctness of parentage.
- Log correlation comes free: Boot stamps `traceId`/`spanId` into the MDC, so request-side and
  consumer-side log lines for one order grep together.
- `trace_parent` is nullable — events created outside a traced request (none today) simply don't
  carry one.

## References

- W3C, *Trace Context* — the `traceparent` header format persisted in the outbox row.
- The "outbox pattern with tracing" write-ups around Debezium/OpenTelemetry document the same
  capture-persist-restore shape for CDC pipelines.
