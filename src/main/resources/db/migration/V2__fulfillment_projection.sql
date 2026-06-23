-- Consumer-side (fulfillment) read model + idempotency ledger for the order-events stream.

-- Deduplication ledger: an event id is recorded once it has been processed, so a redelivered
-- message (the outbox is at-least-once) is skipped.
create table processed_event (
    event_id     varchar(64) primary key,
    processed_at timestamptz not null
);

-- Read model built by consuming order events — one row per order.
create table order_projection (
    order_id     uuid primary key,
    status       varchar(32) not null,
    amount_cents bigint      not null,
    updated_at   timestamptz not null
);
