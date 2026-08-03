-- Milestone 2: hybrid recovery (retries before compensation), dead-letter parking, and
-- trace propagation across the async outbox -> Kafka -> consumer boundary.

alter table orders add column payment_attempts integer not null default 0;

-- W3C traceparent captured when the event is appended (same tx as the state change), so the
-- consumer can join the original request's trace after the async hop.
alter table outbox_event add column trace_parent varchar(64);

alter table order_projection add column trace_id varchar(40);

-- Poison events land here after the consumer's retries are exhausted (order-events.DLT).
create table dead_letter_event (
    id          uuid primary key,
    event_id    varchar(64),
    payload     text        not null,
    reason      text,
    received_at timestamptz not null
);
