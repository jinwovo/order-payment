# ADR-0001 — Idempotent order creation

- **Status:** Accepted
- **Date:** 2026-06

## Context

`POST /orders` is not naturally idempotent: a client that times out and retries, or double-submits,
would otherwise create two orders and two charges. Clients send an `Idempotency-Key` header; the
server must guarantee at-most-once order creation per key.

## Decision

Persist the key in an `idempotency_key` table whose primary key is the key itself, mapping it to the
order id it produced. The key is **claimed before any work**: we generate the order id, `INSERT` the
key→id row and flush. On a duplicate the primary-key constraint raises
`DataIntegrityViolationException`, and we return the already-created order instead of processing
again. A later retry short-circuits at the start via a lookup.

## Rationale

- The database constraint — not application checks — is the source of truth, so it is correct under
  concurrent duplicates (both requests race on the same PK; exactly one wins).
- Claiming the key *before* creating the order avoids orphan orders if two requests collide.

## Consequences

- Verified: two identical requests with the same key produced one order and decremented stock once
  (100 → 98, not → 96).
- The replay returns the order's **current** state. There is a narrow window where the winner has
  claimed the key but not yet committed the order; a concurrent duplicate could observe a not-yet-
  visible order. Acceptable for this milestone; a hardened version would store the full response and
  serve it, or have the loser wait.

## Alternatives considered

- **Check-then-insert in app code** — racy without the unique constraint; rejected.
- **Hash the request body as the key** — removes the client header requirement but couples
  idempotency to exact payload bytes; the explicit key is clearer and standard (Stripe-style).
