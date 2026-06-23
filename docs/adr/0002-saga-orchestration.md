# ADR-0002 — Saga orchestration vs. two-phase commit

- **Status:** Accepted
- **Date:** 2026-06

## Context

Placing an order spans an inventory reservation (our DB) and a payment authorization (an external
PSP). These cannot share a single ACID transaction — the PSP is a remote system, and even within one
DB you don't want to hold a transaction open across a slow network call.

## Decision

Use an **orchestrated saga**: a coordinator runs the steps in order — reserve stock → authorize
payment → confirm — and on failure runs **compensating actions** in reverse (release stock; void the
charge). Each step commits in its own transaction; the orchestrator itself is not transactional.

## Rationale

- **No distributed locks / 2PC.** Two-phase commit needs an XA coordinator and resource managers that
  support it (the PSP doesn't), and holds locks across the prepare phase. A saga trades atomicity for
  availability + compensation, which fits e-commerce checkout.
- **Orchestration over choreography.** A single orchestrator makes the flow and its compensations
  readable in one place — easier to reason about than events bouncing between services for a flow
  this linear.
- Committing each step separately is the whole point: because the payment call sits *between*
  committed steps, a later failure genuinely cannot be rolled back and **must** be compensated.

## Consequences

- Verified: an over-limit order reserved stock, was declined by the PSP, and had its stock released —
  net inventory unchanged (10 → reserved → 10).
- Intermediate states are visible (an order is briefly `PENDING` with stock reserved). That's
  inherent to sagas; consumers must tolerate eventual outcomes.
- Compensations must be idempotent and safe to retry — currently best-effort; durable compensation
  (retry on failure) is roadmap.

## Alternatives considered

- **Single local transaction** — only works if payment were also local/mockable; unrealistic and
  hides the actual distributed-systems problem.
- **Two-phase commit (XA)** — rejected: no PSP support, poor availability, operational weight.
- **Event-choreographed saga** — viable, but for a short linear flow the orchestrator is clearer; a
  choreographed version becomes attractive as steps and services multiply.
