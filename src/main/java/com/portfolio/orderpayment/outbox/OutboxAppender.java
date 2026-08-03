package com.portfolio.orderpayment.outbox;

import com.portfolio.orderpayment.chaos.ChaosContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Appends an event to the outbox. Called inside the same transaction as the state change it
 * describes, so the event and the change commit (or roll back) atomically — no lost or phantom events.
 *
 * <p>The current trace context is captured into the row as a W3C {@code traceparent} — the request
 * that caused the event is long gone by the time the relay publishes, so the trace has to ride the
 * outbox to survive the async hop (ADR-0006).
 */
@Component
@RequiredArgsConstructor
public class OutboxAppender {

    private final OutboxEventRepository outbox;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;

    public void append(String aggregateType, String aggregateId, String type, Object payload) {
        String json = serialize(payload);
        if (ChaosContext.poisonEvent()) {
            // Deliberately corrupt the payload AFTER the state change committed cleanly: the order
            // itself stays valid; only the downstream consumer chokes — the DLT's exact use case.
            json = "POISON:" + json;
        }
        outbox.save(new OutboxEvent(aggregateType, aggregateId, type, json, currentTraceParent()));
    }

    private String currentTraceParent() {
        Span span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        return "00-" + span.context().traceId() + "-" + span.context().spanId() + "-01";
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize outbox payload", e);
        }
    }
}
