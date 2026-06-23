package com.portfolio.orderpayment.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Appends an event to the outbox. Called inside the same transaction as the state change it
 * describes, so the event and the change commit (or roll back) atomically — no lost or phantom events.
 */
@Component
@RequiredArgsConstructor
public class OutboxAppender {

    private final OutboxEventRepository outbox;
    private final ObjectMapper objectMapper;

    public void append(String aggregateType, String aggregateId, String type, Object payload) {
        outbox.save(new OutboxEvent(aggregateType, aggregateId, type, serialize(payload)));
    }

    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize outbox payload", e);
        }
    }
}
