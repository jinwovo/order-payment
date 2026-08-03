package com.portfolio.orderpayment.fulfillment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/** A poison event parked off the DLT — kept queryable so "what failed and why" is one GET away. */
@Entity
@Table(name = "dead_letter_event")
@Getter
public class DeadLetterEvent {

    @Id
    private UUID id;

    @Column(name = "event_id")
    private String eventId;

    @Column(nullable = false)
    private String payload;

    @Column
    private String reason;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected DeadLetterEvent() {
    }

    public DeadLetterEvent(String eventId, String payload, String reason) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.payload = payload;
        this.reason = reason;
        this.receivedAt = Instant.now();
    }
}
