package com.portfolio.orderpayment.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_key")
@Getter
public class IdempotencyKey {

    @Id
    private String id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected IdempotencyKey() {
    }

    public IdempotencyKey(String id, UUID orderId) {
        this.id = id;
        this.orderId = orderId;
        this.createdAt = Instant.now();
    }
}
