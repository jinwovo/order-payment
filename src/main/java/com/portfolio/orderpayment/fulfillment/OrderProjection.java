package com.portfolio.orderpayment.fulfillment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

/** Read model maintained by the order-events consumer — one row per order. */
@Entity
@Table(name = "order_projection")
@Getter
public class OrderProjection {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(nullable = false)
    private String status;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected OrderProjection() {
    }

    public OrderProjection(UUID orderId, String status, long amountCents) {
        this.orderId = orderId;
        this.status = status;
        this.amountCents = amountCents;
        this.updatedAt = Instant.now();
    }
}
