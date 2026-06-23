package com.portfolio.orderpayment.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment")
@Getter
public class Payment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(name = "psp_ref")
    private String pspRef;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Payment() {
    }

    public Payment(UUID orderId, long amountCents, PaymentStatus status, String pspRef) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.amountCents = amountCents;
        this.status = status;
        this.pspRef = pspRef;
        this.createdAt = Instant.now();
    }
}
