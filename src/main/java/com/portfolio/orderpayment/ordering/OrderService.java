package com.portfolio.orderpayment.ordering;

import com.portfolio.orderpayment.outbox.OutboxAppender;
import com.portfolio.orderpayment.payment.Payment;
import com.portfolio.orderpayment.payment.PaymentRepository;
import com.portfolio.orderpayment.payment.PaymentStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Transactional building blocks of the order lifecycle. Each method is its own transaction so the
 * saga orchestrator can compose them across the external payment call; {@code confirm}/{@code reject}
 * write their outbox event in the same transaction as the state change.
 */
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orders;
    private final PaymentRepository payments;
    private final OutboxAppender outbox;

    @Transactional
    public void create(UUID orderId, long totalCents, List<OrderItem> items) {
        Order order = new Order(orderId, totalCents);
        items.forEach(order::addItem);
        orders.save(order);
    }

    @Transactional
    public OrderResponse confirm(UUID orderId, String pspRef, long amountCents) {
        Order order = orders.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        payments.save(new Payment(orderId, amountCents, PaymentStatus.CAPTURED, pspRef));
        order.confirm();
        outbox.append("ORDER", orderId.toString(), "OrderConfirmed", new OrderEvent(orderId, "CONFIRMED", amountCents));
        return OrderResponse.from(order);
    }

    @Transactional
    public OrderResponse reject(UUID orderId, String reason) {
        Order order = orders.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        order.reject(reason);
        outbox.append("ORDER", orderId.toString(), "OrderRejected", new OrderEvent(orderId, "REJECTED", order.getTotalCents()));
        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse view(UUID orderId) {
        return OrderResponse.from(orders.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId)));
    }

    private record OrderEvent(UUID orderId, String status, long amountCents) {
    }
}
