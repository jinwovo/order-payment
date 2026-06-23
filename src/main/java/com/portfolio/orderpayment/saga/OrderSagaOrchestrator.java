package com.portfolio.orderpayment.saga;

import com.portfolio.orderpayment.catalog.Product;
import com.portfolio.orderpayment.catalog.ProductRepository;
import com.portfolio.orderpayment.catalog.UnknownProductException;
import com.portfolio.orderpayment.idempotency.IdempotencyService;
import com.portfolio.orderpayment.inventory.InventoryService;
import com.portfolio.orderpayment.inventory.OutOfStockException;
import com.portfolio.orderpayment.ordering.OrderItem;
import com.portfolio.orderpayment.ordering.OrderResponse;
import com.portfolio.orderpayment.ordering.OrderService;
import com.portfolio.orderpayment.payment.PaymentGateway;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Orchestrates the place-order saga: reserve stock → authorize payment → confirm, compensating in
 * reverse on failure. The orchestrator itself is NOT transactional — each step commits in its own
 * transaction (via separate beans) so that an external payment call sits between committed steps,
 * which is exactly what makes compensation (rather than rollback) necessary.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final ProductRepository products;
    private final IdempotencyService idempotency;
    private final InventoryService inventory;
    private final PaymentGateway paymentGateway;
    private final OrderService orderService;

    public OrderResponse place(String idempotencyKey, List<OrderLine> lines) {
        // 0. Idempotent replay: a retried request returns the original order, no new effects.
        var prior = idempotency.findOrderId(idempotencyKey);
        if (prior.isPresent()) {
            return orderService.view(prior.get());
        }

        // 1. Resolve products, build line items, compute the total.
        List<String> skus = lines.stream().map(OrderLine::sku).toList();
        Map<String, Product> bySku = products.findBySkuIn(skus).stream()
                .collect(Collectors.toMap(Product::getSku, p -> p));

        List<OrderItem> items = new ArrayList<>();
        long total = 0;
        for (OrderLine line : lines) {
            Product product = bySku.get(line.sku());
            if (product == null) {
                throw new UnknownProductException(line.sku());
            }
            items.add(new OrderItem(product.getId(), product.getSku(), line.quantity(), product.getPriceCents()));
            total += product.getPriceCents() * (long) line.quantity();
        }

        // 2. Claim the idempotency key before doing any work; a concurrent duplicate loses the race
        //    here and falls back to the winner's order.
        UUID orderId = UUID.randomUUID();
        try {
            idempotency.claim(idempotencyKey, orderId);
        } catch (DataIntegrityViolationException duplicate) {
            return orderService.view(idempotency.findOrderId(idempotencyKey).orElseThrow());
        }
        orderService.create(orderId, total, items);

        // 3. Saga step 1 — reserve inventory.
        try {
            inventory.reserve(items);
        } catch (OutOfStockException e) {
            log.info("order {} rejected: out of stock {}", orderId, e.getSku());
            return orderService.reject(orderId, "OUT_OF_STOCK: " + e.getSku());
        }

        // 4. Saga step 2 — authorize payment (external). On decline, compensate the reservation.
        PaymentGateway.PaymentResult auth = paymentGateway.authorize(orderId, total);
        if (!auth.approved()) {
            inventory.release(items);
            log.info("order {} rejected: payment declined", orderId);
            return orderService.reject(orderId, "PAYMENT_DECLINED: " + auth.declineReason());
        }

        // 5. Saga step 3 — confirm (capture payment + confirm order + emit event, atomically).
        return orderService.confirm(orderId, auth.pspRef(), total);
    }
}
