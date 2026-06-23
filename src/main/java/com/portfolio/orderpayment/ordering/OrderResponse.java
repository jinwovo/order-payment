package com.portfolio.orderpayment.ordering;

import java.util.List;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        String status,
        long totalCents,
        String failureReason,
        List<Line> items
) {
    public record Line(String sku, int quantity, long unitPriceCents) {
    }

    public static OrderResponse from(Order order) {
        List<Line> lines = order.getItems().stream()
                .map(i -> new Line(i.getSku(), i.getQuantity(), i.getUnitPriceCents()))
                .toList();
        return new OrderResponse(order.getId(), order.getStatus().name(), order.getTotalCents(),
                order.getFailureReason(), lines);
    }
}
