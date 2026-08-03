package com.portfolio.orderpayment.web;

import com.portfolio.orderpayment.fulfillment.OrderProjection;
import com.portfolio.orderpayment.fulfillment.OrderProjectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Exposes the consumer-side read model. A row appears only after the order's event has travelled
 * outbox → Kafka → idempotent consumer, so a 404-then-200 poll on this endpoint observes the
 * eventing pipeline end to end.
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class FulfillmentController {

    private final OrderProjectionRepository projections;

    @GetMapping("/{id}/fulfillment")
    public ResponseEntity<FulfillmentResponse> get(@PathVariable UUID id) {
        return projections.findById(id)
                .map(p -> ResponseEntity.ok(FulfillmentResponse.from(p)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record FulfillmentResponse(UUID orderId, String status, long amountCents, Instant updatedAt,
                                      String traceId) {

        static FulfillmentResponse from(OrderProjection p) {
            return new FulfillmentResponse(p.getOrderId(), p.getStatus(), p.getAmountCents(),
                    p.getUpdatedAt(), p.getTraceId());
        }
    }
}
