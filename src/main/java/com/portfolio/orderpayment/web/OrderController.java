package com.portfolio.orderpayment.web;

import com.portfolio.orderpayment.ordering.OrderResponse;
import com.portfolio.orderpayment.ordering.OrderService;
import com.portfolio.orderpayment.saga.OrderLine;
import com.portfolio.orderpayment.saga.OrderSagaOrchestrator;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderSagaOrchestrator orchestrator;
    private final OrderService orderService;

    @PostMapping
    public OrderResponse place(@RequestHeader("Idempotency-Key") String idempotencyKey,
                               @Valid @RequestBody PlaceOrderRequest request) {
        List<OrderLine> lines = request.lines().stream()
                .map(line -> new OrderLine(line.sku(), line.quantity()))
                .toList();
        return orchestrator.place(idempotencyKey, lines);
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return orderService.view(id);
    }
}
