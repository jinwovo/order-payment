package com.portfolio.orderpayment.fulfillment;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

/**
 * Consumes the {@code order-events} stream and maintains the {@link OrderProjection} read model.
 * Processing is idempotent: the outbox delivers at-least-once, so a redelivered event id (recorded in
 * {@link ProcessedEvent}) is skipped. The dedup check and the projection update share one transaction.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProcessedEventRepository processed;
    private final OrderProjectionRepository projections;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${outbox.topic:order-events}", groupId = "fulfillment")
    @Transactional
    public void onOrderEvent(@Header(name = "event-id", required = false) String eventId,
                             @Payload String payload) {
        if (eventId != null && processed.existsById(eventId)) {
            log.info("duplicate event {} ignored (already processed)", eventId);
            return;
        }

        OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
        projections.save(new OrderProjection(event.orderId(), event.status(), event.amountCents()));
        if (eventId != null) {
            processed.save(new ProcessedEvent(eventId));
        }
        log.info("projected order {} -> {} (event {})", event.orderId(), event.status(), eventId);
    }

    record OrderEvent(UUID orderId, String status, long amountCents) {
    }
}
