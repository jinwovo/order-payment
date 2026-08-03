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
 *
 * <p>The {@code traceparent} header (captured into the outbox row by the producing request, ADR-0006)
 * ties this consumer back to the original trace: the trace id is stored on the projection, and the
 * listener's observation makes the consumer span a child of the request's trace in Zipkin.
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
                             @Header(name = "traceparent", required = false) String traceParent,
                             @Payload String payload) {
        if (eventId != null && processed.existsById(eventId)) {
            log.info("duplicate event {} ignored (already processed)", eventId);
            return;
        }

        OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
        projections.save(new OrderProjection(event.orderId(), event.status(), event.amountCents(),
                traceIdOf(traceParent)));
        if (eventId != null) {
            processed.save(new ProcessedEvent(eventId));
        }
        log.info("projected order {} -> {} (event {})", event.orderId(), event.status(), eventId);
    }

    /** {@code traceparent} is {@code 00-<traceId>-<spanId>-<flags>}. */
    private static String traceIdOf(String traceParent) {
        if (traceParent == null) {
            return null;
        }
        String[] parts = traceParent.split("-");
        return parts.length >= 2 ? parts[1] : null;
    }

    record OrderEvent(UUID orderId, String status, long amountCents) {
    }
}
