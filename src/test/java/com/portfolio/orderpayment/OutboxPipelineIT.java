package com.portfolio.orderpayment;

import com.portfolio.orderpayment.chaos.ChaosContext;
import com.portfolio.orderpayment.fulfillment.DeadLetterEventRepository;
import com.portfolio.orderpayment.fulfillment.OrderProjection;
import com.portfolio.orderpayment.fulfillment.OrderProjectionRepository;
import com.portfolio.orderpayment.ordering.OrderResponse;
import com.portfolio.orderpayment.outbox.OutboxEvent;
import com.portfolio.orderpayment.outbox.OutboxEventRepository;
import com.portfolio.orderpayment.saga.OrderLine;
import com.portfolio.orderpayment.saga.OrderSagaOrchestrator;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The Kafka half of the system, asserted against a real broker: outbox → relay → topic → idempotent
 * consumer → projection, the duplicate-delivery dedup, the poison-event → DLT path (ADR-0005), and
 * the trace surviving the async boundary (ADR-0006).
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, KafkaTestcontainers.class})
@TestPropertySource(properties = {
        // The broker's host port is fixed by KafkaTestcontainers, so the wiring can be too.
        "spring.kafka.bootstrap-servers=localhost:" + KafkaTestcontainers.KAFKA_HOST_PORT,
        "outbox.relay-interval-ms=200",
        "payment.retry.base-backoff-ms=5",
        "payment.retry.max-backoff-ms=10"
})
class OutboxPipelineIT {

    @Autowired
    private OrderSagaOrchestrator orchestrator;
    @Autowired
    private OrderProjectionRepository projections;
    @Autowired
    private DeadLetterEventRepository deadLetters;
    @Autowired
    private OutboxEventRepository outbox;
    @Autowired
    private Tracer tracer;
    @Autowired
    private org.springframework.core.env.Environment env;

    @AfterEach
    void clearChaos() {
        ChaosContext.close();
    }

    @Test
    void confirmedOrder_projectsAcrossTheAsyncBoundary_carryingTheOriginalTrace() {
        Span span = tracer.nextSpan().name("place-order-test");
        OrderResponse order;
        try (Tracer.SpanInScope ignored = tracer.withSpan(span.start())) {
            order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 1)));
        } finally {
            span.end();
        }
        assertEquals("CONFIRMED", order.status());

        await("the projection to appear via outbox → Kafka → consumer", Duration.ofSeconds(60),
                () -> projections.findById(order.id()).isPresent());
        OrderProjection projection = projections.findById(order.id()).orElseThrow();
        assertEquals("CONFIRMED", projection.getStatus());

        OutboxEvent event = outbox.findAll().stream()
                .filter(e -> e.getAggregateId().equals(order.id().toString()))
                .findFirst().orElseThrow();
        assertNotNull(event.getPublishedAt(), "the relay marks rows only after the broker ack");
        assertNotNull(event.getTraceParent(), "the outbox row carries the producing trace");
        assertEquals(span.context().traceId(), projection.getTraceId(),
                "the consumer-side read model records the ORIGINAL request's trace id");
    }

    @Test
    void duplicateDelivery_isAbsorbedByTheConsumerDedup() throws Exception {
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 1)));
        await("the first projection", Duration.ofSeconds(60),
                () -> projections.findById(order.id()).isPresent());
        Instant firstProcessedAt = projections.findById(order.id()).orElseThrow().getUpdatedAt();

        OutboxEvent event = outbox.findAll().stream()
                .filter(e -> e.getAggregateId().equals(order.id().toString()))
                .findFirst().orElseThrow();

        // Redeliver the exact record (same event-id header) as the broker legitimately may.
        Map<String, Object> props = Map.of(
                "bootstrap.servers", env.getRequiredProperty("spring.kafka.bootstrap-servers"),
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("order-events", event.getAggregateId(), event.getPayload());
            record.headers().add(new RecordHeader("event-id",
                    String.valueOf(event.getId()).getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(30, TimeUnit.SECONDS);
        }

        Thread.sleep(3000); // give the consumer time to (wrongly) reprocess — it must not
        assertEquals(firstProcessedAt, projections.findById(order.id()).orElseThrow().getUpdatedAt(),
                "a redelivered event id must be skipped, not reprocessed");
    }

    @Test
    void poisonEvent_parksOnTheDeadLetterTopic_neverTheProjection() {
        ChaosContext.open("poison-event");
        OrderResponse order = orchestrator.place(key(), List.of(new OrderLine("SKU-MOUSE", 1)));
        assertEquals("CONFIRMED", order.status(), "the order itself is fine — only the event is poisoned");

        await("the poison event to land on the DLT after bounded retries", Duration.ofSeconds(60),
                () -> deadLetters.findTop20ByOrderByReceivedAtDesc().stream()
                        .anyMatch(d -> d.getPayload().contains(order.id().toString())));
        var parked = deadLetters.findTop20ByOrderByReceivedAtDesc().stream()
                .filter(d -> d.getPayload().contains(order.id().toString()))
                .findFirst().orElseThrow();
        assertTrue(parked.getPayload().startsWith("POISON:"));
        assertNotNull(parked.getEventId(), "the dedup header rides along to the DLT");
        assertTrue(projections.findById(order.id()).isEmpty(),
                "a poison event must never half-apply to the read model");
    }

    private static void await(String what, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for: " + what);
            }
        }
        fail("timed out waiting for: " + what);
    }

    private static String key() {
        return "pipeline-" + UUID.randomUUID();
    }
}
