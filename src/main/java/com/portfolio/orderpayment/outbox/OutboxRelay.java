package com.portfolio.orderpayment.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox and relays unpublished events to Kafka, marking each row published only after the
 * broker acknowledges. The outbox row id is attached as an {@code event-id} header so the consumer
 * can deduplicate, and the stored {@code traceparent} rides along so the consumer joins the original
 * request's trace (ADR-0006). A send that fails throws, rolling back the batch so rows are retried
 * next cycle — at-least-once delivery decoupled from the request path.
 *
 * <p>The batch is claimed with {@code FOR UPDATE SKIP LOCKED}, so multiple app replicas can run the
 * relay concurrently without double-publishing: each replica locks a disjoint batch (kept honest by
 * the consumer-side dedup either way, since Kafka delivery itself is at-least-once).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafka;

    @Value("${outbox.topic:order-events}")
    private String topic;

    @Scheduled(fixedDelayString = "${outbox.relay-interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outbox.lockPendingBatch();
        for (OutboxEvent event : batch) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, event.getAggregateId(), event.getPayload());
            record.headers().add(new RecordHeader("event-id",
                    String.valueOf(event.getId()).getBytes(StandardCharsets.UTF_8)));
            if (event.getTraceParent() != null) {
                record.headers().add(new RecordHeader("traceparent",
                        event.getTraceParent().getBytes(StandardCharsets.UTF_8)));
            }
            try {
                // Block on the ack so we only mark published once Kafka has the record.
                kafka.send(record).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("kafka publish failed for outbox #" + event.getId(), e);
            }
            event.markPublished();
        }
        if (!batch.isEmpty()) {
            log.info("relayed {} outbox event(s) to topic {}", batch.size(), topic);
        }
    }
}
