package com.portfolio.orderpayment.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox and relays unpublished events to Kafka, marking each row published only after the
 * broker acknowledges. A send that fails throws, rolling back the batch so the rows are retried next
 * cycle — at-least-once delivery decoupled from the request path. Consumers dedupe by event id.
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
        List<OutboxEvent> batch = outbox.findTop100ByPublishedAtIsNullOrderByIdAsc();
        for (OutboxEvent event : batch) {
            try {
                // Block on the ack so we only mark published once Kafka has the record.
                kafka.send(topic, event.getAggregateId(), event.getPayload()).get(5, TimeUnit.SECONDS);
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
