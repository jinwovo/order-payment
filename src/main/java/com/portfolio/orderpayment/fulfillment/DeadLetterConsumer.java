package com.portfolio.orderpayment.fulfillment;

import com.portfolio.orderpayment.config.KafkaErrorHandlingConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/**
 * Drains {@code order-events.DLT} into a queryable table. Parking is terminal by design — a human
 * (or the demo UI) inspects the payload and the recorded failure reason; nothing retries from here.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final DeadLetterEventRepository deadLetters;

    @KafkaListener(topics = "${outbox.topic:order-events}.DLT", groupId = "fulfillment-dlt")
    @Transactional
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String eventId = header(record, "event-id");
        String reason = header(record, KafkaErrorHandlingConfig.DLT_REASON_HEADER);
        deadLetters.save(new DeadLetterEvent(eventId, record.value(), reason));
        log.warn("parked dead letter event-id={} reason={}", eventId, reason);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
