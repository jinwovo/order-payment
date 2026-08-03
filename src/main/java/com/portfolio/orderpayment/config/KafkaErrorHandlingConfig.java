package com.portfolio.orderpayment.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Consumer-side poison-event policy (ADR-0005): a record that keeps failing is retried a bounded
 * number of times with exponential backoff, then parked on {@code <topic>.DLT} instead of blocking
 * the partition or being dropped. Bounding the retries matters as much as having them — unbounded
 * redelivery of a permanently-broken record is a tiny metastable failure loop.
 *
 * <p>The DLT publication is deliberately explicit (headers copied, reason attached, send awaited)
 * so a failed parking attempt surfaces as a loud error instead of a silent drop.
 */
@Slf4j
@Configuration
public class KafkaErrorHandlingConfig {

    public static final String DLT_REASON_HEADER = "dlt-reason";
    public static final String DLT_ORIGINAL_TOPIC_HEADER = "dlt-original-topic";

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template,
                                                 MeterRegistry metrics,
                                                 @Value("${outbox.topic:order-events}") String topic) {
        String deadLetterTopic = topic + ".DLT";
        ExponentialBackOff backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(2000);
        backOff.setMaxAttempts(3);
        return new DefaultErrorHandler((record, ex) -> {
            metrics.counter("orders.events.dlt").increment();
            String reason = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
            log.warn("record from {}-{}@{} exhausted retries — parking on {}: {}",
                    record.topic(), record.partition(), record.offset(), deadLetterTopic, reason);
            try {
                ProducerRecord<String, String> dead = new ProducerRecord<>(
                        deadLetterTopic, null, (String) record.key(), (String) record.value());
                record.headers().forEach(h -> dead.headers().add(h));
                dead.headers().add(new RecordHeader(DLT_ORIGINAL_TOPIC_HEADER,
                        record.topic().getBytes(StandardCharsets.UTF_8)));
                dead.headers().add(new RecordHeader(DLT_REASON_HEADER,
                        String.valueOf(reason).getBytes(StandardCharsets.UTF_8)));
                template.send(dead).get(10, TimeUnit.SECONDS);
            } catch (Exception publishFailure) {
                log.error("DLT publication failed for {}-{}@{} — the record is being dropped",
                        record.topic(), record.partition(), record.offset(), publishFailure);
            }
        }, backOff);
    }
}
