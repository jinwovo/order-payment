package com.portfolio.orderpayment.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Polls the outbox and relays unpublished events. Here it logs them to prove the mechanism; in
 * production the same loop would publish to Kafka and mark each row published in the same tx, giving
 * at-least-once delivery decoupled from the request path.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outbox;

    @Scheduled(fixedDelayString = "${outbox.relay-interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outbox.findTop100ByPublishedAtIsNullOrderByIdAsc();
        for (OutboxEvent event : batch) {
            log.info("OUTBOX -> {} {} #{} {}", event.getAggregateType(), event.getType(), event.getId(), event.getPayload());
            event.markPublished();
        }
    }
}
