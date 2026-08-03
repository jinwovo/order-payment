package com.portfolio.orderpayment.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Claims a batch of unpublished rows with {@code FOR UPDATE SKIP LOCKED}: concurrent relays
     * (multiple app replicas) each lock a disjoint batch instead of double-publishing the same
     * rows. Must run inside the transaction that also marks the rows published.
     */
    @Query(value = "select * from outbox_event where published_at is null "
            + "order by id asc limit 100 for update skip locked", nativeQuery = true)
    List<OutboxEvent> lockPendingBatch();
}
