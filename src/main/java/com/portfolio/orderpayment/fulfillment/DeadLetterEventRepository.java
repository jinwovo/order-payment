package com.portfolio.orderpayment.fulfillment;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEvent, UUID> {

    List<DeadLetterEvent> findTop20ByOrderByReceivedAtDesc();
}
