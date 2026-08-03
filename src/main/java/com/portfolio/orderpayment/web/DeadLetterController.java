package com.portfolio.orderpayment.web;

import com.portfolio.orderpayment.fulfillment.DeadLetterEvent;
import com.portfolio.orderpayment.fulfillment.DeadLetterEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Read side of the DLT: what poison events got parked, and why (newest first). */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class DeadLetterController {

    private final DeadLetterEventRepository deadLetters;

    @GetMapping("/dead-letters")
    public List<DeadLetterView> deadLetters() {
        return deadLetters.findTop20ByOrderByReceivedAtDesc().stream()
                .map(DeadLetterView::from)
                .toList();
    }

    public record DeadLetterView(UUID id, String eventId, String payload, String reason,
                                 Instant receivedAt) {

        static DeadLetterView from(DeadLetterEvent e) {
            return new DeadLetterView(e.getId(), e.getEventId(), e.getPayload(), e.getReason(),
                    e.getReceivedAt());
        }
    }
}
