package com.portfolio.orderpayment.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyKeyRepository keys;

    @Transactional(readOnly = true)
    public Optional<UUID> findOrderId(String key) {
        return keys.findById(key).map(IdempotencyKey::getOrderId);
    }

    /**
     * Claims the key for this order. Flushes immediately so a duplicate concurrent request hits the
     * primary-key constraint and surfaces as {@code DataIntegrityViolationException}, letting the
     * caller fall back to the already-created order instead of processing twice.
     */
    @Transactional
    public void claim(String key, UUID orderId) {
        keys.saveAndFlush(new IdempotencyKey(key, orderId));
    }
}
