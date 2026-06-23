package com.portfolio.orderpayment.fulfillment;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderProjectionRepository extends JpaRepository<OrderProjection, UUID> {
}
