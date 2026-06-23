package com.portfolio.orderpayment.catalog;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    Optional<Product> findBySku(String sku);

    List<Product> findBySkuIn(List<String> skus);

    /**
     * Atomic, conditional stock decrement — the heart of oversell prevention. The {@code stock >= :qty}
     * guard means two concurrent reservations cannot both succeed past the available quantity; returns
     * the number of rows updated (0 = insufficient stock).
     */
    @Modifying
    @Query("update Product p set p.stock = p.stock - :qty where p.id = :id and p.stock >= :qty")
    int reserve(@Param("id") Long id, @Param("qty") int qty);

    @Modifying
    @Query("update Product p set p.stock = p.stock + :qty where p.id = :id")
    int release(@Param("id") Long id, @Param("qty") int qty);
}
