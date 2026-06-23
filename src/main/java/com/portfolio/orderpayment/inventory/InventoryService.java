package com.portfolio.orderpayment.inventory;

import com.portfolio.orderpayment.catalog.ProductRepository;
import com.portfolio.orderpayment.ordering.OrderItem;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class InventoryService {

    private final ProductRepository products;

    /** Reserves stock for every item atomically. If any line can't be satisfied, the exception rolls
     *  back the reservations already made in this transaction — all-or-nothing. */
    @Transactional
    public void reserve(List<OrderItem> items) {
        for (OrderItem item : items) {
            int updated = products.reserve(item.getProductId(), item.getQuantity());
            if (updated == 0) {
                throw new OutOfStockException(item.getSku());
            }
        }
    }

    /** Compensating action: returns reserved stock when a later saga step fails. */
    @Transactional
    public void release(List<OrderItem> items) {
        for (OrderItem item : items) {
            products.release(item.getProductId(), item.getQuantity());
        }
    }
}
