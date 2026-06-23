package com.portfolio.orderpayment.inventory;

import lombok.Getter;

@Getter
public class OutOfStockException extends RuntimeException {

    private final String sku;

    public OutOfStockException(String sku) {
        super("out of stock: " + sku);
        this.sku = sku;
    }
}
