package com.portfolio.orderpayment.catalog;

public class UnknownProductException extends RuntimeException {

    public UnknownProductException(String sku) {
        super("unknown product: " + sku);
    }
}
