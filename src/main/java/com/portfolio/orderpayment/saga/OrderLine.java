package com.portfolio.orderpayment.saga;

/** Neutral input line for the saga, decoupling it from the web request DTO. */
public record OrderLine(String sku, int quantity) {
}
