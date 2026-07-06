package com.portfolio.orderpayment.web;

import com.portfolio.orderpayment.catalog.Product;
import com.portfolio.orderpayment.catalog.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only catalog view with live stock, so clients can watch reservations and compensations
 *  move real inventory. */
@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class CatalogController {

    private final ProductRepository products;

    @GetMapping
    public List<ProductResponse> list() {
        return products.findAll(Sort.by("id")).stream().map(ProductResponse::from).toList();
    }

    public record ProductResponse(String sku, String name, long priceCents, int stock) {

        static ProductResponse from(Product p) {
            return new ProductResponse(p.getSku(), p.getName(), p.getPriceCents(), p.getStock());
        }
    }
}
