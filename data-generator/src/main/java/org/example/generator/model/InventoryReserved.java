package org.example.generator.model;

public record InventoryReserved(
        String checkoutId,
        String sku,
        int quantity
) {
}
