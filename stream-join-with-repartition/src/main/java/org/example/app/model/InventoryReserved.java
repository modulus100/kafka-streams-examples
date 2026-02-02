package org.example.app.model;

public record InventoryReserved(
        String checkoutId,
        String sku,
        int quantity
) {
}
