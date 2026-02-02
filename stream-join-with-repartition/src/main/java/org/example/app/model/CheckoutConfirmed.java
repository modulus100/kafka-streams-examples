package org.example.app.model;

import java.time.Instant;

public record CheckoutConfirmed(
        String checkoutId,
        String customerId,
        String sku,
        int quantity,
        long amountCents,
        String currency,
        Instant joinedAt
) {
}
