package org.example.app.model;

public record PaymentAuthorized(
        String checkoutId,
        String customerId,
        long amountCents,
        String currency
) {
}
