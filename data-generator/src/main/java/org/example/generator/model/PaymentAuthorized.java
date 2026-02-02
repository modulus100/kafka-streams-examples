package org.example.generator.model;

public record PaymentAuthorized(
        String checkoutId,
        String customerId,
        long amountCents,
        String currency
) {
}
