package org.example.generator;

import org.example.generator.model.InventoryReserved;
import org.example.generator.model.PaymentAuthorized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class CheckoutEventGenerator {
    private static final Logger logger = LoggerFactory.getLogger(CheckoutEventGenerator.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final List<String> skus = List.of("sku-123", "sku-456", "sku-789", "sku-abc");
    private final List<String> currencies = List.of("USD", "EUR", "GBP");

    @Value("${generator.payments-topic:payments-authorized}")
    private String paymentsTopic;

    @Value("${generator.inventory-topic:inventory-reserved}")
    private String inventoryTopic;

    @Value("${generator.customer-prefix:cust-}")
    private String customerPrefix;

    @Value("${generator.checkout-prefix:chk-}")
    private String checkoutPrefix;

    public CheckoutEventGenerator(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRateString = "${generator.interval-ms:1000}")
    public void emitEvents() {
        String checkoutId = checkoutPrefix + random.nextLong(1_000_000, 9_999_999);
        String customerId = customerPrefix + random.nextLong(10_000, 99_999);
        String sku = skus.get(random.nextInt(skus.size()));
        int quantity = random.nextInt(1, 5);
        long amountCents = random.nextLong(1_000, 20_000);
        String currency = currencies.get(random.nextInt(currencies.size()));

        PaymentAuthorized payment = new PaymentAuthorized(checkoutId, customerId, amountCents, currency);
        InventoryReserved inventory = new InventoryReserved(checkoutId, sku, quantity);

        kafkaTemplate.send(paymentsTopic, checkoutId, payment);
        kafkaTemplate.send(inventoryTopic, checkoutId, inventory);

        logger.info("Generated checkoutId={} customerId={} sku={} qty={} amountCents={} currency={}",
                checkoutId, customerId, sku, quantity, amountCents, currency);
    }
}
