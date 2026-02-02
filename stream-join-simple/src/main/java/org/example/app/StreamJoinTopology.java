package org.example.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.example.app.model.CheckoutConfirmed;
import org.example.app.model.InventoryReserved;
import org.example.app.model.PaymentAuthorized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;
import java.time.Instant;

@Configuration
public class StreamJoinTopology {
    private static final String PAYMENTS_TOPIC = "payments-authorized";
    private static final String INVENTORY_TOPIC = "inventory-reserved";
    private static final String OUTPUT_TOPIC = "checkout-confirmed";

    @Bean
    public KStream<String, CheckoutConfirmed> checkoutJoinStream(StreamsBuilder builder) {
        JacksonJsonSerde<PaymentAuthorized> paymentSerde = jsonSerde(PaymentAuthorized.class);
        JacksonJsonSerde<InventoryReserved> inventorySerde = jsonSerde(InventoryReserved.class);
        JacksonJsonSerde<CheckoutConfirmed> confirmedSerde = jsonSerde(CheckoutConfirmed.class);

        KStream<String, PaymentAuthorized> payments = builder.stream(
                PAYMENTS_TOPIC,
                Consumed.with(Serdes.String(), paymentSerde)
        ).selectKey((key, value) -> value == null ? null : value.checkoutId());

        KStream<String, InventoryReserved> inventory = builder.stream(
                INVENTORY_TOPIC,
                Consumed.with(Serdes.String(), inventorySerde)
        ).selectKey((key, value) -> value == null ? null : value.checkoutId());

        KStream<String, CheckoutConfirmed> joined = payments.join(
                inventory,
                (payment, reservation) -> new CheckoutConfirmed(
                        payment.checkoutId(),
                        payment.customerId(),
                        reservation.sku(),
                        reservation.quantity(),
                        payment.amountCents(),
                        payment.currency(),
                        Instant.now()
                ),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                StreamJoined.with(Serdes.String(), paymentSerde, inventorySerde)
        );

        joined.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), confirmedSerde));
        return joined;
    }

    private static <T> JacksonJsonSerde<T> jsonSerde(Class<T> targetType) {
        return new JacksonJsonSerde<T>(targetType)
                .ignoreTypeHeaders()
                .noTypeInfo();
    }
}
