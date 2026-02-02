package org.example.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Configuration
public class DeduplicationTopology {
    private static final String DEDUP_STORE_NAME = "deduplication-store";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private record DedupKey(
            String orderId,
            String customerId,
            String cardName,
            Long amountCents,
            String currency
    ) {
    }

    @Value("${deduplication.input-topic}")
    private String inputTopic;

    @Value("${deduplication.output-topic}")
    private String outputTopic;

    @Value("${deduplication.ttl-ms:60000}")
    private long ttlMs;

    @Value("${deduplication.cleanup-interval-ms:10000}")
    private long cleanupIntervalMs;

    @Bean
    public KStream<String, String> dedupStream(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DEDUP_STORE_NAME),
                Serdes.String(),
                Serdes.Long()
        );
        builder.addStateStore(storeBuilder);

        KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> deduped = input
                .selectKey((key, value) -> fingerprint(value))
                .process(processorSupplier(), DEDUP_STORE_NAME);

        deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return deduped;
    }

    private ProcessorSupplier<String, String, String, String> processorSupplier() {
        return () -> new DeduplicationProcessor(DEDUP_STORE_NAME, ttlMs, cleanupIntervalMs);
    }

    private String fingerprint(String value) {
        if (value == null) {
            return null;
        }

        try {
            JsonNode node = objectMapper.readTree(value);
            DedupKey key = new DedupKey(
                    normalize(textOrNull(node, "orderId")),
                    normalize(textOrNull(node, "customerId")),
                    normalize(textOrNull(node, "cardName")),
                    longOrNull(node, "amountCents"),
                    normalize(textOrNull(node, "currency"))
            );

            return Integer.toHexString(key.hashCode());
        } catch (Exception e) {
            DedupKey key = new DedupKey(null, null, normalize(value), null, null);
            return Integer.toHexString(key.hashCode());
        }
    }

    private static String textOrNull(JsonNode node, String fieldName) {
        if (node == null || fieldName == null) {
            return null;
        }

        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull() || field.isMissingNode()) {
            return null;
        }

        String v = field.asText();
        return v == null || v.isBlank() ? null : v;
    }

    private static Long longOrNull(JsonNode node, String fieldName) {
        if (node == null || fieldName == null) {
            return null;
        }

        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull() || field.isMissingNode()) {
            return null;
        }

        if (field.isNumber()) {
            return field.longValue();
        }

        try {
            String v = field.asText();
            if (v == null || v.isBlank()) {
                return null;
            }
            return Long.parseLong(v);
        } catch (Exception e) {
            return null;
        }
    }

    private static String normalize(String s) {
        if (s == null) {
            return null;
        }
        String v = s.trim();
        return v.isEmpty() ? null : v.toLowerCase();
    }

}
