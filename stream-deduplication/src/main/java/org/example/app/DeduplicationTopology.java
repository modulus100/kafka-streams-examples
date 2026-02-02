package org.example.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

@Configuration
public class DeduplicationTopology {
    private static final String DEDUP_STORE_NAME = "deduplication-store";

    private final ObjectMapper objectMapper = new ObjectMapper();

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
        return () -> new DeduplicationProcessor(ttlMs, cleanupIntervalMs);
    }

    private String fingerprint(String value) {
        if (value == null) {
            return null;
        }

//        try {
            JsonNode node = objectMapper.readTree(value);
            String canonical = node.toString();
            return sha256Hex(canonical);
//        } catch (JsonProcessingException e) {
//            return sha256Hex(value);
//        }
    }

    private static String sha256Hex(String input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }

        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static class DeduplicationProcessor implements org.apache.kafka.streams.processor.api.Processor<String, String, String, String> {
        private final long ttlMs;
        private final long cleanupIntervalMs;

        private ProcessorContext<String, String> context;
        private KeyValueStore<String, Long> store;

        private DeduplicationProcessor(long ttlMs, long cleanupIntervalMs) {
            this.ttlMs = ttlMs;
            this.cleanupIntervalMs = cleanupIntervalMs;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.store = context.getStateStore(DEDUP_STORE_NAME);

            context.schedule(Duration.ofMillis(cleanupIntervalMs), org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (var iter = store.all()) {
                    while (iter.hasNext()) {
                        var entry = iter.next();
                        Long lastSeen = entry.value;
                        if (lastSeen == null) {
                            store.delete(entry.key);
                            continue;
                        }
                        if (timestamp - lastSeen > ttlMs) {
                            store.delete(entry.key);
                        }
                    }
                }
            });
        }

        @Override
        public void process(Record<String, String> record) {
            if (record == null) {
                return;
            }

            String fingerprint = record.key();
            if (fingerprint == null) {
                return;
            }

            long now = context.currentSystemTimeMs();
            Long lastSeen = store.get(fingerprint);
            if (lastSeen == null || now - lastSeen > ttlMs) {
                store.put(fingerprint, now);
                context.forward(record);
            }
        }
    }
}
