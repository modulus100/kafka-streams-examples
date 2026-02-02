package org.example.app;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class DeduplicationProcessor implements Processor<String, String, String, String> {
    private final String storeName;
    private final long ttlMs;
    private final long cleanupIntervalMs;

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, Long> store;

    public DeduplicationProcessor(String storeName, long ttlMs, long cleanupIntervalMs) {
        this.storeName = storeName;
        this.ttlMs = ttlMs;
        this.cleanupIntervalMs = cleanupIntervalMs;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);

        context.schedule(
                Duration.ofMillis(cleanupIntervalMs),
                org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME,
                timestamp -> {
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
                }
        );
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
