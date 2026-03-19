package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;

/**
 * Detects session inactivity by tracking the last message time per session ID
 * on the {@code think-request-response} stream.
 *
 * <p>A wall-clock punctuator runs every 30 seconds. If a session has had no
 * activity for over 1 minute, it emits a record to the {@code session-end}
 * topic (key = session ID, value = "{}") and removes the tracking entry.
 */
public class SessionEndProcessor {

    private static final Logger log = LoggerFactory.getLogger(SessionEndProcessor.class);

    public static final String LAST_SEEN_STORE = "session-last-seen-store";
    public static final Duration INACTIVITY_THRESHOLD = Duration.ofSeconds(
            Long.parseLong(System.getenv().getOrDefault("MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS", "20")));
    static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(5);

    public static void register(StreamsBuilder builder, KStream<String, ThinkResponse> thinkStream) {
        register(builder, thinkStream, INACTIVITY_THRESHOLD);
    }

    public static void register(StreamsBuilder builder, KStream<String, ThinkResponse> thinkStream,
                                Duration inactivityThreshold) {

        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LAST_SEEN_STORE),
                        Serdes.String(),
                        Serdes.Long()
                );

        builder.addStateStore(storeBuilder);

        thinkStream
                .mapValues(v -> v != null ? "{}" : null)
                .process(new InactivityProcessorSupplier(inactivityThreshold), LAST_SEEN_STORE)
                .filter((k, v) -> v != null)
                .to(Topics.SESSION_END, Produced.with(Serdes.String(), Serdes.String()));
    }

    static class InactivityProcessorSupplier
            implements ProcessorSupplier<String, String, String, String> {

        private final Duration inactivityThreshold;

        InactivityProcessorSupplier(Duration inactivityThreshold) {
            this.inactivityThreshold = inactivityThreshold;
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Set.of();
        }

        @Override
        public ContextualProcessor<String, String, String, String> get() {
            return new InactivityProcessor(inactivityThreshold);
        }
    }

    static class InactivityProcessor
            extends ContextualProcessor<String, String, String, String> {

        private final Duration inactivityThreshold;
        private KeyValueStore<String, Long> lastSeenStore;

        InactivityProcessor(Duration inactivityThreshold) {
            this.inactivityThreshold = inactivityThreshold;
        }

        @Override
        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
            super.init(context);
            this.lastSeenStore = context.getStateStore(LAST_SEEN_STORE);

            context.schedule(PUNCTUATE_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::checkInactivity);

            log.info("SessionEndProcessor initialized: inactivity_threshold={}s punctuate_interval={}s",
                    inactivityThreshold.getSeconds(), PUNCTUATE_INTERVAL.getSeconds());
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.key() == null || record.value() == null) return;
            // Update last-seen timestamp; don't forward the record downstream
            lastSeenStore.put(record.key(), System.currentTimeMillis());
        }

        private void checkInactivity(long punctuateTimestamp) {
            long now = System.currentTimeMillis();
            long thresholdMs = inactivityThreshold.toMillis();

            try (KeyValueIterator<String, Long> iter = lastSeenStore.all()) {
                while (iter.hasNext()) {
                    var entry = iter.next();
                    long elapsed = now - entry.value;

                    if (elapsed > thresholdMs) {
                        String sessionId = entry.key;
                        log.info("[{}] Session inactive for {}s — emitting session-end",
                                sessionId, elapsed / 1000);

                        context().forward(new Record<>(sessionId, "{}", now));
                        lastSeenStore.delete(sessionId);
                    }
                }
            }
        }
    }
}
