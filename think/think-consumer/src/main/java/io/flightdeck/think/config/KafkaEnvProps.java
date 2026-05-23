package io.flightdeck.think.config;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Pass-through of {@code KAFKA_*} environment variables into a Kafka client
 * {@link Properties}. {@code KAFKA_FOO_BAR_BAZ} becomes {@code foo.bar.baz}.
 *
 * <p>Application-level variables that share the {@code KAFKA_} prefix but are
 * not Kafka client config are skipped (input/output topic, consumer group).
 * These will be renamed without the prefix in the future.
 *
 * <p>No validation is done — unknown keys are forwarded as-is and Kafka will
 * log a warning and ignore them.
 */
public final class KafkaEnvProps {

    private static final String PREFIX = "KAFKA_";

    private static final Set<String> SKIP = Set.of(
            "KAFKA_INPUT_TOPIC",
            "KAFKA_OUTPUT_TOPIC",
            "KAFKA_CONSUMER_GROUP"
    );

    private KafkaEnvProps() {}

    public static void apply(Properties props) {
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PREFIX) || SKIP.contains(key)) continue;
            String mapped = key.substring(PREFIX.length()).toLowerCase().replace('_', '.');
            props.put(mapped, entry.getValue());
        }
    }
}
