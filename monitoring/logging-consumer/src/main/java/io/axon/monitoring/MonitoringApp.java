package io.axon.monitoring;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Monitoring consumer that subscribes to all pipeline topics
 * and logs every message with topic name, timestamp, key, and value.
 */
public class MonitoringApp {

    private static final Logger log = LoggerFactory.getLogger(MonitoringApp.class);

    private static final List<String> TOPICS = List.of(
            "message-input",
            "session-context",
            "enriched-message-input",
            "think-request-response",
            "tool-use",
            "tool-use-dlq",
            "tool-use-result",
            "tool-use-all-complete",
            "tool-use-latency",
            "session-cost",
            "message-output"
    );

    private static final String BOOTSTRAP_SERVERS =
            env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    private static final String CONSUMER_GROUP =
            env("KAFKA_CONSUMER_GROUP", "monitoring-consumer-group");

    public static void main(String[] args) {
        log.info("Starting Monitoring Consumer");
        log.info("  Kafka:  {}", BOOTSTRAP_SERVERS);
        log.info("  Topics: {}", TOPICS);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(TOPICS);
            log.info("Subscribed — waiting for messages...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    String time = Instant.ofEpochMilli(record.timestamp()).toString();
                    String key = record.key() != null ? record.key() : "null";
                    String value = record.value() != null ? record.value() : "null";

                    log.info("[{}] topic={} partition={} offset={} key={} value={}",
                            time, record.topic(), record.partition(), record.offset(), key, value);
                }
            }
        }
    }

private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
