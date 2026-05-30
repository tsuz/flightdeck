package io.flightdeck.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Produces async tool results to the Kafka {@code tool-use-result} topic — the
 * same topic the synchronous tool consumers write to. The record key is the
 * session_id so results co-partition with the aggregator's per-session store.
 *
 * <p>Fed by {@link ToolResponseHandler} when an external system calls back to
 * {@code POST /api/tool/response} with the result of an async tool.
 */
public class ToolResultProducer {

    private static final Logger log = LoggerFactory.getLogger(ToolResultProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-tool-use-result";

    private static final String BOOTSTRAP_SERVERS =
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    private final KafkaProducer<String, String> producer;

    public ToolResultProducer() {
        Properties props = new Properties();
        KafkaEnvProps.apply(props);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        log.info("Tool result producer initialized — bootstrap={} topic={}", BOOTSTRAP_SERVERS, TOPIC);
    }

    /** Sends a tool-use-result record to {@code tool-use-result}, keyed by sessionId. */
    public void send(String sessionId, String resultJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sessionId, resultJson);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to produce to {}", sessionId, TOPIC, exception);
            } else {
                log.debug("[{}] Produced to {} partition={} offset={}",
                        sessionId, TOPIC, metadata.partition(), metadata.offset());
            }
        });
    }

    public void close() {
        producer.close();
        log.info("Tool result producer closed");
    }
}
