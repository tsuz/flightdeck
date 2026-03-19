package io.flightdeck.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Produces chat messages to the Kafka {@code message-input} topic.
 * The record key is the session_id so that all messages for a session
 * land on the same partition.
 */
public class KafkaMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-message-input";

    private static final String BOOTSTRAP_SERVERS =
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    private final KafkaProducer<String, String> producer;

    public KafkaMessageProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        log.info("Kafka producer initialized — bootstrap={} topic={}", BOOTSTRAP_SERVERS, TOPIC);
    }

    /**
     * Sends a message to the message-input topic, keyed by sessionId.
     */
    public void send(String sessionId, String messageJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sessionId, messageJson);
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
        log.info("Kafka producer closed");
    }
}
