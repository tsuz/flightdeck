package io.flightdeck.api;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces chat messages to the Kafka {@code message-input} topic.
 * The record key is the session_id so that all messages for a session
 * land on the same partition.
 *
 * <p>Wraps the process-wide shared {@link Producer} (see
 * {@link KafkaProducerFactory}); it does not own the producer and so does not
 * close it.
 */
public class KafkaMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-message-input";

    private final Producer<String, String> producer;

    public KafkaMessageProducer(Producer<String, String> producer) {
        this.producer = producer;
        log.info("Kafka message producer wired to shared producer — topic={}", TOPIC);
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
}
