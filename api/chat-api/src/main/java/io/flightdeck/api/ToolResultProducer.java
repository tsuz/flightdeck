package io.flightdeck.api;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces async tool results to the Kafka {@code tool-use-result} topic — the
 * same topic the synchronous tool consumers write to. The record key is the
 * session_id so results co-partition with the aggregator's per-session store.
 * <p>Fed by {@link ToolResponseHandler} when an external system calls back to
 * {@code POST /api/tools/response} with the result of an async tool.
 *
 * <p>Wraps the process-wide shared {@link Producer} (see
 * {@link KafkaProducerFactory}); it does not own the producer and so does not
 * close it.
 */
public class ToolResultProducer {

    private static final Logger log = LoggerFactory.getLogger(ToolResultProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-tool-use-result";

    private final Producer<String, String> producer;

    public ToolResultProducer(Producer<String, String> producer) {
        this.producer = producer;
        log.info("Tool result producer wired to shared producer — topic={}", TOPIC);
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
}
