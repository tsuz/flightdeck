package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * Consumes all pipeline topics and forwards each record as a
 * {@code pipeline_event} WebSocket message to connected clients.
 */
public class PipelineConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PipelineConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

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
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String CONSUMER_GROUP =
            ChatApiApp.env("PIPELINE_CONSUMER_GROUP", "chat-api-pipeline-group");

    private final ChatWebSocketServer wsServer;
    private volatile boolean running = true;

    public PipelineConsumer(ChatWebSocketServer wsServer) {
        this.wsServer = wsServer;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(TOPICS);
            log.info("Pipeline consumer started — listening on {} topics", TOPICS.size());

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String envelope = buildPipelineEvent(record);
                        if (envelope != null) {
                            wsServer.broadcastPipelineEvent(record.key(), envelope);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to process pipeline record from {}: {}",
                                record.topic(), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("Pipeline consumer error", e);
            }
        }
        log.info("Pipeline consumer stopped");
    }

    private String buildPipelineEvent(ConsumerRecord<String, String> record) {
        try {
            String sessionId = record.key() != null ? record.key() : "unknown";
            String topic = record.topic();
            String kafkaTimestamp = Instant.ofEpochMilli(record.timestamp()).toString();

            // Parse the value as JSON for structured display
            JsonNode value = null;
            try {
                value = mapper.readTree(record.value());
            } catch (Exception ignored) {
                // value may not be valid JSON
            }

            ObjectNode event = mapper.createObjectNode();
            event.put("topic", topic);
            event.put("sessionId", sessionId);
            event.put("timestamp", kafkaTimestamp);
            event.put("partition", record.partition());
            event.put("offset", record.offset());

            if (value != null) {
                event.set("value", value);

                // Extract cost info for LLM-related topics
                if ("think-request-response".equals(topic)) {
                    ObjectNode cost = mapper.createObjectNode();
                    if (value.has("input_tokens")) cost.put("inputTokens", value.get("input_tokens").asInt());
                    if (value.has("output_tokens")) cost.put("outputTokens", value.get("output_tokens").asInt());
                    if (value.has("cost")) cost.put("dollars", value.get("cost").asDouble());
                    if (value.has("end_turn")) cost.put("endTurn", value.get("end_turn").asBoolean());
                    if (cost.size() > 0) event.set("cost", cost);
                }

                // Extract latency for tool results
                if ("tool-use-result".equals(topic) && value.has("latency_ms")) {
                    event.put("latencyMs", value.get("latency_ms").asLong());
                }
            } else {
                event.put("rawValue", record.value() != null ? record.value() : "null");
            }

            // Wrap in WebSocket envelope
            ObjectNode envelope = mapper.createObjectNode();
            envelope.put("type", "pipeline_event");
            envelope.set("data", event);

            return mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            log.warn("Failed to build pipeline event: {}", e.getMessage());
            return null;
        }
    }

    public void stop() {
        running = false;
    }
}
