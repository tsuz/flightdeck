package io.flightdeck.api;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer that reads UserResponse records from the message-output topic
 * and forwards them to connected WebSocket clients via {@link ChatWebSocketServer}.
 */
public class OutputConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutputConsumer.class);

    private static final String TOPIC = "message-output";
    private static final String BOOTSTRAP_SERVERS =
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String CONSUMER_GROUP =
            ChatApiApp.env("OUTPUT_CONSUMER_GROUP", "chat-api-output-group");

    private final ChatWebSocketServer wsServer;
    private volatile boolean running = true;

    public OutputConsumer(ChatWebSocketServer wsServer) {
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
            consumer.subscribe(List.of(TOPIC));
            log.info("Output consumer started — listening on topic: {}", TOPIC);

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    String sessionId = record.key() != null ? record.key() : "unknown";
                    log.info("[{}] Received response from message-output", sessionId);
                    wsServer.broadcastResponse(sessionId, record.value());
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("Output consumer error", e);
            }
        }
        log.info("Output consumer stopped");
    }

    public void stop() {
        running = false;
    }
}
