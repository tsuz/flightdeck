package io.axon.think;

import io.axon.think.config.AppConfig;
import io.axon.think.consumer.ThinkConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Think Consumer service.
 * <p>
 * Reads from {@code enriched-message-input}, calls RAG + Claude API,
 * and produces {@code ThinkResponse} to {@code think-request-response}.
 */
public class ThinkConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(ThinkConsumerApp.class);

    public static void main(String[] args) {
        log.info("Starting Think Consumer");
        log.info("  Kafka:       {}", AppConfig.BOOTSTRAP_SERVERS);
        log.info("  Input topic: {}", AppConfig.INPUT_TOPIC);
        log.info("  Output topic:{}", AppConfig.OUTPUT_TOPIC);
        log.info("  Claude model:{}", AppConfig.CLAUDE_MODEL);
        log.info("  RAG endpoint:{}", AppConfig.RAG_API_URL);

        try (ThinkConsumer consumer = new ThinkConsumer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                consumer.shutdown();
            }));

            consumer.run();
        } catch (Exception e) {
            log.error("Think Consumer failed", e);
            System.exit(1);
        }
    }
}
