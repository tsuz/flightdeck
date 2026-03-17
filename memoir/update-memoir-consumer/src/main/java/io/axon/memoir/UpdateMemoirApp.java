package io.axon.memoir;

import io.axon.memoir.config.AppConfig;
import io.axon.memoir.consumer.UpdateMemoirConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Update Memoir Consumer.
 * Reads session-end snapshots, calls Claude Sonnet to update the memoir,
 * and writes the result back to the memoir-context topic.
 */
public class UpdateMemoirApp {

    private static final Logger log = LoggerFactory.getLogger(UpdateMemoirApp.class);

    public static void main(String[] args) {
        log.info("Starting Update Memoir Consumer");
        log.info("  Kafka:        {}", AppConfig.BOOTSTRAP_SERVERS);
        log.info("  Input topic:  {}", AppConfig.INPUT_TOPIC);
        log.info("  Output topic: {}", AppConfig.OUTPUT_TOPIC);
        log.info("  Claude model: {}", AppConfig.CLAUDE_MODEL);

        try (UpdateMemoirConsumer consumer = new UpdateMemoirConsumer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                consumer.shutdown();
            }));
            consumer.run();
        } catch (Exception e) {
            log.error("Update Memoir Consumer failed", e);
            System.exit(1);
        }
    }
}
