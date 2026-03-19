package io.flightdeck.toolexec;

import io.flightdeck.toolexec.config.AppConfig;
import io.flightdeck.toolexec.consumer.ToolExecutionConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Tool Execution Consumer service.
 * <p>
 * Reads {@code ToolUseItem} from {@code tool-use}, executes the requested tool,
 * and produces {@code ToolUseResult} to {@code tool-use-result}.
 */
public class ToolExecutionApp {

    private static final Logger log = LoggerFactory.getLogger(ToolExecutionApp.class);

    public static void main(String[] args) {
        log.info("Starting Tool Execution Consumer");
        log.info("  Kafka:        {}", AppConfig.BOOTSTRAP_SERVERS);
        log.info("  Input topic:  {}", AppConfig.INPUT_TOPIC);
        log.info("  Output topic: {}", AppConfig.OUTPUT_TOPIC);

        try (ToolExecutionConsumer consumer = new ToolExecutionConsumer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                consumer.shutdown();
            }));

            consumer.run();
        } catch (Exception e) {
            log.error("Tool Execution Consumer failed", e);
            System.exit(1);
        }
    }
}
