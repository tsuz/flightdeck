package io.axon.toolexec.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.axon.toolexec.config.AppConfig;
import io.axon.toolexec.executor.ToolExecutor;
import io.axon.toolexec.executor.ToolExecutorRegistry;
import io.axon.toolexec.model.ToolUseItem;
import io.axon.toolexec.model.ToolUseResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer that reads {@link ToolUseItem} from {@code tool-use},
 * executes the requested tool function via the {@link ToolExecutorRegistry},
 * and produces a {@link ToolUseResult} to {@code tool-use-result}.
 */
public class ToolExecutionConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ToolExecutionConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final ToolExecutorRegistry registry;
    private volatile boolean running = true;

    public ToolExecutionConsumer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.consumer = createConsumer();
        this.producer = createProducer();
        this.registry = new ToolExecutorRegistry(mapper);
    }

    /**
     * Main poll loop — consumes, executes tools, and produces results until shutdown.
     */
    public void run() {
        consumer.subscribe(List.of(AppConfig.INPUT_TOPIC));
        log.info("Tool execution consumer started — listening on topic: {}", AppConfig.INPUT_TOPIC);

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(
                    Duration.ofMillis(AppConfig.POLL_TIMEOUT_MS));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    processRecord(record);
                } catch (Exception e) {
                    log.error("[{}] Failed to process record at offset {}: {}",
                            record.key(), record.offset(), e.getMessage(), e);
                }
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }

        log.info("Tool execution consumer shutting down");
    }

    /**
     * Processes a single tool-use record:
     * 1. Deserialize ToolUseItem
     * 2. Look up the executor for the tool name
     * 3. Execute the tool and measure latency
     * 4. Produce ToolUseResult to tool-use-result
     */
    void processRecord(ConsumerRecord<String, String> record) throws Exception {
        String sessionId = record.key();

        // 1. Deserialize input
        ToolUseItem item = mapper.readValue(record.value(), ToolUseItem.class);

        log.info("[{}] Executing tool: name={} tool_use_id={}", sessionId, item.name(), item.toolUseId());

        // 2. Look up executor
        ToolExecutor executor = registry.get(item.name());

        ToolUseResult result;
        if (executor == null) {
            log.warn("[{}] No executor registered for tool: {}", sessionId, item.name());
            result = new ToolUseResult(
                    sessionId,
                    item.toolUseId(),
                    item.name(),
                    Map.of("error", "Unknown tool: " + item.name()),
                    0L,
                    "error",
                    Instant.now().toString()
            );
        } else {
            // 3. Execute and measure latency
            result = executeTool(executor, item, sessionId);
        }

        // 4. Produce to tool-use-result
        String outputJson = mapper.writeValueAsString(result);
        ProducerRecord<String, String> outputRecord = new ProducerRecord<>(
                AppConfig.OUTPUT_TOPIC, sessionId, outputJson);

        producer.send(outputRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to produce ToolUseResult: {}", sessionId, exception.getMessage());
            } else {
                log.info("[{}] ToolUseResult produced: tool={} status={} latency={}ms partition={} offset={}",
                        sessionId, result.name(), result.status(), result.latencyMs(),
                        metadata.partition(), metadata.offset());
            }
        });
        producer.flush();
    }

    /**
     * Executes a tool, captures the result or error, and measures latency.
     */
    private ToolUseResult executeTool(ToolExecutor executor, ToolUseItem item, String sessionId) {
        long startMs = System.currentTimeMillis();

        try {
            Map<String, Object> toolResult = executor.execute(item.input());
            long latencyMs = System.currentTimeMillis() - startMs;

            log.info("[{}] Tool {} executed successfully in {}ms", sessionId, item.name(), latencyMs);

            return new ToolUseResult(
                    sessionId,
                    item.toolUseId(),
                    item.name(),
                    toolResult,
                    latencyMs,
                    "success",
                    Instant.now().toString()
            );

        } catch (Exception e) {
            long latencyMs = System.currentTimeMillis() - startMs;

            log.error("[{}] Tool {} failed after {}ms: {}", sessionId, item.name(), latencyMs, e.getMessage());

            return new ToolUseResult(
                    sessionId,
                    item.toolUseId(),
                    item.name(),
                    Map.of("error", e.getMessage() != null ? e.getMessage() : "Unknown error"),
                    latencyMs,
                    "error",
                    Instant.now().toString()
            );
        }
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void close() {
        shutdown();
        consumer.close();
        producer.close();
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(props);
    }
}
