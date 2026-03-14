package io.axon.think.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.axon.think.config.AppConfig;
import io.axon.think.model.FullSessionContext;
import io.axon.think.model.ThinkResponse;
import io.axon.think.service.ClaudeApiService;
import io.axon.think.service.RagService;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer that reads {@link FullSessionContext} from {@code enriched-message-input},
 * calls RAG for additional context, invokes the Claude API with available tools,
 * and produces a {@link ThinkResponse} to {@code think-request-response}.
 */
public class ThinkConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThinkConsumer.class);

    private static final String SYSTEM_PROMPT_TEMPLATE = """
            You are an intelligent AI assistant with access to various tools.
            Analyze the user's request and determine the best course of action.
            Use the available tools when needed to fulfill the user's request.
            If you can answer directly without tools, do so.

            Be concise and helpful. When using tools, explain what you're doing and why.

            %s""";

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final RagService ragService;
    private final ClaudeApiService claudeApiService;
    private volatile boolean running = true;

    public ThinkConsumer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.consumer = createConsumer();
        this.producer = createProducer();
        this.ragService = new RagService(mapper);
        this.claudeApiService = new ClaudeApiService(mapper);
    }

    /**
     * Main poll loop — consumes, processes, and produces until shutdown.
     */
    public void run() {
        consumer.subscribe(List.of(AppConfig.INPUT_TOPIC));
        log.info("Think consumer started — listening on topic: {}", AppConfig.INPUT_TOPIC);

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

        log.info("Think consumer shutting down");
    }

    /**
     * Processes a single enriched-message-input record:
     * 1. Deserialize FullSessionContext
     * 2. Query RAG for relevant context
     * 3. Build Claude API request with tools
     * 4. Produce ThinkResponse to think-request-response
     */
    void processRecord(ConsumerRecord<String, String> record) throws Exception {
        // 1. Deserialize input
        FullSessionContext context = mapper.readValue(record.value(), FullSessionContext.class);

        // Use record key if present, otherwise fall back to session_id from the payload
        String sessionId = record.key() != null ? record.key() : context.sessionId();
        String userId = context.userId();

        log.info("[{}] Processing enriched input: history_size={} latest_role={}",
                sessionId,
                context.history() != null ? context.history().size() : 0,
                context.latestInput() != null ? context.latestInput().role() : "null");

        // 2. Query RAG for prompt context
        String latestContent = context.latestInput() != null ? context.latestInput().content() : "";
        List<String> ragChunks = ragService.retrieveContext(latestContent, sessionId);

        // 3. Build system prompt with RAG context
        String systemPrompt = buildSystemPrompt(ragChunks);

        // 4. Convert history + latest input to Claude message format
        List<Map<String, Object>> claudeMessages = ClaudeApiService.toClaudeMessages(
                context.history(), context.latestInput());

        // 5. Call Claude API
        ThinkResponse thinkResponse = claudeApiService.call(systemPrompt, claudeMessages, sessionId, userId);

        // 6. Produce to think-request-response
        String outputJson = mapper.writeValueAsString(thinkResponse);
        ProducerRecord<String, String> outputRecord = new ProducerRecord<>(
                AppConfig.OUTPUT_TOPIC, sessionId, outputJson);

        producer.send(outputRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to produce ThinkResponse: {}", sessionId, exception.getMessage());
            } else {
                log.info("[{}] ThinkResponse produced to {} partition={} offset={}",
                        sessionId, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
        producer.flush();
    }

    /**
     * Builds the system prompt, injecting RAG context if available.
     */
    static String buildSystemPrompt(List<String> ragChunks) {
        if (ragChunks == null || ragChunks.isEmpty()) {
            return String.format(SYSTEM_PROMPT_TEMPLATE, "");
        }

        StringBuilder ragContext = new StringBuilder("\n\nRelevant context from knowledge base:\n");
        for (int i = 0; i < ragChunks.size(); i++) {
            ragContext.append(String.format("\n--- Document %d ---\n%s\n", i + 1, ragChunks.get(i)));
        }
        ragContext.append("\nUse the above context to inform your response when relevant.");

        return String.format(SYSTEM_PROMPT_TEMPLATE, ragContext.toString());
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
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // Process one at a time for LLM calls
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
