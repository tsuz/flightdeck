package io.flightdeck.think.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.flightdeck.think.config.AppConfig;
import io.flightdeck.think.model.FullSessionContext;
import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;
import io.flightdeck.think.service.ClaudeApiService;
import io.flightdeck.think.service.GeminiApiService;
import io.flightdeck.think.service.LlmApiService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer that reads {@link FullSessionContext} from {@code enriched-message-input},
 * invokes the Claude API with available tools,
 * and produces a {@link ThinkResponse} to {@code think-request-response}.
 */
public class ThinkConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ThinkConsumer.class);

    private static final String DEFAULT_SYSTEM_PROMPT = """
            You are an intelligent AI assistant with access to various tools.
            Analyze the user's request and determine the best course of action.
            Use the available tools when needed to fulfill the user's request.
            If you can answer directly without tools, do so.

            Be concise and helpful. When using tools, explain what you're doing and why.""";

    private static final String SYSTEM_PROMPT_TEMPLATE = loadSystemPromptTemplate() + "\n\n%s";

    private static String loadSystemPromptTemplate() {
        return loadSystemPromptFromFile(AppConfig.SYSTEM_PROMPT_FILE);
    }

    /**
     * Loads a system prompt from a file path. Returns the default prompt if
     * the path is null or blank. Throws if the file is specified but not found.
     */
    static String loadSystemPromptFromFile(String file) {
        if (file == null || file.isBlank()) {
            log.info("SYSTEM_PROMPT_FILE not set — using default system prompt");
            return DEFAULT_SYSTEM_PROMPT;
        }
        java.nio.file.Path path = java.nio.file.Path.of(file);
        if (!java.nio.file.Files.exists(path)) {
            throw new IllegalStateException("SYSTEM_PROMPT_FILE not found: " + path.toAbsolutePath());
        }
        try {
            String prompt = java.nio.file.Files.readString(path);
            log.info("Loaded system prompt from {}", file);
            return prompt;
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Failed to read SYSTEM_PROMPT_FILE: " + path.toAbsolutePath(), e);
        }
    }

    private final KafkaConsumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final ObjectMapper mapper;
    private final LlmApiService llmApiService;
    private volatile boolean running = true;

    public ThinkConsumer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.consumer = createConsumer();
        this.producer = createProducer();
        this.llmApiService = createLlmService(mapper);
    }

    /** Package-private constructor for unit testing with mock dependencies. */
    ThinkConsumer(LlmApiService llmApiService, Producer<String, String> producer, ObjectMapper mapper) {
        this.llmApiService = llmApiService;
        this.mapper = mapper;
        this.consumer = null;
        this.producer = producer;
    }

    private static LlmApiService createLlmService(ObjectMapper mapper) {
        String provider = AppConfig.LLM_PROVIDER.toLowerCase();
        return switch (provider) {
            case "gemini" -> {
                if (AppConfig.GEMINI_API_KEY.isBlank()) {
                    throw new IllegalStateException(
                            "GEMINI_API_KEY is required when LLM_PROVIDER=gemini");
                }
                log.info("Using Gemini LLM provider (model={})", AppConfig.GEMINI_MODEL);
                yield new GeminiApiService(mapper);
            }
            default -> {
                if (AppConfig.CLAUDE_API_KEY.isBlank()) {
                    throw new IllegalStateException(
                            "CLAUDE_API_KEY is required when LLM_PROVIDER=claude");
                }
                log.info("Using Claude LLM provider (model={})", AppConfig.CLAUDE_MODEL);
                yield new ClaudeApiService(mapper);
            }
        };
    }

    /**
     * Main poll loop — consumes, processes, and produces until shutdown.
     */
    public void run() {
        consumer.subscribe(List.of(AppConfig.INPUT_TOPIC));
        log.info("Think consumer started — listening on topic: {}", AppConfig.INPUT_TOPIC);
        log.info("Compaction config: COMPACTION_USER_MESSAGE_TRIGGER={} COMPACTION_USER_MESSAGE_UNTIL={} COMPACTION_PROMPT={}",
                AppConfig.COMPACTION_USER_MESSAGE_TRIGGER,
                AppConfig.COMPACTION_USER_MESSAGE_UNTIL,
                AppConfig.COMPACTION_PROMPT.length() > 80
                        ? AppConfig.COMPACTION_PROMPT.substring(0, 80) + "..."
                        : AppConfig.COMPACTION_PROMPT);

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
     * 2. Build Claude API request with tools
     * 3. Produce ThinkResponse to think-request-response
     */
    void processRecord(ConsumerRecord<String, String> record) throws Exception {
        // 1. Deserialize input
        FullSessionContext context = mapper.readValue(record.value(), FullSessionContext.class);

        String sessionId = record.key() != null ? record.key() : context.sessionId();
        String userId = context.userId();

        log.info("[{}] Processing enriched input: history_size={} latest_role={}",
                sessionId,
                context.history() != null ? context.history().size() : 0,
                context.latestInput() != null ? context.latestInput().role() : "null");

        // 2. Check session budget
        if (AppConfig.BUDGET_PRICE_PER_SESSION != null
                && context.cost() != null
                && context.cost() >= AppConfig.BUDGET_PRICE_PER_SESSION) {

            log.warn("[{}] Session budget exceeded: cumulative_cost=${} >= budget=${}",
                    sessionId,
                    String.format("%.6f", context.cost()),
                    String.format("%.2f", AppConfig.BUDGET_PRICE_PER_SESSION));

            String budgetMessage = String.format(
                    "You have used too many tokens. Session budget of $%.2f has been reached.",
                    AppConfig.BUDGET_PRICE_PER_SESSION);

            ThinkResponse budgetResponse = new ThinkResponse(
                    sessionId, userId, null, context.cost(), 0, 0,
                    context.history(),
                    context.latestInput(),
                    List.of(new MessageInput(sessionId, userId, "assistant", budgetMessage,
                            java.time.Instant.now().toString(), null)),
                    null, true, java.time.Instant.now().toString());

            produceResponse(sessionId, budgetResponse);
            return;
        }

        // 3. Compact history if user message count exceeds trigger
        List<MessageInput> effectiveHistory = context.history();

        if (AppConfig.COMPACTION_USER_MESSAGE_TRIGGER > 0
                && effectiveHistory != null) {

            long userMessageCount = effectiveHistory.stream()
                    .filter(m -> "user".equals(m.role()))
                    .count();

            if (userMessageCount >= AppConfig.COMPACTION_USER_MESSAGE_TRIGGER) {
                int splitIndex = findCompactionSplitIndex(
                        effectiveHistory, AppConfig.COMPACTION_USER_MESSAGE_UNTIL);

                if (splitIndex > 0) {
                    // Guard: skip compaction during active tool loop
                    boolean midToolLoop = context.latestInput() != null
                            && "tool".equals(context.latestInput().role());

                    if (!midToolLoop) {
                        log.info("[{}] Compacting history: {} user messages >= trigger {}, keeping from index {}",
                                sessionId, userMessageCount,
                                AppConfig.COMPACTION_USER_MESSAGE_TRIGGER, splitIndex);

                        List<MessageInput> oldMessages = effectiveHistory.subList(0, splitIndex);
                        List<MessageInput> recentMessages = effectiveHistory.subList(
                                splitIndex, effectiveHistory.size());

                        // Summarize old messages via LLM
                        List<Map<String, Object>> summaryInput =
                                llmApiService.toApiMessages(oldMessages, null);
                        ThinkResponse summaryResponse = llmApiService.call(
                                AppConfig.COMPACTION_PROMPT, summaryInput, sessionId, userId);

                        String summaryText = extractTextFromMessages(
                                summaryResponse.lastInputResponse());

                        MessageInput summaryMsg = new MessageInput(
                                sessionId, userId, "assistant",
                                "[Conversation Summary]\n" + summaryText,
                                java.time.Instant.now().toString(), null);

                        List<MessageInput> compacted = new ArrayList<>();
                        compacted.add(summaryMsg);
                        compacted.addAll(recentMessages);
                        effectiveHistory = compacted;

                        log.info("[{}] History compacted: {} messages → {}",
                                sessionId, context.history().size(), effectiveHistory.size());
                    }
                }
            }
        }

        // 4. Build system prompt with memoir context
        String systemPrompt = buildSystemPrompt(context.memoirContext());

        // 5. Convert history + latest input to LLM provider message format
        List<Map<String, Object>> llmMessages = llmApiService.toApiMessages(
                effectiveHistory, context.latestInput());

        // 6. Call LLM API
        ThinkResponse thinkResponse = llmApiService.call(systemPrompt, llmMessages, sessionId, userId);

        // 7. Build final ThinkResponse with previousMessages, lastInputMessage, lastInputResponse
        Double prevSessionCost = context.cost();

        thinkResponse = new ThinkResponse(
                sessionId,
                userId,
                thinkResponse.cost(),
                prevSessionCost,
                thinkResponse.inputTokens(),
                thinkResponse.outputTokens(),
                effectiveHistory,                           // previousMessages
                context.latestInput(),                      // lastInputMessage
                thinkResponse.lastInputResponse(),          // lastInputResponse (from LLM service)
                thinkResponse.toolUses(),
                thinkResponse.endTurn(),
                thinkResponse.timestamp());

        // 8. Produce to think-request-response
        log.info("[{}] Producing ThinkResponse: previousMessages={} lastInputMessage={} lastInputResponse={}",
                sessionId,
                thinkResponse.previousMessages() != null ? thinkResponse.previousMessages().size() : 0,
                thinkResponse.lastInputMessage() != null ? thinkResponse.lastInputMessage().role() : "null",
                thinkResponse.lastInputResponse() != null ? thinkResponse.lastInputResponse().size() : 0);

        produceResponse(sessionId, thinkResponse);
    }

    private void produceResponse(String sessionId, ThinkResponse response) throws Exception {
        String outputJson = mapper.writeValueAsString(response);
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
     * Builds the system prompt, injecting memoir context if available.
     */
    static String buildSystemPrompt(String memoirContext) {
        StringBuilder extra = new StringBuilder();

        if (memoirContext != null && !memoirContext.isBlank()) {
            extra.append("\n\nUser memoir (known facts about this user from previous sessions):\n");
            extra.append(memoirContext);
            extra.append("\n\nUse the memoir to personalize your responses.");
        }

        return String.format(SYSTEM_PROMPT_TEMPLATE, extra.toString());
    }

    /**
     * Emits an error response to the output topic so the user sees the failure.
     */
    void emitErrorResponse(ConsumerRecord<String, String> record, Exception e) {
        try {
            String sessionId = record.key() != null ? record.key() : "unknown";
            String userId = "";

            // Try to extract userId from the payload
            try {
                FullSessionContext ctx = mapper.readValue(record.value(), FullSessionContext.class);
                if (ctx.userId() != null) userId = ctx.userId();
            } catch (Exception ignored) {}

            ThinkResponse errorResponse = buildErrorResponse(sessionId, userId, e);

            String outputJson = mapper.writeValueAsString(errorResponse);
            producer.send(new ProducerRecord<>(AppConfig.OUTPUT_TOPIC, sessionId, outputJson));
            producer.flush();
            log.info("[{}] Emitted error response to {}", sessionId, AppConfig.OUTPUT_TOPIC);
        } catch (Exception ex) {
            log.error("Failed to emit error response: {}", ex.getMessage(), ex);
        }
    }

    /**
     * Builds a ThinkResponse representing an error — endTurn=true with a user-visible message.
     */
    static ThinkResponse buildErrorResponse(String sessionId, String userId, Exception e) {
        String errorMessage = "Sorry, an error occurred while processing your request: " + e.getMessage();
        return new ThinkResponse(
                sessionId, userId, null, null, 0, 0,
                null, null,
                List.of(new MessageInput(sessionId, userId, "assistant", errorMessage,
                        java.time.Instant.now().toString(), null)),
                null, true, java.time.Instant.now().toString());
    }

    /**
     * Finds the index in the history list where the split should happen for compaction.
     * Everything before this index will be summarized; everything from this index onward is kept.
     *
     * @param history  the full message history
     * @param keepLast number of user messages to keep (from the end)
     * @return the index of the first kept user message, or -1 if nothing to compact
     */
    static int findCompactionSplitIndex(List<MessageInput> history, int keepLast) {
        if (history == null || keepLast <= 0) return -1;

        // Count total user messages
        long totalUserMessages = history.stream()
                .filter(m -> "user".equals(m.role()))
                .count();

        if (totalUserMessages <= keepLast) return -1;

        // Find the index of the (totalUserMessages - keepLast + 1)th user message
        // i.e., the first user message we want to keep
        long target = totalUserMessages - keepLast;
        long seen = 0;
        for (int i = 0; i < history.size(); i++) {
            if ("user".equals(history.get(i).role())) {
                seen++;
                if (seen > target) {
                    return i; // this is the first user message to keep
                }
            }
        }
        return -1;
    }

    /**
     * Checks whether a message contains tool_use content blocks (structured content).
     */
    static boolean hasToolUseContent(MessageInput msg) {
        if (msg.content() instanceof List<?> blocks) {
            return blocks.stream().anyMatch(b ->
                    b instanceof Map<?, ?> m && "tool_use".equals(m.get("type")));
        }
        return false;
    }

    /**
     * Extracts plain text from a list of messages (used for compaction summary).
     */
    static String extractTextFromMessages(List<MessageInput> messages) {
        if (messages == null) return "";
        StringBuilder sb = new StringBuilder();
        for (MessageInput msg : messages) {
            if (msg.content() instanceof String text) {
                if (!sb.isEmpty()) sb.append("\n");
                sb.append(text);
            }
        }
        return sb.toString();
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
