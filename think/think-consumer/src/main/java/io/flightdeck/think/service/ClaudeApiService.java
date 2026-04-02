package io.flightdeck.think.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flightdeck.think.config.AppConfig;
import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;
import io.flightdeck.think.model.ToolUseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Calls the Claude Messages API and translates the response into a {@link ThinkResponse}.
 */
public class ClaudeApiService implements LlmApiService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeApiService.class);

    private static final String ANTHROPIC_VERSION = "2023-06-01";

    // Token pricing from environment variables (per-token, not per-million)
    private static final Double INPUT_TOKEN_PRICE;
    private static final Double OUTPUT_TOKEN_PRICE;

    static {
        String inputPriceStr = System.getenv("INPUT_TOKEN_PRICE");
        String outputPriceStr = System.getenv("OUTPUT_TOKEN_PRICE");

        if (inputPriceStr == null || inputPriceStr.isBlank()
                || outputPriceStr == null || outputPriceStr.isBlank()) {
            log.warn("INPUT_TOKEN_PRICE and/or OUTPUT_TOKEN_PRICE not set — cost will not be calculated");
            INPUT_TOKEN_PRICE = null;
            OUTPUT_TOKEN_PRICE = null;
        } else {
            INPUT_TOKEN_PRICE = Double.parseDouble(inputPriceStr);
            OUTPUT_TOKEN_PRICE = Double.parseDouble(outputPriceStr);
            log.info("Token pricing loaded: INPUT_TOKEN_PRICE={} OUTPUT_TOKEN_PRICE={}", INPUT_TOKEN_PRICE, OUTPUT_TOKEN_PRICE);
        }
    }

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String apiUrl;
    private final String apiKey;
    private final String model;
    private final int maxTokens;

    public ClaudeApiService(ObjectMapper mapper) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.mapper = mapper;
        this.apiUrl = AppConfig.CLAUDE_API_URL;
        this.apiKey = AppConfig.CLAUDE_API_KEY;
        this.model = AppConfig.CLAUDE_MODEL;
        this.maxTokens = AppConfig.CLAUDE_MAX_TOKENS;
    }

    /**
     * Sends conversation history and tools to Claude and returns a ThinkResponse.
     *
     * @param systemPrompt  the system prompt (including RAG context)
     * @param messages       conversation messages in Claude API format
     * @param sessionId      session ID for the response
     * @param userId         user ID for the response
     * @return a ThinkResponse with messages, tool uses, cost, and token counts
     */
    public ThinkResponse call(String systemPrompt,
                              List<Map<String, Object>> messages,
                              String sessionId,
                              String userId) {

        try {
            Map<String, Object> requestBody = buildRequestBody(systemPrompt, messages);

            String body = mapper.writeValueAsString(requestBody);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .header("x-api-key", apiKey)
                    .header("anthropic-version", ANTHROPIC_VERSION)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(120))
                    .build();

            log.info("[{}] Calling Claude API: model={} messages={}", sessionId, model, messages.size());
            log.debug("[{}] Request body:\n{}", sessionId, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("[{}] Claude API returned status {}: {}", sessionId, response.statusCode(), response.body());
                log.error("[{}] Request messages:\n{}", sessionId, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(messages));
                throw new RuntimeException("Claude API error: HTTP " + response.statusCode());
            }

            return parseResponse(response.body(), sessionId, userId);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Claude API call failed for session " + sessionId, e);
        }
    }

    private Map<String, Object> buildRequestBody(String systemPrompt, List<Map<String, Object>> messages) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", model);
        body.put("max_tokens", maxTokens);
        body.put("system", systemPrompt);
        body.put("messages", messages);

        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        if (!tools.isEmpty()) {
            body.put("tools", tools);
        }

        return body;
    }

    /**
     * Parses the Claude API response JSON into a ThinkResponse.
     */
    ThinkResponse parseResponse(String responseBody, String sessionId, String userId) {
        try {
            JsonNode root = mapper.readTree(responseBody);

            // Extract token usage
            JsonNode usage = root.get("usage");
            int inputTokens = usage != null ? usage.path("input_tokens").asInt(0) : 0;
            int outputTokens = usage != null ? usage.path("output_tokens").asInt(0) : 0;

            // Calculate cost (null if pricing env vars not set)
            Double cost = (INPUT_TOKEN_PRICE != null && OUTPUT_TOKEN_PRICE != null)
                    ? (inputTokens / 1_000_000.0 * INPUT_TOKEN_PRICE) + (outputTokens / 1_000_000.0 * OUTPUT_TOKEN_PRICE)
                    : null;

            // Determine if this is an end turn
            String stopReason = root.path("stop_reason").asText("end_turn");
            boolean endTurn = !"tool_use".equals(stopReason);

            // Parse content blocks into messages and tool uses
            JsonNode contentBlocks = root.get("content");
            List<MessageInput> responseMessages = new ArrayList<>();
            List<ToolUseItem> toolUses = new ArrayList<>();

            // First pass: count tool_use blocks to set totalTools on each item
            int totalToolCount = 0;
            if (contentBlocks != null && contentBlocks.isArray()) {
                for (JsonNode block : contentBlocks) {
                    if ("tool_use".equals(block.path("type").asText())) {
                        totalToolCount++;
                    }
                }
            }

            if (contentBlocks != null && contentBlocks.isArray()) {
                // Build the full content blocks list for history preservation
                List<Map<String, Object>> fullContentBlocks = new ArrayList<>();

                for (JsonNode block : contentBlocks) {
                    String type = block.path("type").asText();

                    if ("text".equals(type)) {
                        String text = block.path("text").asText("");
                        fullContentBlocks.add(Map.of("type", "text", "text", text));
                    } else if ("tool_use".equals(type)) {
                        String toolUseId = block.path("id").asText();
                        String toolName = block.path("name").asText();
                        Map<String, Object> input = block.has("input")
                                ? mapper.convertValue(block.get("input"), Map.class)
                                : Map.of();

                        fullContentBlocks.add(new LinkedHashMap<>(Map.of(
                                "type", "tool_use",
                                "id", toolUseId,
                                "name", toolName,
                                "input", input
                        )));

                        toolUses.add(new ToolUseItem(
                                toolUseId,
                                toolName,      // toolId = name for routing
                                toolName,
                                input,
                                sessionId,
                                totalToolCount,
                                Instant.now().toString()
                        ));
                    }
                }

                // If there are tool_use blocks, store the full content blocks as structured
                // data so the tool_use IDs are preserved in history for the next API call.
                if (!toolUses.isEmpty()) {
                    responseMessages.add(new MessageInput(
                            sessionId, userId, "assistant", fullContentBlocks,
                            Instant.now().toString(), null
                    ));
                } else {
                    // Text-only response — store as plain strings
                    for (Map<String, Object> cb : fullContentBlocks) {
                        if ("text".equals(cb.get("type"))) {
                            responseMessages.add(new MessageInput(
                                    sessionId, userId, "assistant", cb.get("text"),
                                    Instant.now().toString(), null
                            ));
                        }
                    }
                }
            }

            log.info("[{}] Claude response: input_tokens={} output_tokens={} tool_uses={} end_turn={} cost={}",
                    sessionId, inputTokens, outputTokens, toolUses.size(), endTurn,
                    cost != null ? String.format("$%.6f", cost) : "null");

            return new ThinkResponse(
                    sessionId,
                    userId,
                    cost,
                    null,   // prevSessionCost — set by ThinkConsumer
                    inputTokens,
                    outputTokens,
                    responseMessages,
                    toolUses.isEmpty() ? null : toolUses,
                    endTurn,
                    Instant.now().toString(),
                    null    // compactedHistory — set by ThinkConsumer when compaction triggers
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Claude API response for session " + sessionId, e);
        }
    }

    /**
     * Converts internal MessageInput history into Claude API message format.
     * Groups consecutive same-role messages and handles tool-result messages.
     */
    @Override
    public List<Map<String, Object>> toApiMessages(List<MessageInput> history, MessageInput latestInput) {
        return toClaudeMessages(history, latestInput);
    }

    public static List<Map<String, Object>> toClaudeMessages(List<MessageInput> history, MessageInput latestInput) {
        List<Map<String, Object>> messages = new ArrayList<>();

        // Add history
        if (history != null) {
            for (MessageInput msg : history) {
                addMessage(messages, msg);
            }
        }

        // Add latest input
        if (latestInput != null) {
            addMessage(messages, latestInput);
        }

        return messages;
    }

    private static void addMessage(List<Map<String, Object>> messages, MessageInput msg) {
        if (msg == null || msg.content() == null) return;

        String role = msg.role();
        if (role == null) role = "user";

        // Claude API uses "user" and "assistant" roles.
        // Tool results are sent as "user" role with tool_result content blocks.
        if ("tool".equals(role)) {
            List<Map<String, Object>> toolResultBlocks = buildToolResultBlocks(msg);
            if (!toolResultBlocks.isEmpty()) {
                messages.add(Map.of("role", "user", "content", toolResultBlocks));
            }
        } else if (msg.content() instanceof List<?>) {
            // Structured content blocks (e.g. assistant message with text + tool_use)
            // Pass through as-is to preserve tool_use IDs for the Claude API
            messages.add(new LinkedHashMap<>(Map.of("role", role, "content", msg.content())));
        } else {
            // Plain text content — merge consecutive same-role messages to avoid API errors
            if (!messages.isEmpty()) {
                Map<String, Object> last = messages.get(messages.size() - 1);
                if (role.equals(last.get("role")) && last.get("content") instanceof String lastContent) {
                    last.put("content", lastContent + "\n\n" + msg.contentAsString());
                    return;
                }
            }
            messages.add(new LinkedHashMap<>(Map.of("role", role, "content", msg.contentAsString())));
        }
    }

    /**
     * Builds Claude API tool_result content blocks from a tool-role MessageInput.
     * Each tool result in the content list becomes a separate tool_result block
     * with the matching tool_use_id, as required by the Claude Messages API.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buildToolResultBlocks(MessageInput msg) {
        List<Map<String, Object>> blocks = new ArrayList<>();
        Object content = msg.content();

        if (content instanceof List<?> resultList) {
            // Content is a list of tool result maps from TransformToolUseDoneProcessor
            for (Object entry : resultList) {
                if (entry instanceof Map<?, ?> resultMap) {
                    Map<String, Object> block = new LinkedHashMap<>();
                    block.put("type", "tool_result");
                    block.put("tool_use_id", resultMap.get("tool_use_id"));

                    // Serialize the result data as JSON string for the content field
                    Object result = resultMap.get("result");
                    if (result != null) {
                        try {
                            block.put("content", new ObjectMapper().writeValueAsString(result));
                        } catch (Exception e) {
                            block.put("content", result.toString());
                        }
                    } else {
                        block.put("content", "{}");
                    }

                    blocks.add(block);
                }
            }
        } else {
            // Fallback: content is a plain string — use tool_use_id from metadata if available
            Map<String, Object> block = new LinkedHashMap<>();
            block.put("type", "tool_result");
            if (msg.metadata() != null && msg.metadata().containsKey("tool_use_id")) {
                block.put("tool_use_id", msg.metadata().get("tool_use_id"));
            }
            block.put("content", msg.contentAsString());
            blocks.add(block);
        }

        return blocks;
    }
}
