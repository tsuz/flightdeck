package io.axon.think.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axon.think.config.AppConfig;
import io.axon.think.model.MessageInput;
import io.axon.think.model.ThinkResponse;
import io.axon.think.model.ToolUseItem;
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
public class ClaudeApiService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeApiService.class);

    private static final String ANTHROPIC_VERSION = "2023-06-01";

    // Sonnet 4 pricing per million tokens
    private static final double INPUT_PRICE_PER_M  = 3.0;
    private static final double OUTPUT_PRICE_PER_M = 15.0;

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

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("[{}] Claude API returned status {}: {}", sessionId, response.statusCode(), response.body());
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

        if (AppConfig.TOOLS_ENABLED) {
            List<Map<String, Object>> tools = ToolDefinitions.getTools();
            if (!tools.isEmpty()) {
                body.put("tools", tools);
            }
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

            // Calculate cost
            double cost = (inputTokens * INPUT_PRICE_PER_M / 1_000_000.0)
                        + (outputTokens * OUTPUT_PRICE_PER_M / 1_000_000.0);

            // Determine if this is an end turn
            String stopReason = root.path("stop_reason").asText("end_turn");
            boolean endTurn = !"tool_use".equals(stopReason);

            // Parse content blocks into messages and tool uses
            JsonNode contentBlocks = root.get("content");
            List<MessageInput> responseMessages = new ArrayList<>();
            List<ToolUseItem> toolUses = new ArrayList<>();

            if (contentBlocks != null && contentBlocks.isArray()) {
                for (JsonNode block : contentBlocks) {
                    String type = block.path("type").asText();

                    if ("text".equals(type)) {
                        String text = block.path("text").asText("");
                        responseMessages.add(new MessageInput(
                                sessionId, userId, "assistant", text,
                                Instant.now().toString(), null
                        ));
                    } else if ("tool_use".equals(type)) {
                        String toolUseId = block.path("id").asText();
                        String toolName = block.path("name").asText();
                        Map<String, Object> input = block.has("input")
                                ? mapper.convertValue(block.get("input"), Map.class)
                                : Map.of();

                        toolUses.add(new ToolUseItem(
                                toolUseId,
                                toolName,      // toolId = name for routing
                                toolName,
                                input,
                                sessionId,
                                Instant.now().toString()
                        ));
                    }
                }
            }

            log.info("[{}] Claude response: input_tokens={} output_tokens={} tool_uses={} end_turn={} cost=${}",
                    sessionId, inputTokens, outputTokens, toolUses.size(), endTurn,
                    String.format("%.6f", cost));

            return new ThinkResponse(
                    sessionId,
                    userId,
                    cost,
                    inputTokens,
                    outputTokens,
                    responseMessages,
                    toolUses.isEmpty() ? null : toolUses,
                    endTurn,
                    Instant.now().toString()
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Claude API response for session " + sessionId, e);
        }
    }

    /**
     * Converts internal MessageInput history into Claude API message format.
     * Groups consecutive same-role messages and handles tool-result messages.
     */
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
            // Tool results get special handling — wrap in tool_result content block
            Map<String, Object> toolResultBlock = new LinkedHashMap<>();
            toolResultBlock.put("type", "tool_result");

            // Extract tool_use_id from metadata if available
            if (msg.metadata() != null && msg.metadata().containsKey("tool_use_id")) {
                toolResultBlock.put("tool_use_id", msg.metadata().get("tool_use_id"));
            }
            toolResultBlock.put("content", msg.content());

            messages.add(Map.of("role", "user", "content", List.of(toolResultBlock)));
        } else {
            // Merge consecutive same-role messages to avoid API errors
            if (!messages.isEmpty()) {
                Map<String, Object> last = messages.get(messages.size() - 1);
                if (role.equals(last.get("role")) && last.get("content") instanceof String lastContent) {
                    last.put("content", lastContent + "\n\n" + msg.content());
                    return;
                }
            }
            messages.add(new LinkedHashMap<>(Map.of("role", role, "content", msg.content())));
        }
    }
}
