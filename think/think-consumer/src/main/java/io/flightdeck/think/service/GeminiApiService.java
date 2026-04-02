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
 * Calls the Gemini generateContent API and translates the response into a {@link ThinkResponse}.
 */
public class GeminiApiService implements LlmApiService {

    private static final Logger log = LoggerFactory.getLogger(GeminiApiService.class);

    private static final Double INPUT_TOKEN_PRICE;
    private static final Double OUTPUT_TOKEN_PRICE;

    static {
        String inputPriceStr = System.getenv("INPUT_TOKEN_PRICE");
        String outputPriceStr = System.getenv("OUTPUT_TOKEN_PRICE");

        if (inputPriceStr == null || inputPriceStr.isBlank()
                || outputPriceStr == null || outputPriceStr.isBlank()) {
            INPUT_TOKEN_PRICE = null;
            OUTPUT_TOKEN_PRICE = null;
        } else {
            INPUT_TOKEN_PRICE = Double.parseDouble(inputPriceStr);
            OUTPUT_TOKEN_PRICE = Double.parseDouble(outputPriceStr);
        }
    }

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String apiBaseUrl;
    private final String apiKey;
    private final String model;
    private final int maxTokens;

    public GeminiApiService(ObjectMapper mapper) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.mapper = mapper;
        this.apiBaseUrl = AppConfig.GEMINI_API_URL;
        this.apiKey = AppConfig.GEMINI_API_KEY;
        this.model = AppConfig.GEMINI_MODEL;
        this.maxTokens = AppConfig.GEMINI_MAX_TOKENS;
    }

    @Override
    public ThinkResponse call(String systemPrompt,
                              List<Map<String, Object>> messages,
                              String sessionId,
                              String userId) {
        try {
            Map<String, Object> requestBody = buildRequestBody(systemPrompt, messages);
            String body = mapper.writeValueAsString(requestBody);

            String url = String.format("%s/models/%s:generateContent?key=%s",
                    apiBaseUrl, model, apiKey);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(120))
                    .build();

            log.info("[{}] Calling Gemini API: model={} messages={}", sessionId, model, messages.size());
            log.debug("[{}] Request body:\n{}", sessionId, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("[{}] Gemini API returned status {}: {}", sessionId, response.statusCode(), response.body());
                throw new RuntimeException("Gemini API error: HTTP " + response.statusCode());
            }

            return parseResponse(response.body(), sessionId, userId);

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Gemini API call failed for session " + sessionId, e);
        }
    }

    private Map<String, Object> buildRequestBody(String systemPrompt, List<Map<String, Object>> contents) {
        Map<String, Object> body = new LinkedHashMap<>();

        // System instruction
        body.put("system_instruction", Map.of(
                "parts", List.of(Map.of("text", systemPrompt))
        ));

        body.put("contents", contents);

        // Convert tools from Claude format to Gemini format
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        if (!tools.isEmpty()) {
            List<Map<String, Object>> functionDeclarations = new ArrayList<>();
            for (Map<String, Object> tool : tools) {
                Map<String, Object> funcDecl = new LinkedHashMap<>();
                funcDecl.put("name", tool.get("name"));
                funcDecl.put("description", tool.get("description"));
                if (tool.get("input_schema") != null) {
                    funcDecl.put("parameters", tool.get("input_schema"));
                }
                functionDeclarations.add(funcDecl);
            }
            body.put("tools", List.of(Map.of("function_declarations", functionDeclarations)));
        }

        body.put("generationConfig", Map.of("maxOutputTokens", maxTokens));

        return body;
    }

    ThinkResponse parseResponse(String responseBody, String sessionId, String userId) {
        try {
            JsonNode root = mapper.readTree(responseBody);

            // Extract token usage
            JsonNode usageMeta = root.get("usageMetadata");
            int inputTokens = usageMeta != null ? usageMeta.path("promptTokenCount").asInt(0) : 0;
            int outputTokens = usageMeta != null ? usageMeta.path("candidatesTokenCount").asInt(0) : 0;

            Double cost = (INPUT_TOKEN_PRICE != null && OUTPUT_TOKEN_PRICE != null)
                    ? (inputTokens / 1_000_000.0 * INPUT_TOKEN_PRICE) + (outputTokens / 1_000_000.0 * OUTPUT_TOKEN_PRICE)
                    : null;

            // Get first candidate
            JsonNode candidates = root.get("candidates");
            if (candidates == null || !candidates.isArray() || candidates.isEmpty()) {
                throw new RuntimeException("No candidates in Gemini response");
            }
            JsonNode candidate = candidates.get(0);

            // Determine end turn from finishReason and presence of function calls
            String finishReason = candidate.path("finishReason").asText("STOP");
            JsonNode parts = candidate.path("content").path("parts");

            boolean hasFunctionCall = false;
            if (parts.isArray()) {
                for (JsonNode part : parts) {
                    if (part.has("functionCall")) {
                        hasFunctionCall = true;
                        break;
                    }
                }
            }
            boolean endTurn = !hasFunctionCall;

            // Parse parts into messages and tool uses
            List<MessageInput> responseMessages = new ArrayList<>();
            List<ToolUseItem> toolUses = new ArrayList<>();

            int totalToolCount = 0;
            if (parts.isArray()) {
                for (JsonNode part : parts) {
                    if (part.has("functionCall")) totalToolCount++;
                }
            }

            if (parts.isArray()) {
                // Build content blocks in Claude-compatible format for history preservation
                List<Map<String, Object>> fullContentBlocks = new ArrayList<>();

                for (JsonNode part : parts) {
                    if (part.has("text")) {
                        String text = part.path("text").asText("");
                        fullContentBlocks.add(Map.of("type", "text", "text", text));
                    } else if (part.has("functionCall")) {
                        JsonNode fc = part.get("functionCall");
                        String toolName = fc.path("name").asText();
                        Map<String, Object> args = fc.has("args")
                                ? mapper.convertValue(fc.get("args"), Map.class)
                                : Map.of();

                        // Generate a synthetic tool_use_id for downstream compatibility
                        String toolUseId = "toolu_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);

                        fullContentBlocks.add(new LinkedHashMap<>(Map.of(
                                "type", "tool_use",
                                "id", toolUseId,
                                "name", toolName,
                                "input", args
                        )));

                        toolUses.add(new ToolUseItem(
                                toolUseId,
                                toolName,
                                toolName,
                                args,
                                sessionId,
                                totalToolCount,
                                Instant.now().toString()
                        ));
                    }
                }

                if (!toolUses.isEmpty()) {
                    responseMessages.add(new MessageInput(
                            sessionId, userId, "assistant", fullContentBlocks,
                            Instant.now().toString(), null
                    ));
                } else {
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

            log.info("[{}] Gemini response: input_tokens={} output_tokens={} tool_uses={} end_turn={} cost={}",
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
            throw new RuntimeException("Failed to parse Gemini API response for session " + sessionId, e);
        }
    }

    /**
     * Converts internal MessageInput history into Gemini API contents format.
     * Maps assistant→model, handles functionCall/functionResponse parts.
     */
    @Override
    public List<Map<String, Object>> toApiMessages(List<MessageInput> history, MessageInput latestInput) {
        // First pass: build tool_use_id → name mapping from assistant messages
        Map<String, String> toolIdToName = buildToolIdToNameMap(history);

        List<Map<String, Object>> contents = new ArrayList<>();

        if (history != null) {
            for (MessageInput msg : history) {
                addGeminiMessage(contents, msg, toolIdToName);
            }
        }

        if (latestInput != null) {
            addGeminiMessage(contents, latestInput, toolIdToName);
        }

        return contents;
    }

    private void addGeminiMessage(List<Map<String, Object>> contents, MessageInput msg,
                                  Map<String, String> toolIdToName) {
        if (msg == null || msg.content() == null) return;

        String role = msg.role();
        if (role == null) role = "user";

        if ("tool".equals(role)) {
            // Convert tool results to functionResponse parts
            List<Map<String, Object>> parts = buildFunctionResponseParts(msg, toolIdToName);
            if (!parts.isEmpty()) {
                contents.add(Map.of("role", "user", "parts", parts));
            }
        } else if ("assistant".equals(role)) {
            String geminiRole = "model";

            if (msg.content() instanceof List<?> contentBlocks) {
                // Structured content blocks — convert tool_use to functionCall
                List<Map<String, Object>> parts = new ArrayList<>();
                for (Object block : contentBlocks) {
                    if (block instanceof Map<?, ?> blockMap) {
                        String type = (String) blockMap.get("type");
                        if ("text".equals(type)) {
                            String text = (String) blockMap.get("text");
                            if (text != null && !text.isEmpty()) {
                                parts.add(Map.of("text", text));
                            }
                        } else if ("tool_use".equals(type)) {
                            Map<String, Object> functionCall = new LinkedHashMap<>();
                            functionCall.put("name", blockMap.get("name"));
                            functionCall.put("args", blockMap.get("input"));
                            parts.add(Map.of("functionCall", functionCall));
                        }
                    }
                }
                if (!parts.isEmpty()) {
                    contents.add(Map.of("role", geminiRole, "parts", parts));
                }
            } else {
                // Plain text
                appendOrMergeGemini(contents, geminiRole, msg.contentAsString());
            }
        } else {
            // user role
            appendOrMergeGemini(contents, "user", msg.contentAsString());
        }
    }

    private void appendOrMergeGemini(List<Map<String, Object>> contents, String role, String text) {
        if (!contents.isEmpty()) {
            Map<String, Object> last = contents.get(contents.size() - 1);
            if (role.equals(last.get("role"))) {
                Object partsObj = last.get("parts");
                if (partsObj instanceof List<?> partsList && !partsList.isEmpty()) {
                    Object lastPart = partsList.get(partsList.size() - 1);
                    if (lastPart instanceof Map<?, ?> lastPartMap && lastPartMap.containsKey("text")) {
                        // Merge by adding a new text part
                        List<Map<String, Object>> newParts = new ArrayList<>((List<Map<String, Object>>) partsObj);
                        newParts.add(Map.of("text", text));
                        contents.set(contents.size() - 1, Map.of("role", role, "parts", newParts));
                        return;
                    }
                }
            }
        }
        contents.add(new LinkedHashMap<>(Map.of("role", role, "parts", List.of(Map.of("text", text)))));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> buildFunctionResponseParts(MessageInput msg,
                                                                  Map<String, String> toolIdToName) {
        List<Map<String, Object>> parts = new ArrayList<>();
        Object content = msg.content();

        if (content instanceof List<?> resultList) {
            for (Object entry : resultList) {
                if (entry instanceof Map<?, ?> resultMap) {
                    String toolUseId = (String) resultMap.get("tool_use_id");
                    String name = toolIdToName.getOrDefault(toolUseId, "unknown");

                    Object result = resultMap.get("result");
                    Map<String, Object> responseData;
                    if (result instanceof Map) {
                        responseData = (Map<String, Object>) result;
                    } else {
                        responseData = Map.of("result", result != null ? result.toString() : "{}");
                    }

                    parts.add(Map.of("functionResponse", Map.of(
                            "name", name,
                            "response", responseData
                    )));
                }
            }
        } else {
            // Fallback: plain string result
            String name = "unknown";
            if (msg.metadata() != null && msg.metadata().containsKey("tool_use_id")) {
                name = toolIdToName.getOrDefault(msg.metadata().get("tool_use_id"), "unknown");
            }
            parts.add(Map.of("functionResponse", Map.of(
                    "name", name,
                    "response", Map.of("result", msg.contentAsString())
            )));
        }

        return parts;
    }

    /**
     * Scans history for assistant messages with tool_use blocks to build
     * a mapping from tool_use_id to tool name.
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> buildToolIdToNameMap(List<MessageInput> history) {
        Map<String, String> map = new HashMap<>();
        if (history == null) return map;

        for (MessageInput msg : history) {
            if ("assistant".equals(msg.role()) && msg.content() instanceof List<?> blocks) {
                for (Object block : blocks) {
                    if (block instanceof Map<?, ?> blockMap) {
                        if ("tool_use".equals(blockMap.get("type"))) {
                            String id = (String) blockMap.get("id");
                            String name = (String) blockMap.get("name");
                            if (id != null && name != null) {
                                map.put(id, name);
                            }
                        }
                    }
                }
            }
        }
        return map;
    }
}
