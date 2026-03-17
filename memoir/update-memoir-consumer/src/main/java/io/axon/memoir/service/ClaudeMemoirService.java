package io.axon.memoir.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axon.memoir.config.AppConfig;
import io.axon.memoir.model.MemoirSessionEnd;
import io.axon.memoir.model.MessageInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Calls Claude Sonnet to update a session's memoir based on the previous
 * memoir context and the last conversation exchange.
 */
public class ClaudeMemoirService {

    private static final Logger log = LoggerFactory.getLogger(ClaudeMemoirService.class);

    private static final String ANTHROPIC_VERSION = "2023-06-01";

    private static final String SYSTEM_PROMPT = """
            You are a memoir updater. You maintain a minimal key-fact summary of a user \
            across sessions.

            You will receive:
            1. The PREVIOUS MEMOIR (may be empty if this is the first session)
            2. The LATEST CONVERSATION from the session that just ended

            Rules:
            - Extract ONLY concrete, reusable facts: name, age, nationality, role, \
              preferences, decisions, action items, unresolved questions
            - Do NOT record what the assistant said or offered
            - Do NOT record session metadata ("first interaction", "brief exchange", etc.)
            - Do NOT pad with filler when there is little information — a 1-line memoir \
              is perfectly fine
            - If the conversation contains no new key facts, return the previous memoir unchanged
            - Keep the memoir under 1000 tokens — bullet points, no prose
            - Write in the same language the user used

            Output ONLY the updated memoir text. No preamble, no explanation.""";

    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public ClaudeMemoirService(ObjectMapper mapper) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.mapper = mapper;
    }

    /**
     * Calls Claude Sonnet to produce an updated memoir from the previous
     * memoir and the last session's conversation.
     *
     * @return the updated memoir text
     */
    public String updateMemoir(String sessionId, MemoirSessionEnd snapshot) {
        try {
            String userMessage = buildUserMessage(snapshot);

            Map<String, Object> requestBody = new LinkedHashMap<>();
            requestBody.put("model", AppConfig.CLAUDE_MODEL);
            requestBody.put("max_tokens", AppConfig.CLAUDE_MAX_TOKENS);
            requestBody.put("system", SYSTEM_PROMPT);
            requestBody.put("messages", List.of(
                    Map.of("role", "user", "content", userMessage)
            ));

            String body = mapper.writeValueAsString(requestBody);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(AppConfig.CLAUDE_API_URL))
                    .header("Content-Type", "application/json")
                    .header("x-api-key", AppConfig.CLAUDE_API_KEY)
                    .header("anthropic-version", ANTHROPIC_VERSION)
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(120))
                    .build();

            log.info("[{}] Calling Claude API for memoir update: model={}", sessionId, AppConfig.CLAUDE_MODEL);
            log.debug("[{}] Request body:\n{}", sessionId, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("[{}] Claude API returned status {}: {}", sessionId, response.statusCode(), response.body());
                throw new RuntimeException("Claude API error: HTTP " + response.statusCode());
            }

            String updatedMemoir = extractText(response.body());

            log.info("[{}] Memoir updated successfully ({} chars)", sessionId, updatedMemoir.length());
            log.debug("[{}] Updated memoir:\n{}", sessionId, updatedMemoir);

            return updatedMemoir;

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Memoir update failed for session " + sessionId, e);
        }
    }

    /**
     * Builds the user message containing the previous memoir and latest conversation.
     */
    String buildUserMessage(MemoirSessionEnd snapshot) {
        StringBuilder sb = new StringBuilder();

        // Previous memoir
        sb.append("## PREVIOUS MEMOIR\n\n");
        if (snapshot.memoirContext() != null && !snapshot.memoirContext().isBlank()) {
            sb.append(snapshot.memoirContext());
        } else {
            sb.append("(No previous memoir — this is the first session)");
        }
        sb.append("\n\n");

        // Latest conversation
        sb.append("## LATEST CONVERSATION\n\n");
        if (snapshot.thinkResponse() != null && snapshot.thinkResponse().messages() != null) {
            for (MessageInput msg : snapshot.thinkResponse().messages()) {
                String role = msg.role() != null ? msg.role() : "unknown";
                String content = msg.contentAsString();
                if (content != null && !content.isBlank()) {
                    sb.append("[").append(role).append("]: ").append(content).append("\n\n");
                }
            }
        } else {
            sb.append("(No conversation messages available)");
        }

        return sb.toString();
    }

    /**
     * Extracts the text content from a Claude API response.
     */
    private String extractText(String responseBody) {
        try {
            JsonNode root = mapper.readTree(responseBody);
            JsonNode content = root.get("content");

            if (content != null && content.isArray()) {
                StringBuilder text = new StringBuilder();
                for (JsonNode block : content) {
                    if ("text".equals(block.path("type").asText())) {
                        if (!text.isEmpty()) text.append("\n\n");
                        text.append(block.path("text").asText(""));
                    }
                }
                return text.toString();
            }

            return "";
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Claude API response", e);
        }
    }
}
