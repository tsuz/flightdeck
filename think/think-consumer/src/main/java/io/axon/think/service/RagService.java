package io.axon.think.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axon.think.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Retrieves relevant context from a RAG (Retrieval-Augmented Generation) service.
 * <p>
 * Sends the user's latest query to the RAG endpoint and returns matching
 * document chunks that should be injected into the Claude system prompt.
 */
public class RagService {

    private static final Logger log = LoggerFactory.getLogger(RagService.class);

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String ragUrl;
    private final int topK;

    public RagService(ObjectMapper mapper) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = mapper;
        this.ragUrl = AppConfig.RAG_API_URL;
        this.topK = AppConfig.RAG_TOP_K;
    }

    /**
     * Queries the RAG service for documents relevant to the given query.
     *
     * @param query     the user's latest message content
     * @param sessionId the session ID for logging
     * @return a list of relevant document chunks, or empty list on failure
     */
    public List<String> retrieveContext(String query, String sessionId) {
        try {
            Map<String, Object> requestBody = Map.of(
                    "query", query,
                    "top_k", topK,
                    "session_id", sessionId
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ragUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(requestBody)))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            log.info("[{}] Querying RAG service: query_len={}", sessionId, query.length());

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.warn("[{}] RAG service returned status {}", sessionId, response.statusCode());
                return List.of();
            }

            return parseRagResponse(response.body(), sessionId);

        } catch (Exception e) {
            log.warn("[{}] RAG service call failed — proceeding without context: {}", sessionId, e.getMessage());
            return List.of();
        }
    }

    private List<String> parseRagResponse(String responseBody, String sessionId) {
        try {
            JsonNode root = mapper.readTree(responseBody);
            JsonNode documents = root.has("documents") ? root.get("documents") : root.get("results");

            if (documents == null || !documents.isArray()) {
                log.warn("[{}] RAG response has no documents array", sessionId);
                return List.of();
            }

            List<String> chunks = new ArrayList<>();
            for (JsonNode doc : documents) {
                String content = doc.has("content") ? doc.get("content").asText()
                        : doc.has("text") ? doc.get("text").asText()
                        : doc.toString();
                if (content != null && !content.isBlank()) {
                    chunks.add(content);
                }
            }

            log.info("[{}] RAG returned {} relevant chunks", sessionId, chunks.size());
            return chunks;

        } catch (Exception e) {
            log.warn("[{}] Failed to parse RAG response: {}", sessionId, e.getMessage());
            return List.of();
        }
    }
}
