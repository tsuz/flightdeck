package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Handles POST /api/chat
 *
 * Accepts a minimal payload:
 *   { "session_id": "session-20260315-2250", "content": "hello" }
 *
 * <p>{@code content} may also be an array of content blocks so a caller can
 * attach images by reference (the think consumer passes block lists straight
 * through to the LLM provider):
 * <pre>
 *   { "session_id": "...", "content": [
 *       { "type": "image", "source": { "type": "url", "url": "https://..." } },
 *       { "type": "text",  "text": "this is what I see" } ] }
 * </pre>
 * Only {@code text} and {@code image} blocks are accepted, and an image source
 * must be a {@code url} (https) or a Files API {@code file} id — never inline
 * base64, since the full session history is replayed inside every downstream
 * Kafka record. No size limit is enforced here: the Kafka record limit and the
 * model's own image limits are the guard.
 *
 * Enriches it into a full message-input record and produces to Kafka:
 *   { "session_id": "...", "user_id": "user_42", "role": "user",
 *     "content": "hello", "timestamp": "...",
 *     "metadata": { "locale": "en-US", "client": "web" } }
 *
 * <p>For multi-agent calls the request may also carry a transport-level
 * {@code reply} descriptor naming where this session's terminal response should
 * be delivered, e.g.:
 * <pre>
 *   { "session_id": "...", "content": "...",
 *     "reply": { "callbackService": "my-agent-a", "bearerToken": "&lt;HMAC&gt;" } }
 * </pre>
 * {@code callbackService} is a logical name resolved server-side against
 * {@code ALLOWED_HOST_MAPPING} ({@link CallbackRegistry}); the caller never
 * supplies a URL, so the descriptor cannot steer the callback at an arbitrary
 * host. Unknown names are rejected here with a 400.
 * The {@code reply} object is NOT placed into the message content/metadata — it
 * is written to the reply-to topic (keyed by session_id) so it never reaches the
 * LLM. The agent processes the request as an ordinary chat.
 */
public class ChatHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaMessageProducer producer;
    private final ReplyToProducer replyToProducer;

    public ChatHandler(KafkaMessageProducer producer, ReplyToProducer replyToProducer) {
        this.producer = producer;
        this.replyToProducer = replyToProducer;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // CORS preflight
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        try (InputStream is = exchange.getRequestBody()) {
            JsonNode body = mapper.readTree(is);

            String sessionId = requireField(body, "session_id");
            JsonNode content = parseContent(body);

            // Transport-level reply routing (multi-agent). Written to the reply-to
            // topic keyed by session_id; never placed into the message content.
            if (body.hasNonNull("reply")) {
                JsonNode reply = body.get("reply");
                if (!reply.isObject()) {
                    throw new IllegalArgumentException("'reply' must be an object");
                }
                String callbackService = reply.path("callbackService").asText("");
                if (callbackService.isBlank()) {
                    throw new IllegalArgumentException("'reply' must include 'callbackService'");
                }
                // Fail closed: reject a descriptor naming a service this agent is not
                // configured to call back, so unroutable routes never reach the
                // reply-to topic and the caller gets an immediate 400.
                if (!CallbackRegistry.isKnown(callbackService)) {
                    throw new IllegalArgumentException("unknown callbackService: " + callbackService);
                }
                replyToProducer.send(sessionId, mapper.writeValueAsString(reply));
                log.info("[{}] Stored reply-to descriptor (callbackService={})",
                        sessionId, callbackService);
            }

            // Build the full message-input payload
            ObjectNode message = mapper.createObjectNode();
            message.put("session_id", sessionId);
            message.put("user_id", body.has("user_id") ? body.get("user_id").asText() : "user_42");
            message.put("role", "user");
            message.set("content", content);
            message.put("timestamp", Instant.now().toString());

            ObjectNode metadata = mapper.createObjectNode();
            metadata.put("locale", "en-US");
            metadata.put("client", "web");
            message.set("metadata", metadata);

            String messageJson = mapper.writeValueAsString(message);

            producer.send(sessionId, messageJson);

            log.info("[{}] Produced message to message-input: {}", sessionId,
                    truncate(summarizeContent(content), 80));

            ObjectNode response = mapper.createObjectNode();
            response.put("status", "ok");
            response.put("session_id", sessionId);
            sendJson(exchange, 200, mapper.writeValueAsString(response));

        } catch (IllegalArgumentException e) {
            sendJson(exchange, 400, "{\"error\":\"" + e.getMessage() + "\"}");
        } catch (Exception e) {
            log.error("Failed to handle chat request", e);
            sendJson(exchange, 500, "{\"error\":\"Internal server error\"}");
        }
    }

    /**
     * Validates and returns the {@code content} field: a non-blank string, or a
     * non-empty array of {@code text} / {@code image} blocks. Throws
     * {@link IllegalArgumentException} (mapped to HTTP 400) on anything else.
     */
    static JsonNode parseContent(JsonNode body) {
        if (body == null || !body.hasNonNull("content")) {
            throw new IllegalArgumentException("Missing required field: content");
        }
        JsonNode content = body.get("content");
        if (content.isTextual()) {
            if (content.asText().isBlank()) {
                throw new IllegalArgumentException("Missing required field: content");
            }
            return content;
        }
        if (!content.isArray()) {
            throw new IllegalArgumentException("'content' must be a string or an array of content blocks");
        }
        if (content.isEmpty()) {
            throw new IllegalArgumentException("'content' must not be empty");
        }
        for (JsonNode block : content) {
            if (!block.isObject()) {
                throw new IllegalArgumentException("content blocks must be objects");
            }
            String type = block.path("type").asText("");
            switch (type) {
                case "text" -> {
                    if (!block.path("text").isTextual() || block.get("text").asText().isBlank()) {
                        throw new IllegalArgumentException("text block requires a non-empty 'text'");
                    }
                }
                case "image" -> {
                    JsonNode source = block.path("source");
                    if (!source.isObject()) {
                        throw new IllegalArgumentException("image block requires a 'source' object");
                    }
                    String sourceType = source.path("type").asText("");
                    if ("url".equals(sourceType)) {
                        String url = source.path("url").asText("");
                        if (!url.startsWith("https://")) {
                            throw new IllegalArgumentException("image url must be an https URL");
                        }
                    } else if ("file".equals(sourceType)) {
                        if (source.path("file_id").asText("").isBlank()) {
                            throw new IllegalArgumentException("image file source requires 'file_id'");
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "image source type must be 'url' or 'file' (inline base64 is not accepted)");
                    }
                }
                default -> throw new IllegalArgumentException("unsupported content block type: '" + type + "'");
            }
        }
        return content;
    }

    /** One-line text rendering of content for logs: text blocks verbatim, images as a marker. */
    static String summarizeContent(JsonNode content) {
        if (content == null) return "";
        if (content.isTextual()) return content.asText();
        StringBuilder sb = new StringBuilder();
        for (JsonNode block : content) {
            if (sb.length() > 0) sb.append(' ');
            if ("image".equals(block.path("type").asText())) {
                sb.append("[image]");
            } else {
                sb.append(block.path("text").asText(""));
            }
        }
        return sb.toString();
    }

    private static String requireField(JsonNode body, String field) {
        if (body == null || !body.has(field) || body.get(field).asText().isBlank()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return body.get(field).asText();
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
