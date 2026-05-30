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
 * Enriches it into a full message-input record and produces to Kafka:
 *   { "session_id": "...", "user_id": "user_42", "role": "user",
 *     "content": "hello", "timestamp": "...",
 *     "metadata": { "locale": "en-US", "client": "web" } }
 *
 * <p>For multi-agent calls the request may also carry a transport-level
 * {@code reply} descriptor telling the pipeline where this session's terminal
 * response should be delivered, e.g.:
 * <pre>
 *   { "session_id": "...", "content": "...",
 *     "reply": { "type": "RESTAPI", "endpoint": "https://agent-a...",
 *                "method": "POST", "path": "/api/tools/response",
 *                "responseAsField": "result", "bearerToken": "&lt;HMAC&gt;" } }
 * </pre>
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
            String content = requireField(body, "content");

            // Transport-level reply routing (multi-agent). Written to the reply-to
            // topic keyed by session_id; never placed into the message content.
            if (body.hasNonNull("reply")) {
                JsonNode reply = body.get("reply");
                if (!reply.isObject()) {
                    throw new IllegalArgumentException("'reply' must be an object");
                }
                replyToProducer.send(sessionId, mapper.writeValueAsString(reply));
                log.info("[{}] Stored reply-to descriptor (type={})",
                        sessionId, reply.path("type").asText("?"));
            }

            // Build the full message-input payload
            ObjectNode message = mapper.createObjectNode();
            message.put("session_id", sessionId);
            message.put("user_id", body.has("user_id") ? body.get("user_id").asText() : "user_42");
            message.put("role", "user");
            message.put("content", content);
            message.put("timestamp", Instant.now().toString());

            ObjectNode metadata = mapper.createObjectNode();
            metadata.put("locale", "en-US");
            metadata.put("client", "web");
            message.set("metadata", metadata);

            String messageJson = mapper.writeValueAsString(message);

            producer.send(sessionId, messageJson);

            log.info("[{}] Produced message to message-input: {}", sessionId, truncate(content, 80));

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
