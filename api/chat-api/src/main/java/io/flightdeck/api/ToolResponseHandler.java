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
 * Handles {@code POST /api/tools/response} — the callback endpoint for
 * asynchronous tools, including sub-agent (multi-agent) replies.
 *
 * <p>When a tool consumer dispatches an async tool it acks the {@code tool-use}
 * message immediately (via {@code ctx.pending()}) and hands the external system
 * a signed callback token. When the external work finishes, the external system
 * calls this endpoint with the token as a bearer credential and the result in
 * the body:
 *
 * <pre>
 *   POST /api/tools/response
 *   Authorization: Bearer &lt;signed token&gt;
 *   { "result": &lt;any JSON&gt; }
 *   → 202 Accepted
 * </pre>
 *
 * <p>The token carries session_id, tool_use_id, tool_id, name and total_tools,
 * so the body only needs the result payload. The handler verifies the token and
 * writes a complete {@code ToolUseResult} into {@code tool-use-result} — the
 * exact same topic and shape a synchronous tool would have produced. The
 * aggregator cannot tell the two apart and dedupes by tool_use_id, so duplicate
 * callbacks are safe.
 *
 * <p>This is a server-to-server endpoint authenticated by the bearer token, so
 * no CORS headers are emitted.
 */
public class ToolResponseHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(ToolResponseHandler.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String BEARER_PREFIX = "Bearer ";

    private final ToolResultProducer producer;
    private final String callbackSecret;

    public ToolResponseHandler(ToolResultProducer producer, String callbackSecret) {
        this.producer = producer;
        this.callbackSecret = callbackSecret;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        try (InputStream is = exchange.getRequestBody()) {
            // Auth: HMAC callback token carried as `Authorization: Bearer <token>`.
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            String token = (authHeader != null && authHeader.startsWith(BEARER_PREFIX))
                    ? authHeader.substring(BEARER_PREFIX.length()).trim()
                    : null;

            ToolCallbackToken.Payload payload;
            try {
                payload = ToolCallbackToken.verify(token, callbackSecret);
            } catch (ToolCallbackToken.InvalidTokenException e) {
                log.warn("Rejected tool callback: {}", e.getMessage());
                sendJson(exchange, 401, "{\"error\":\"" + e.getMessage() + "\"}");
                return;
            }

            JsonNode body = mapper.readTree(is);

            // The body IS the tool result payload (e.g. { "result": "<text>" }).
            // ToolUseResult.result is an object, so use the body directly when it
            // is one and wrap primitives/arrays under a "result" key otherwise.
            ObjectNode resultObj;
            if (body != null && body.isObject()) {
                resultObj = (ObjectNode) body;
            } else {
                resultObj = mapper.createObjectNode();
                if (body != null && !body.isNull()) {
                    resultObj.set("result", body);
                }
            }

            // Latency measured from when the token was minted (iat, epoch seconds).
            long latencyMs = payload.iat() > 0
                    ? Math.max(0, System.currentTimeMillis() - payload.iat() * 1000L)
                    : 0L;

            ObjectNode result = mapper.createObjectNode();
            result.put("session_id", payload.sessionId());
            result.put("tool_use_id", payload.toolUseId());
            if (payload.toolId() != null) {
                result.put("tool_id", payload.toolId());
            }
            result.put("name", payload.name());
            result.set("result", resultObj);
            result.put("latency_ms", latencyMs);
            result.put("status", "success");
            result.put("total_tools", payload.totalTools());
            result.put("timestamp", Instant.now().toString());

            producer.send(payload.sessionId(), mapper.writeValueAsString(result));

            log.info("[{}] Async tool result accepted via callback — tool_use_id={} tool_id={}",
                    payload.sessionId(), payload.toolUseId(), payload.toolId());

            sendJson(exchange, 202,
                    "{\"status\":\"accepted\",\"tool_use_id\":\"" + payload.toolUseId() + "\"}");

        } catch (Exception e) {
            log.error("Failed to handle tool callback", e);
            sendJson(exchange, 500, "{\"error\":\"Internal server error\"}");
        }
    }

    private static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
