package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer that reads {@code UserResponse} records from the message-output
 * topic and delivers each one to its destination:
 *
 * <ul>
 *   <li><b>WebSocket</b> — the default. Forwarded to connected browser clients
 *       via {@link ChatWebSocketServer}.</li>
 *   <li><b>HTTP callback</b> — when the record carries a {@code reply_to}
 *       descriptor (a multi-agent call). The response is POSTed back to the
 *       calling agent's endpoint with the HMAC token as a bearer credential.
 *       On success the reply-to route is tombstoned so the one-shot call cannot
 *       be double-delivered.</li>
 * </ul>
 *
 * <h3>Retry policy for HTTP callbacks</h3>
 * Retriable failures (connection errors, timeouts, HTTP 5xx/408/429) are retried
 * with exponential backoff up to {@code REPLY_RETRY_MAX_MS} (default 120s, kept
 * below the consumer's {@code max.poll.interval.ms} to avoid a rebalance).
 * Non-retriable failures (other 4xx, e.g. an expired/invalid token) are logged
 * and skipped.
 */
public class OutputConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OutputConsumer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-message-output";
    private static final String BOOTSTRAP_SERVERS =
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String CONSUMER_GROUP =
            ChatApiApp.env("OUTPUT_CONSUMER_GROUP", "chat-api-output-group");

    /** Total budget for retrying a single HTTP callback before giving up. */
    private static final long REPLY_RETRY_MAX_MS =
            Long.parseLong(ChatApiApp.env("REPLY_RETRY_MAX_MS", "120000"));
    private static final long RETRY_INITIAL_BACKOFF_MS = 1_000L;
    private static final long RETRY_MAX_BACKOFF_MS = 15_000L;

    private static final String DEFAULT_RESPONSE_FIELD = "result";

    private final ChatWebSocketServer wsServer;
    private final ReplyToProducer replyToProducer;
    private final HttpClient httpClient;
    private volatile boolean running = true;

    public OutputConsumer(ChatWebSocketServer wsServer, ReplyToProducer replyToProducer) {
        this.wsServer = wsServer;
        this.replyToProducer = replyToProducer;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public void run() {
        Properties props = new Properties();
        KafkaEnvProps.apply(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            log.info("Output consumer started — listening on topic: {}", TOPIC);

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    String sessionId = record.key() != null ? record.key() : "unknown";
                    deliver(sessionId, record.value());
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("Output consumer error", e);
            }
        }
        log.info("Output consumer stopped");
    }

    /** Routes a message-output record either to an HTTP callback or to WebSocket clients. */
    private void deliver(String sessionId, String value) {
        JsonNode replyTo = null;
        if (value != null) {
            try {
                JsonNode root = mapper.readTree(value);
                JsonNode r = root.get("reply_to");
                if (r != null && r.isObject()) {
                    replyTo = r;
                }
            } catch (Exception e) {
                log.warn("[{}] Failed to parse message-output — falling back to WebSocket: {}",
                        sessionId, e.getMessage());
            }
        }

        if (replyTo != null && "RESTAPI".equalsIgnoreCase(replyTo.path("type").asText())) {
            log.info("[{}] Delivering response via HTTP callback", sessionId);
            deliverHttp(sessionId, value, replyTo);
        } else {
            log.info("[{}] Received response from message-output", sessionId);
            wsServer.broadcastResponse(sessionId, value);
        }
    }

    /** POSTs the response back to the calling agent, with bounded retries. */
    private void deliverHttp(String sessionId, String value, JsonNode replyTo) {
        final HttpRequest request;
        try {
            request = buildRequest(value, replyTo);
        } catch (Exception e) {
            log.error("[{}] Invalid reply-to descriptor — skipping delivery: {}", sessionId, e.getMessage());
            return;
        }

        long deadline = System.currentTimeMillis() + REPLY_RETRY_MAX_MS;
        long backoff = RETRY_INITIAL_BACKOFF_MS;
        int attempt = 0;

        while (running) {
            attempt++;
            try {
                HttpResponse<String> resp =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                int code = resp.statusCode();
                if (code >= 200 && code < 300) {
                    log.info("[{}] Reply delivered (HTTP {}, attempt {}) — tombstoning route",
                            sessionId, code, attempt);
                    replyToProducer.tombstone(sessionId);
                    return;
                }
                if (!isRetriable(code)) {
                    log.error("[{}] Reply delivery failed with non-retriable HTTP {} — skipping. Body: {}",
                            sessionId, code, truncate(resp.body()));
                    return;
                }
                log.warn("[{}] Reply delivery got retriable HTTP {} (attempt {})", sessionId, code, attempt);
            } catch (Exception e) {
                // Connection/timeout errors are retriable.
                log.warn("[{}] Reply delivery error (attempt {}): {}", sessionId, attempt, e.getMessage());
            }

            if (System.currentTimeMillis() + backoff >= deadline) {
                log.error("[{}] Reply delivery exhausted retry budget ({}ms) after {} attempts — skipping",
                        sessionId, REPLY_RETRY_MAX_MS, attempt);
                return;
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            backoff = Math.min(backoff * 2, RETRY_MAX_BACKOFF_MS);
        }
    }

    /** Builds the callback HTTP request from the reply-to descriptor and the response body. */
    private HttpRequest buildRequest(String value, JsonNode replyTo) throws Exception {
        String endpoint = replyTo.path("endpoint").asText("");
        String path = replyTo.path("path").asText("");
        String method = replyTo.path("method").asText("POST");
        String bearerToken = replyTo.path("bearerToken").asText("");
        String responseField = replyTo.path("responseAsField").asText(DEFAULT_RESPONSE_FIELD);

        if (endpoint.isBlank()) {
            throw new IllegalArgumentException("reply-to descriptor missing 'endpoint'");
        }

        String url = joinUrl(endpoint, path);

        // The agent's free-form answer goes under the caller-specified field name.
        String content = "";
        try {
            JsonNode root = mapper.readTree(value);
            content = root.path("content").asText("");
        } catch (Exception ignore) {
            // value already validated upstream; fall through with empty content
        }
        String body = mapper.writeValueAsString(
                mapper.createObjectNode().put(responseField, content));

        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method.toUpperCase(), HttpRequest.BodyPublishers.ofString(body));
        if (!bearerToken.isBlank()) {
            b.header("Authorization", "Bearer " + bearerToken);
        }
        return b.build();
    }

    private static String joinUrl(String endpoint, String path) {
        if (path == null || path.isBlank()) return endpoint;
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        String p = path.startsWith("/") ? path : "/" + path;
        return base + p;
    }

    private static boolean isRetriable(int code) {
        return code >= 500 || code == 408 || code == 429;
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }

    public void stop() {
        running = false;
    }
}
