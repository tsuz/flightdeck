package io.flightdeck.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of the shared {@link org.apache.kafka.clients.producer.Producer}:
 * drives the real HTTP endpoints and asserts that records for all three outbound
 * topics — {@code message-input}, {@code reply-to} and {@code tool-use-result} —
 * land on the <em>single</em> producer instance wired into the three wrappers.
 *
 * <p>A {@link MockProducer} stands in for the Kafka broker, so the test exercises
 * the full {@code HTTP handler → producer wrapper → ProducerRecord} path without
 * a running cluster. {@code AGENT_NAME} (= {@code test-agent}) and
 * {@code ALLOWED_HOST_MAPPING} are provided by the surefire configuration.
 */
class SharedProducerApiIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String AGENT_NAME = "test-agent";
    private static final String CALLBACK_SECRET = "integration-test-secret";

    private static final String MESSAGE_INPUT_TOPIC   = AGENT_NAME + "-message-input";
    private static final String REPLY_TO_TOPIC        = AGENT_NAME + "-reply-to";
    private static final String TOOL_USE_RESULT_TOPIC = AGENT_NAME + "-tool-use-result";

    private MockProducer<String, String> sharedProducer;
    private HttpServer httpServer;
    private String baseUrl;
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void startServer() throws Exception {
        // One shared producer feeding all three wrappers — the unit under test.
        sharedProducer = new MockProducer<>(
                true, new ZeroPartitioner(), new StringSerializer(), new StringSerializer());

        KafkaMessageProducer messageProducer = new KafkaMessageProducer(sharedProducer);
        ReplyToProducer replyToProducer = new ReplyToProducer(sharedProducer);
        ToolResultProducer toolResultProducer = new ToolResultProducer(sharedProducer);

        httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/api/chat", new ChatHandler(messageProducer, replyToProducer));
        httpServer.createContext("/api/tools/response",
                new ToolResponseHandler(toolResultProducer, CALLBACK_SECRET));
        httpServer.setExecutor(null);
        httpServer.start();
        baseUrl = "http://127.0.0.1:" + httpServer.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    @Test
    @DisplayName("Both API endpoints route all three topics through the one shared producer")
    void allThreeTopicsProducedThroughSharedProducer() throws Exception {
        String sessionId = "session-itest-1";

        // 1. POST /api/chat with a reply descriptor → message-input + reply-to.
        ObjectNode reply = MAPPER.createObjectNode()
                .put("callbackService", "test-callback")
                .put("bearerToken", "tok-abc");
        ObjectNode chatBody = MAPPER.createObjectNode();
        chatBody.put("session_id", sessionId);
        chatBody.put("content", "hello there");
        chatBody.set("reply", reply);

        HttpResponse<String> chatResp = post("/api/chat", chatBody.toString(), null);
        assertThat(chatResp.statusCode()).isEqualTo(200);

        // 2. POST /api/tools/response with a valid callback token → tool-use-result.
        String toolUseId = "tool-use-itest-1";
        String token = mintToken(sessionId, toolUseId, "my_tool");
        HttpResponse<String> toolResp =
                post("/api/tools/response", "{\"result\":\"done\"}", "Bearer " + token);
        assertThat(toolResp.statusCode()).isEqualTo(202);

        // 3. All three records landed on the single shared producer, keyed by session_id.
        List<ProducerRecord<String, String>> history = sharedProducer.history();
        Map<String, ProducerRecord<String, String>> byTopic = history.stream()
                .collect(Collectors.toMap(ProducerRecord::topic, r -> r, (a, b) -> a));

        assertThat(byTopic.keySet()).containsExactlyInAnyOrder(
                MESSAGE_INPUT_TOPIC, REPLY_TO_TOPIC, TOOL_USE_RESULT_TOPIC);
        assertThat(history).allSatisfy(r -> assertThat(r.key()).isEqualTo(sessionId));

        assertThat(byTopic.get(MESSAGE_INPUT_TOPIC).value()).contains("hello there");
        assertThat(byTopic.get(REPLY_TO_TOPIC).value()).contains("test-callback");
        assertThat(byTopic.get(TOOL_USE_RESULT_TOPIC).value()).contains(toolUseId);
    }

    @Test
    @DisplayName("/api/chat without a reply descriptor produces only message-input")
    void chatWithoutReplyProducesOnlyMessageInput() throws Exception {
        String sessionId = "session-itest-2";
        ObjectNode chatBody = MAPPER.createObjectNode();
        chatBody.put("session_id", sessionId);
        chatBody.put("content", "no reply here");

        HttpResponse<String> resp = post("/api/chat", chatBody.toString(), null);
        assertThat(resp.statusCode()).isEqualTo(200);

        List<ProducerRecord<String, String>> history = sharedProducer.history();
        assertThat(history).hasSize(1);
        assertThat(history.get(0).topic()).isEqualTo(MESSAGE_INPUT_TOPIC);
        assertThat(history.get(0).key()).isEqualTo(sessionId);
    }

    private HttpResponse<String> post(String path, String body, String authHeader) throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (authHeader != null) {
            req.header("Authorization", authHeader);
        }
        return client.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * Mints a callback token matching the SDK / {@link ToolCallbackToken} layout:
     * {@code base64url(payloadJson) "." base64url(HMAC_SHA256(secret, payloadJson))}.
     */
    private static String mintToken(String sessionId, String toolUseId, String name) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("session_id", sessionId);
        payload.put("tool_use_id", toolUseId);
        payload.put("name", name);
        payload.put("total_tools", 1);
        payload.put("iat", Instant.now().getEpochSecond());
        payload.put("exp", 0); // 0 → no expiry check

        byte[] payloadBytes = MAPPER.writeValueAsBytes(payload);
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(CALLBACK_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] sig = mac.doFinal(payloadBytes);

        return enc.encodeToString(payloadBytes) + "." + enc.encodeToString(sig);
    }

    /** MockProducer (Kafka 4.x) needs a Partitioner; the assertions ignore partitions. */
    private static final class ZeroPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
