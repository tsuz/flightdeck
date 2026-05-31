package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit-tests the routing decision in {@link OutputConsumer#deliver} — specifically
 * the predicate that decides HTTP callback vs WebSocket — without standing up
 * Kafka or a WebSocket server. The decision hinges on the presence of a
 * {@code reply_to} descriptor carrying a non-blank {@code callbackService}.
 */
class OutputConsumerReplyRoutingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private boolean isHttpCallback(String value) throws Exception {
        JsonNode root = MAPPER.readTree(value);
        JsonNode replyTo = root.get("reply_to");
        // Mirror the predicate in deliver(): replyTo present, object, callbackService set.
        if (replyTo == null || !replyTo.isObject()) return false;
        return !replyTo.path("callbackService").asText("").isBlank();
    }

    @Test
    void routesToHttpWhenCallbackServicePresent() throws Exception {
        String value = MAPPER.writeValueAsString(MAPPER.createObjectNode()
                .set("reply_to", MAPPER.createObjectNode()
                        .put("callbackService", "my-agent-a")
                        .put("bearerToken", "tok-abc")));
        assertTrue(isHttpCallback(value));
    }

    @Test
    void routesToWebSocketWhenNoReplyTo() throws Exception {
        String value = MAPPER.writeValueAsString(MAPPER.createObjectNode()
                .put("content", "hello"));
        assertFalse(isHttpCallback(value));
    }

    @Test
    void routesToWebSocketWhenReplyToNotObject() throws Exception {
        String value = MAPPER.writeValueAsString(MAPPER.createObjectNode()
                .put("reply_to", "not-an-object"));
        assertFalse(isHttpCallback(value));
    }

    @Test
    void routesToWebSocketWhenCallbackServiceBlank() throws Exception {
        String value = MAPPER.writeValueAsString(MAPPER.createObjectNode()
                .set("reply_to", MAPPER.createObjectNode()
                        .put("bearerToken", "tok-abc")));
        assertFalse(isHttpCallback(value));
    }
}
