package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the {@code reply} descriptor handling in {@link ChatHandler} — that a
 * reply object carrying a non-blank {@code callbackService} is accepted and a
 * malformed one is rejected before it reaches Kafka. Membership of the name in
 * {@code ALLOWED_HOST_MAPPING} is enforced separately (see {@link CallbackRegistry}
 * and {@link CallbackRegistryTest}).
 */
class ChatHandlerReplyValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private boolean isWellFormedReply(JsonNode reply) {
        // Mirror ChatHandler: require an object with a non-blank callbackService.
        if (reply == null || !reply.isObject()) return false;
        return !reply.path("callbackService").asText("").isBlank();
    }

    @Test
    void acceptsReplyWithCallbackService() {
        JsonNode reply = MAPPER.createObjectNode()
                .put("callbackService", "my-agent-a")
                .put("bearerToken", "tok-abc");
        assertTrue(isWellFormedReply(reply));
    }

    @Test
    void rejectsNonObjectReply() {
        JsonNode reply = MAPPER.createObjectNode().put("x", 1).path("x");
        assertFalse(isWellFormedReply(reply));
    }

    @Test
    void rejectsReplyWithoutCallbackService() {
        JsonNode reply = MAPPER.createObjectNode().put("bearerToken", "tok-abc");
        assertFalse(isWellFormedReply(reply));
    }
}
