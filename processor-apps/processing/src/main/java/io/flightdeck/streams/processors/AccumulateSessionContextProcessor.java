package io.flightdeck.streams.processors;

import io.flightdeck.streams.model.MessageInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Formerly the session-context accumulator. Now superseded by the
 * think-request-response KTable which carries full history directly.
 *
 * <p>Static helpers retained for reuse.
 */
public class AccumulateSessionContextProcessor {

    private AccumulateSessionContextProcessor() {}

    /**
     * Immutably appends {@code newMessages} after {@code history}.
     * Null-safe on both inputs.
     */
    static List<MessageInput> appendMessages(
            List<MessageInput> history,
            List<MessageInput> newMessages) {

        List<MessageInput> combined = new ArrayList<>();
        if (history     != null) combined.addAll(history);
        if (newMessages != null) combined.addAll(newMessages);
        return Collections.unmodifiableList(combined);
    }

    /**
     * Returns {@code incoming} if non-blank, otherwise falls back to
     * {@code existing} to preserve user identity across turns.
     */
    static String resolveUserId(String existing, String incoming) {
        return (incoming != null && !incoming.isBlank()) ? incoming : existing;
    }
}
