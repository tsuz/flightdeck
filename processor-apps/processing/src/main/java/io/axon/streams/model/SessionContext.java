package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Accumulated per-session conversation history stored in the
 * {@code message-context} KTable.
 *
 * <ul>
 *   <li>{@code messages} — the most recent LLM exchange only (latest turn)</li>
 *   <li>{@code history}  — the full chronological conversation so far</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionContext(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("user_id")    String userId,
        @JsonProperty("cost")       double cost,
        @JsonProperty("llm_calls")  int llmCalls,
        @JsonProperty("messages")   List<MessageInput> messages,  // latest turn only
        @JsonProperty("history")    List<MessageInput> history,   // full conversation
        @JsonProperty("timestamp")  String timestamp
) {
    /** Empty context — used as the Kafka Streams aggregate initialiser. */
    public static SessionContext empty(String sessionId) {
        return new SessionContext(sessionId, null, 0.0, 0, List.of(), List.of(), null);
    }
}