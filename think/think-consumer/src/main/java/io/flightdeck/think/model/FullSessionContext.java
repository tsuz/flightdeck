package io.flightdeck.think.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FullSessionContext(
        @JsonProperty("session_id")      String sessionId,
        @JsonProperty("user_id")         String userId,
        @JsonProperty("cost")            Double cost,
        @JsonProperty("history")         List<MessageInput> history,
        @JsonProperty("latest_input")    MessageInput latestInput,
        @JsonProperty("memoir_context")  String memoirContext,
        @JsonProperty("system_prompt")   String systemPrompt,
        @JsonProperty("timestamp")       String timestamp
) {
    /** Convenience constructor for contexts without a per-user system prompt override. */
    public FullSessionContext(String sessionId, String userId, Double cost,
                              List<MessageInput> history, MessageInput latestInput,
                              String memoirContext, String timestamp) {
        this(sessionId, userId, cost, history, latestInput, memoirContext, null, timestamp);
    }
}
