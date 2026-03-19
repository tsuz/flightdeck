package io.flightdeck.memoir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Snapshot received when a session ends: the previous memoir context
 * combined with the last LLM response from that session.
 * Consumed from {@code memoir-context-session-end}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MemoirSessionEnd(
        @JsonProperty("session_id")      String sessionId,
        @JsonProperty("user_id")         String userId,
        @JsonProperty("memoir_context")   String memoirContext,
        @JsonProperty("think_response")   ThinkResponse thinkResponse,
        @JsonProperty("timestamp")        String timestamp
) {}
