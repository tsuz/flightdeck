package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Snapshot emitted when a session ends: combines the long-term memoir context
 * with the last LLM response for that session.
 * Published onto {@code memoir-context-session-end}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MemoirSessionEnd(
        @JsonProperty("session_id")      String sessionId,
        @JsonProperty("user_id")         String userId,
        @JsonProperty("memoir_context")   String memoirContext,
        @JsonProperty("think_response")   ThinkResponse thinkResponse,
        @JsonProperty("timestamp")        String timestamp
) {}
