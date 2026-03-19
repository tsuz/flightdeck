package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Produced by the stream–table join in {@code AccumulateSessionContextProcessor}.
 * Combines the full conversation history from the {@code message-context} KTable
 * with the latest incoming user message from {@code message-input}.
 * Published onto {@code enriched-message-input} and consumed by the Think processor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FullSessionContext(
        @JsonProperty("session_id")      String sessionId,
        @JsonProperty("user_id")         String userId,
        @JsonProperty("history")         List<MessageInput> history,
        @JsonProperty("latest_input")    MessageInput latestInput,
        @JsonProperty("memoir_context")  String memoirContext,
        @JsonProperty("timestamp")       String timestamp
) {}