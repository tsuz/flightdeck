package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Produced by the stream–table join in {@code AccumulateMessageContextProcessor}.
 * Combines the full conversation history from the {@code message-context} KTable
 * with the latest incoming user message from {@code message-input}.
 * Published onto {@code full-message-context} and consumed by the Think processor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FullMessageContext(
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("user_id")      String userId,
        @JsonProperty("history")      List<AgentMessage> history,
        @JsonProperty("latest_input") AgentMessage latestInput,
        @JsonProperty("timestamp")    String timestamp
) {}