package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The full response from the LLM (Claude API) after processing a
 * {@code enriched-message-input}.  May contain zero or more tool-use blocks.
 * Published onto {@code think-request-response}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ThinkResponse(
        @JsonProperty("session_id")    String sessionId,
        @JsonProperty("user_id")       String userId,
        @JsonProperty("cost")          double cost,
        @JsonProperty("input_tokens")  int inputTokens,
        @JsonProperty("output_tokens") int outputTokens,
        @JsonProperty("messages")      List<MessageInput> messages,
        @JsonProperty("tool_uses")     List<ToolUseItem> toolUses,
        @JsonProperty("end_turn")      boolean endTurn,
        @JsonProperty("timestamp")     String timestamp
) {}