package io.flightdeck.think.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ThinkResponse(
        @JsonProperty("session_id")    String sessionId,
        @JsonProperty("user_id")       String userId,
        @JsonProperty("cost")              Double cost,
        @JsonProperty("prev_session_cost") Double prevSessionCost,
        @JsonProperty("input_tokens")      int inputTokens,
        @JsonProperty("output_tokens") int outputTokens,
        @JsonProperty("messages")      List<MessageInput> messages,
        @JsonProperty("tool_uses")     List<ToolUseItem> toolUses,
        @JsonProperty("end_turn")      boolean endTurn,
        @JsonProperty("timestamp")     String timestamp,
        @JsonProperty("compacted_history") List<MessageInput> compactedHistory
) {}
