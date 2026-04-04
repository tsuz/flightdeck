package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The full response from the LLM after processing an
 * {@code enriched-message-input}.  May contain zero or more tool-use blocks.
 * Published onto {@code think-request-response}.
 *
 * <ul>
 *   <li>{@code previousMessages} — full conversation history before this turn (compacted if applicable)</li>
 *   <li>{@code lastInputMessage} — the message from message-input that triggered this turn</li>
 *   <li>{@code lastInputResponse} — the LLM's response messages for this turn</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ThinkResponse(
        @JsonProperty("session_id")              String sessionId,
        @JsonProperty("user_id")                 String userId,
        @JsonProperty("cost")                    Double cost,
        @JsonProperty("prev_session_cost")       Double prevSessionCost,
        @JsonProperty("input_tokens")            int inputTokens,
        @JsonProperty("output_tokens")           int outputTokens,
        @JsonProperty("previous_messages")       List<MessageInput> previousMessages,
        @JsonProperty("last_input_message")      MessageInput lastInputMessage,
        @JsonProperty("last_input_response")     List<MessageInput> lastInputResponse,
        @JsonProperty("tool_uses")               List<ToolUseItem> toolUses,
        @JsonProperty("end_turn")                boolean endTurn,
        @JsonProperty("compaction")              boolean compaction,
        @JsonProperty("compaction_input_tokens") int compactionInputTokens,
        @JsonProperty("compaction_output_tokens") int compactionOutputTokens,
        @JsonProperty("compaction_cost")         double compactionCost,
        @JsonProperty("timestamp")               String timestamp
) {}
