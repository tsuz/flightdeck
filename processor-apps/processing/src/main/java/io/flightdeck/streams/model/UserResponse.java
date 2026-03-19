package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Final outbound envelope published to {@code message-output} once the LLM
 * signals end-of-turn with no outstanding tool calls.
 *
 * <p>Produced by {@code EndTurnProcessor} as the terminal step of a
 * conversation turn — the value the user-facing layer actually delivers
 * to the end user.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UserResponse(
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("user_id")      String userId,
        @JsonProperty("content")      String content,
        @JsonProperty("llm_calls")    int llmCalls,
        @JsonProperty("input_tokens") int inputTokens,
        @JsonProperty("output_tokens") int outputTokens,
        @JsonProperty("cost")         double cost,
        @JsonProperty("source_agent") String sourceAgent,  // which agent produced this
        @JsonProperty("timestamp")    String timestamp
) {}