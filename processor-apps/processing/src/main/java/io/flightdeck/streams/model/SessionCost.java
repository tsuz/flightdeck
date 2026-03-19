package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Running cost aggregate for a single conversation session.
 * Published onto {@code session-cost} by {@code SessionCostAggregationProcessor}
 * every time a new {@link ThinkResponse} is processed.
 *
 * <p>The diagram annotation: <em>"Aggregate cost per conversation"</em> and
 * <em>"Emit Tombstone when aggregated"</em>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionCost(
        @JsonProperty("session_id")          String sessionId,
        @JsonProperty("user_id")             String userId,
        @JsonProperty("llm_calls")           int llmCalls,
        @JsonProperty("total_input_tokens")  int totalInputTokens,
        @JsonProperty("total_output_tokens") int totalOutputTokens,
        @JsonProperty("estimated_cost_usd")  double estimatedCostUsd,
        @JsonProperty("timestamp")           String timestamp
) {
    /** Zero-value initialiser used by the Kafka Streams aggregator. */
    public static SessionCost zero(String sessionId, String userId) {
        return new SessionCost(sessionId, userId, 0, 0, 0, 0.0, null);
    }
}