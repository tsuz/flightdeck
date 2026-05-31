package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Final outbound envelope published to {@code message-output} once the LLM
 * signals end-of-turn with no outstanding tool calls.
 *
 * <p>Produced by {@code EndTurnProcessor} as the terminal step of a
 * conversation turn — the value the user-facing layer actually delivers
 * to the end user.
 *
 * <p>{@code replyTo} is an optional transport-level routing descriptor joined in
 * at end-turn from the reply-to topic. It is present only for sessions that were
 * invoked with a {@code reply} object (multi-agent calls); for ordinary
 * interactive chats it is null and omitted from the serialized output. The
 * OutputConsumer uses it to deliver the response back to the caller instead of a
 * WebSocket client.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record UserResponse(
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("user_id")      String userId,
        @JsonProperty("content")      String content,
        @JsonProperty("input_tokens") int inputTokens,
        @JsonProperty("output_tokens") int outputTokens,
        @JsonProperty("cost")         Double cost,
        @JsonProperty("source_agent") String sourceAgent,  // which agent produced this
        @JsonProperty("reply_to")     Map<String, Object> replyTo,  // multi-agent reply route (nullable)
        @JsonProperty("timestamp")    String timestamp
) {}
