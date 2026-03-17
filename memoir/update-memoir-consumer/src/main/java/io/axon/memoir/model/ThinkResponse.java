package io.axon.memoir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Mirrors the ThinkResponse from the processing module.
 * Only the fields needed for memoir update are included.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ThinkResponse(
        @JsonProperty("session_id")    String sessionId,
        @JsonProperty("user_id")       String userId,
        @JsonProperty("messages")      List<MessageInput> messages,
        @JsonProperty("timestamp")     String timestamp
) {}
