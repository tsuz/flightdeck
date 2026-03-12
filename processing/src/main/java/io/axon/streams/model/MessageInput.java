package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Core message envelope flowing through the agent pipeline.
 * Represents a single turn from any participant: user, assistant, or tool.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MessageInput(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("user_id")    String userId,
        @JsonProperty("role")       String role,       // "user" | "assistant" | "tool"
        @JsonProperty("content")    String content,
        @JsonProperty("timestamp")  String timestamp,
        @JsonProperty("metadata")   Map<String, Object> metadata
) {}