package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Core message envelope flowing through the agent pipeline.
 * Represents a single turn from any participant: user, assistant, or tool.
 *
 * Content is Object to support both plain text (String) and structured data
 * (List/Map for tool results) without double-serialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MessageInput(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("user_id")    String userId,
        @JsonProperty("role")       String role,       // "user" | "assistant" | "tool"
        @JsonProperty("content")    Object content,
        @JsonProperty("timestamp")  String timestamp,
        @JsonProperty("metadata")   Map<String, Object> metadata
) {

    /** Returns content as a String (for user/assistant messages or when content is text). */
    public String contentAsString() {
        return content != null ? content.toString() : null;
    }
}