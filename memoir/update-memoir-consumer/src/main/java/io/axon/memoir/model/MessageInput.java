package io.axon.memoir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * A single message turn (user, assistant, or tool).
 * Mirrors the processing module's MessageInput.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MessageInput(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("user_id")    String userId,
        @JsonProperty("role")       String role,
        @JsonProperty("content")    Object content,
        @JsonProperty("timestamp")  String timestamp,
        @JsonProperty("metadata")   Map<String, Object> metadata
) {
    public String contentAsString() {
        return content != null ? content.toString() : null;
    }
}
