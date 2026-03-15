package io.axon.think.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MessageInput(
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("user_id")    String userId,
        @JsonProperty("role")       String role,
        @JsonProperty("content")    Object content,
        @JsonProperty("timestamp")  String timestamp,
        @JsonProperty("metadata")   Map<String, Object> metadata
) {

    /** Returns content as a String (for user/assistant messages or when content is text). */
    public String contentAsString() {
        return content != null ? content.toString() : null;
    }
}
