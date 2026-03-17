package io.axon.think.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FullSessionContext(
        @JsonProperty("session_id")      String sessionId,
        @JsonProperty("user_id")         String userId,
        @JsonProperty("history")         List<MessageInput> history,
        @JsonProperty("latest_input")    MessageInput latestInput,
        @JsonProperty("timestamp")       String timestamp
) {}
