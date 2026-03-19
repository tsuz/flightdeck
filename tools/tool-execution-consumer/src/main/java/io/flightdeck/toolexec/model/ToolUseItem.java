package io.flightdeck.toolexec.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolUseItem(
        @JsonProperty("tool_use_id")  String toolUseId,
        @JsonProperty("tool_id")      String toolId,
        @JsonProperty("name")         String name,
        @JsonProperty("input")        Map<String, Object> input,
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("total_tools")  int totalTools,
        @JsonProperty("timestamp")    String timestamp
) {}
