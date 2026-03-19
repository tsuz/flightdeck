package io.flightdeck.toolexec.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolUseResult(
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("tool_use_id")  String toolUseId,
        @JsonProperty("name")         String name,
        @JsonProperty("result")       Map<String, Object> result,
        @JsonProperty("latency_ms")   long latencyMs,
        @JsonProperty("status")       String status,
        @JsonProperty("total_tools")  int totalTools,
        @JsonProperty("timestamp")    String timestamp
) {}
