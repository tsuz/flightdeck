package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * The result returned after a tool function has been executed.
 * Published onto {@code tool-use-result}, keyed by {@code session_id}.
 * Consumed by {@code ToolResultAggregationProcessor} and
 * {@code ToolLatencyAggregationProcessor}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolUseResult(
        @JsonProperty("session_id")   String sessionId,
        @JsonProperty("tool_use_id")  String toolUseId,
        @JsonProperty("name")         String name,
        @JsonProperty("result")       Map<String, Object> result,
        @JsonProperty("latency_ms")   long latencyMs,
        @JsonProperty("status")       String status,   // "success" | "error"
        @JsonProperty("total_tools")  int totalTools,
        @JsonProperty("timestamp")    String timestamp
) {}