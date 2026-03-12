package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-tool execution latency metric published onto {@code tool-use-latency},
 * keyed by {@code tool_name}.  Produced by {@code ToolLatencyAggregationProcessor}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolLatency(
        @JsonProperty("tool_name")   String toolName,
        @JsonProperty("latency_ms")  long latencyMs,
        @JsonProperty("session_id")  String sessionId,
        @JsonProperty("timestamp")   String timestamp
) {}