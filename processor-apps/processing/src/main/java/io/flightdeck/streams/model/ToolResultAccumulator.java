package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Per-session accumulator held in RocksDB state store while tool results are
 * arriving. Published to {@code tool-use-all-complete} once every expected tool
 * result has been collected.
 *
 * <h3>Expected set vs count</h3>
 * The accumulator is seeded from {@code think-request-response} with the full
 * {@link ExpectedTool} set (every {@code tool_use_id} + {@code name} the LLM
 * requested) plus a wall-clock {@code deadlineMs}. Completion is then defined as
 * "every expected tool_use_id has a result". This holds whether a result was
 * produced synchronously by a tool consumer or asynchronously via the
 * {@code /api/tool/response} callback — they are indistinguishable on the
 * {@code tool-use-result} topic.
 *
 * <p>If results arrive before the seed (cross-topic ordering), the accumulator
 * falls back to count-based completion using {@code expectedCount} until the
 * seed merges in the expected set.
 *
 * <h3>One-shot guard &amp; tombstone</h3>
 * The {@code emitted} flag prevents a second emission from late/duplicate
 * results. After emission the entry is kept as an {@code emitted=true} tombstone
 * (rather than deleted) until {@code deadlineMs} passes, so a late async
 * callback for an already-completed turn is ignored instead of starting a fresh,
 * never-completing accumulator.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolResultAccumulator(
        @JsonProperty("session_id")     String sessionId,
        @JsonProperty("user_id")        String userId,
        @JsonProperty("expected_count") int expectedCount,
        @JsonProperty("results")        List<ToolUseResult> results,
        @JsonProperty("expected")       List<ExpectedTool> expected,
        @JsonProperty("deadline_ms")    long deadlineMs,
        @JsonProperty("emitted")        boolean emitted,
        @JsonProperty("timestamp")      String timestamp
) {

    /** A tool call the LLM requested, recorded so timeouts can synthesise an error result. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ExpectedTool(
            @JsonProperty("tool_use_id") String toolUseId,
            @JsonProperty("name")        String name
    ) {}

    /** Zero-value initialiser used when a new session's first result arrives (no seed yet). */
    public static ToolResultAccumulator empty(String sessionId, String userId, int expectedCount) {
        return new ToolResultAccumulator(
                sessionId, userId, expectedCount, List.of(), List.of(), 0L, false, null);
    }

    /** Returns {@code true} when all expected tool results have been collected. */
    public boolean isComplete() {
        if (emitted || results == null) return false;
        if (expected != null && !expected.isEmpty()) {
            return missing().isEmpty();
        }
        // Fallback before the seed has merged: count-based.
        return expectedCount > 0 && results.size() >= expectedCount;
    }

    /** Expected tools that do not yet have a corresponding result, by tool_use_id. */
    public List<ExpectedTool> missing() {
        if (expected == null || expected.isEmpty()) return List.of();
        Set<String> have = results == null ? Set.of()
                : results.stream()
                        .map(ToolUseResult::toolUseId)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        return expected.stream()
                .filter(e -> e.toolUseId() != null && !have.contains(e.toolUseId()))
                .collect(Collectors.toList());
    }
}
