package io.axon.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Per-session accumulator held in RocksDB state store while tool results are
 * arriving.  Published to {@code tool-use-all-complete} once
 * {@code results.size() == expectedCount}.
 *
 * <p>The {@code emitted} flag acts as a one-shot guard — once the complete
 * event has been forwarded downstream the flag is set to {@code true} so
 * that any late-arriving duplicate results do not trigger a second emission.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ToolResultAccumulator(
        @JsonProperty("session_id")     String sessionId,
        @JsonProperty("user_id")        String userId,
        @JsonProperty("expected_count") int expectedCount,
        @JsonProperty("results")        List<ToolUseResult> results,
        @JsonProperty("emitted")        boolean emitted,
        @JsonProperty("timestamp")      String timestamp
) {
    /** Zero-value initialiser used when a new session's first result arrives. */
    public static ToolResultAccumulator empty(String sessionId, String userId, int expectedCount) {
        return new ToolResultAccumulator(sessionId, userId, expectedCount, List.of(), false, null);
    }

    /** Returns {@code true} when all expected tool results have been collected. */
    public boolean isComplete() {
        return !emitted && results != null && results.size() >= expectedCount && expectedCount > 0;
    }
}