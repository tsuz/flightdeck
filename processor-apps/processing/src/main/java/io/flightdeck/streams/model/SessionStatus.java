package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-session concurrency state owned by the
 * {@code SessionConcurrencyGuardProcessor}'s {@code session-status-store}.
 *
 * <p>A session is {@code busy} from the moment a user message is admitted (a turn
 * starts) until a <em>clean end-turn</em> response is observed
 * ({@code endTurn == true} with no outstanding tool calls). While busy, a new
 * incoming user message is rejected rather than appended to the conversation —
 * appending it mid-turn would place a {@code tool_use} block immediately before a
 * {@code user} message instead of its {@code tool_result}, which the LLM API
 * rejects with a 400 and which kills the turn.
 *
 * <ul>
 *   <li>{@code turnStartedMs} — wall-clock ms when the current turn was admitted.</li>
 *   <li>{@code lastActivityMs} — wall-clock ms of the most recent event for this
 *       session (admit, tool continuation, or think response). Drives the
 *       idle-based TTL safety net that unwedges a session whose turn crashed or
 *       whose end-turn signal was lost.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionStatus(
        @JsonProperty("busy")             boolean busy,
        @JsonProperty("turn_started_ms")  long turnStartedMs,
        @JsonProperty("last_activity_ms") long lastActivityMs,
        @JsonProperty("timestamp")        String timestamp
) {}
