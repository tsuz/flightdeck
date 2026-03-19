package io.flightdeck.streams.config;

/**
 * Central registry of all Kafka topic names used in the Flightdeck agent pipeline.
 * Mirrors every topic node visible in the architecture diagram.
 */
public final class Topics {

    private Topics() {}

    /** Agent name — required, used to prefix all topic names as {AGENT_NAME}-{topic}. */
    public static final String AGENT_NAME = requireEnv("AGENT_NAME");
    public static final String PREFIX = AGENT_NAME + "-";

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable " + key + " is not set");
        }
        return value;
    }

    // ── Inbound ──────────────────────────────────────────────────────────────
    /** Raw user messages + scheduler-triggered events */
    public static final String MESSAGE_INPUT            = PREFIX + "message-input";

    // ── Context building ─────────────────────────────────────────────────────
    /** Per-session accumulated conversation history (KTable backing store) */
    public static final String SESSION_CONTEXT           = PREFIX + "session-context";

    /** Merged: historical context + latest user message, ready for the LLM */
    public static final String ENRICHED_MESSAGE_INPUT   = PREFIX + "enriched-message-input";

    // ── LLM reasoning ────────────────────────────────────────────────────────
    /** LLM (Claude) response including any tool-use blocks */
    public static final String THINK_REQUEST_RESPONSE   = PREFIX + "think-request-response";

    // ── Tool dispatch ─────────────────────────────────────────────────────────
    /** One message per individual tool-use item, keyed by session_id */
    public static final String TOOL_USE                 = PREFIX + "tool-use";

    /** Dead-letter queue for failed / unroutable tool-use items */
    public static final String TOOL_USE_DLQ             = PREFIX + "tool-use-dlq";

    /** Results returned from tool execution, keyed by session_id */
    public static final String TOOL_USE_RESULT          = PREFIX + "tool-use-result";

    /** Fired once ALL expected tool results for a session have arrived */
    public static final String TOOL_USE_ALL_COMPLETE    = PREFIX + "tool-use-all-complete";

    // ── Observability ─────────────────────────────────────────────────────────
    /** Aggregated cost (tokens × pricing) per conversation session */
    public static final String SESSION_COST             = PREFIX + "session-cost";

    /** Per-tool latency metrics, keyed by tool_name */
    public static final String TOOL_USE_LATENCY         = PREFIX + "tool-use-latency";

    // ── Session lifecycle ──────────────────────────────────────────────────────
    /** Emitted when a session has been inactive for a configured period */
    public static final String SESSION_END              = PREFIX + "session-end";

    // ── Memoir ───────────────────────────────────────────────────────────────
    /** Long-term memoir / summary context per session (KTable) */
    public static final String MEMOIR_CONTEXT           = PREFIX + "memoir-context";

    /** Joined snapshot emitted on session end: memoir + last response */
    public static final String MEMOIR_CONTEXT_SESSION_END = PREFIX + "memoir-context-session-end";

    // ── Outbound ──────────────────────────────────────────────────────────────
    /** Final responses sent back to the user-facing layer */
    public static final String MESSAGE_OUTPUT           = PREFIX + "message-output";
}