package io.flightdeck.streams.config;

/**
 * Central registry of all Kafka topic names used in the Flightdeck agent pipeline.
 * Mirrors every topic node visible in the architecture diagram.
 */
public final class Topics {

    private Topics() {}

    // ── Inbound ──────────────────────────────────────────────────────────────
    /** Raw user messages + scheduler-triggered events */
    public static final String MESSAGE_INPUT            = "message-input";

    // ── Context building ─────────────────────────────────────────────────────
    /** Per-session accumulated conversation history (KTable backing store) */
    public static final String SESSION_CONTEXT           = "session-context";

    /** Merged: historical context + latest user message, ready for the LLM */
    public static final String ENRICHED_MESSAGE_INPUT   = "enriched-message-input";

    // ── LLM reasoning ────────────────────────────────────────────────────────
    /** LLM (Claude) response including any tool-use blocks */
    public static final String THINK_REQUEST_RESPONSE   = "think-request-response";

    // ── Tool dispatch ─────────────────────────────────────────────────────────
    /** One message per individual tool-use item, keyed by session_id */
    public static final String TOOL_USE                 = "tool-use";

    /** Dead-letter queue for failed / unroutable tool-use items */
    public static final String TOOL_USE_DLQ             = "tool-use-dlq";

    /** Results returned from tool execution, keyed by session_id */
    public static final String TOOL_USE_RESULT          = "tool-use-result";

    /** Fired once ALL expected tool results for a session have arrived */
    public static final String TOOL_USE_ALL_COMPLETE    = "tool-use-all-complete";

    // ── Observability ─────────────────────────────────────────────────────────
    /** Aggregated cost (tokens × pricing) per conversation session */
    public static final String SESSION_COST             = "session-cost";

    /** Per-tool latency metrics, keyed by tool_name */
    public static final String TOOL_USE_LATENCY         = "tool-use-latency";

    // ── Session lifecycle ──────────────────────────────────────────────────────
    /** Emitted when a session has been inactive for a configured period */
    public static final String SESSION_END              = "session-end";

    // ── Memoir ───────────────────────────────────────────────────────────────
    /** Long-term memoir / summary context per session (KTable) */
    public static final String MEMOIR_CONTEXT           = "memoir-context";

    /** Joined snapshot emitted on session end: memoir + last response */
    public static final String MEMOIR_CONTEXT_SESSION_END = "memoir-context-session-end";

    // ── Outbound ──────────────────────────────────────────────────────────────
    /** Final responses sent back to the user-facing layer */
    public static final String MESSAGE_OUTPUT           = "message-output";
}