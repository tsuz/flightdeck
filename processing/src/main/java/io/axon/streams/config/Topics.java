package io.axon.streams.config;

/**
 * Central registry of all Kafka topic names used in the Axon agent pipeline.
 * Mirrors every topic node visible in the architecture diagram.
 */
public final class Topics {

    private Topics() {}

    // ── Inbound ──────────────────────────────────────────────────────────────
    /** Raw user messages + scheduler-triggered events */
    public static final String MESSAGE_INPUT            = "message-input";

    // ── Context building ─────────────────────────────────────────────────────
    /** Per-session accumulated conversation history (KTable backing store) */
    public static final String MESSAGE_CONTEXT          = "message-context";

    /** Merged: historical context + latest user message, ready for the LLM */
    public static final String FULL_MESSAGE_CONTEXT     = "full-message-context";

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

    // ── Outbound ──────────────────────────────────────────────────────────────
    /** Final responses sent back to the user-facing layer */
    public static final String MESSAGE_OUTPUT           = "message-output";
}