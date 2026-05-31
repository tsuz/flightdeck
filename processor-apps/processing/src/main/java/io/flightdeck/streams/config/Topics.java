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

    // ── Multi-agent reply routing ──────────────────────────────────────────────
    /**
     * Per-session reply-routing descriptors, keyed by session_id. Written by the
     * chat-api {@code /api/chat} endpoint when an inbound request carries a
     * {@code reply} object, joined into {@code message-output} at end-turn, and
     * consumed by the chat-api OutputConsumer to deliver the response back to the
     * caller. Compacted with a time-based retention ({@code REPLY_TO_STATE_TTL_MS})
     * so stale routes are eventually dropped.
     *
     * <p>TODO(multi-agent): the key is {@code session_id}, so a session can hold
     * only ONE outstanding reply route at a time — a second {@code reply}
     * descriptor for the same session overwrites the first. To support multiple
     * concurrent callbacks within one session (e.g. parallel sub-agent calls that
     * reuse the session), key by {@code sessionId:requestId} instead and carry the
     * requestId through the join. Deferred — one route per session is sufficient
     * for the current one-shot delegation flow.
     */
    public static final String REPLY_TO                 = PREFIX + "reply-to";
}