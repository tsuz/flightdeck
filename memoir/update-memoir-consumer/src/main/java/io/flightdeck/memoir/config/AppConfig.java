package io.flightdeck.memoir.config;

/**
 * Static configuration loaded from environment variables with sensible defaults.
 */
public final class AppConfig {

    private AppConfig() {}

    // ── Kafka ────────────────────────────────────────────────────────────────
    public static final String BOOTSTRAP_SERVERS =
            env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String CONSUMER_GROUP =
            env("KAFKA_CONSUMER_GROUP", "update-memoir-consumer-group");
    public static final String INPUT_TOPIC =
            env("KAFKA_INPUT_TOPIC", "memoir-context-session-end");
    public static final String OUTPUT_TOPIC =
            env("KAFKA_OUTPUT_TOPIC", "memoir-context");

    // ── Claude API ───────────────────────────────────────────────────────────
    public static final String CLAUDE_API_KEY =
            env("CLAUDE_API_KEY", "");
    public static final String CLAUDE_API_URL =
            env("CLAUDE_API_URL", "https://api.anthropic.com/v1/messages");
    public static final String CLAUDE_MODEL =
            env("CLAUDE_MODEL", "claude-sonnet-4-20250514");
    public static final int CLAUDE_MAX_TOKENS =
            Integer.parseInt(env("CLAUDE_MAX_TOKENS", "4096"));

    // ── Tuning ───────────────────────────────────────────────────────────────
    public static final long POLL_TIMEOUT_MS =
            Long.parseLong(env("POLL_TIMEOUT_MS", "1000"));

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
