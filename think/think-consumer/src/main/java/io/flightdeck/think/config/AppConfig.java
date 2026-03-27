package io.flightdeck.think.config;

/**
 * Environment-based configuration for the Think Consumer.
 * All values fall back to sensible defaults for local development.
 */
public final class AppConfig {

    private AppConfig() {}

    // ── Kafka ───────────────────────────────────────────────────────────────
    public static final String AGENT_NAME = requireEnv("AGENT_NAME");

    public static final String BOOTSTRAP_SERVERS =
            env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final String CONSUMER_GROUP =
            env("KAFKA_CONSUMER_GROUP", "think-consumer-group");

    public static final String INPUT_TOPIC =
            env("KAFKA_INPUT_TOPIC", AGENT_NAME + "-enriched-message-input");

    public static final String OUTPUT_TOPIC =
            env("KAFKA_OUTPUT_TOPIC", AGENT_NAME + "-think-request-response");

    // ── LLM Provider ────────────────────────────────────────────────────────
    public static final String LLM_PROVIDER =
            env("LLM_PROVIDER", "claude");

    // ── Claude API ──────────────────────────────────────────────────────────
    public static final String CLAUDE_API_KEY =
            env("CLAUDE_API_KEY", "");

    public static final String CLAUDE_API_URL =
            env("CLAUDE_API_URL", "https://api.anthropic.com/v1/messages");

    public static final String CLAUDE_MODEL =
            env("CLAUDE_MODEL", "claude-haiku-4-5-20251001");

    public static final int CLAUDE_MAX_TOKENS =
            Integer.parseInt(env("CLAUDE_MAX_TOKENS", "4096"));

    // ── Gemini API ──────────────────────────────────────────────────────────
    public static final String GEMINI_API_KEY =
            env("GEMINI_API_KEY", "");

    public static final String GEMINI_API_URL =
            env("GEMINI_API_URL", "https://generativelanguage.googleapis.com/v1beta");

    public static final String GEMINI_MODEL =
            env("GEMINI_MODEL", "gemini-2.5-flash");

    public static final int GEMINI_MAX_TOKENS =
            Integer.parseInt(env("GEMINI_MAX_TOKENS", "4096"));

    // ── System prompt ──────────────────────────────────────────────────────
    public static final String SYSTEM_PROMPT_FILE = env("SYSTEM_PROMPT_FILE", "");

    // ── Tools ─────────────────────────────────────────────────────────────
    public static final String TOOLS_JSON_FILE = env("TOOLS_JSON_FILE", "");

    // ── Budget ────────────────────────────────────────────────────────────
    public static final Double BUDGET_PRICE_PER_SESSION;

    static {
        String val = System.getenv("BUDGET_PRICE_PER_SESSION");
        BUDGET_PRICE_PER_SESSION = (val != null && !val.isBlank()) ? Double.parseDouble(val) : null;
    }

    // ── Consumer tuning ─────────────────────────────────────────────────────
    public static final long POLL_TIMEOUT_MS =
            Long.parseLong(env("POLL_TIMEOUT_MS", "1000"));

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable " + key + " is not set");
        }
        return value;
    }
}
