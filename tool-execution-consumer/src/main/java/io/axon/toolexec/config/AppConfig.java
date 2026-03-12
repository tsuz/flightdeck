package io.axon.toolexec.config;

/**
 * Environment-based configuration for the Tool Execution Consumer.
 */
public final class AppConfig {

    private AppConfig() {}

    // ── Kafka ───────────────────────────────────────────────────────────────
    public static final String BOOTSTRAP_SERVERS =
            env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final String CONSUMER_GROUP =
            env("KAFKA_CONSUMER_GROUP", "tool-execution-consumer-group");

    public static final String INPUT_TOPIC =
            env("KAFKA_INPUT_TOPIC", "tool-use");

    public static final String OUTPUT_TOPIC =
            env("KAFKA_OUTPUT_TOPIC", "tool-use-result");

    // ── Tool service endpoints ──────────────────────────────────────────────
    public static final String WEB_SEARCH_URL =
            env("WEB_SEARCH_URL", "http://localhost:8082/api/tools/web-search");

    public static final String CONTACTS_URL =
            env("CONTACTS_URL", "http://localhost:8082/api/tools/contacts");

    public static final String CALENDAR_URL =
            env("CALENDAR_URL", "http://localhost:8082/api/tools/calendar");

    public static final String EMAIL_URL =
            env("EMAIL_URL", "http://localhost:8082/api/tools/email");

    public static final String TASKS_URL =
            env("TASKS_URL", "http://localhost:8082/api/tools/tasks");

    // ── Consumer tuning ─────────────────────────────────────────────────────
    public static final long POLL_TIMEOUT_MS =
            Long.parseLong(env("POLL_TIMEOUT_MS", "1000"));

    public static final int HTTP_TIMEOUT_SECONDS =
            Integer.parseInt(env("HTTP_TIMEOUT_SECONDS", "30"));

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
