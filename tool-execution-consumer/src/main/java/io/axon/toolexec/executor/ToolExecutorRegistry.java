package io.axon.toolexec.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axon.toolexec.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry that maps tool names to their executor implementations.
 * Looks up the correct executor for each incoming {@code ToolUseItem}.
 */
public class ToolExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ToolExecutorRegistry.class);

    private final Map<String, ToolExecutor> executors = new HashMap<>();

    public ToolExecutorRegistry(ObjectMapper mapper) {
        register(new HttpToolExecutor("web_search", AppConfig.WEB_SEARCH_URL, mapper));
        register(new HttpToolExecutor("lookup_contacts", AppConfig.CONTACTS_URL, mapper));
        register(new HttpToolExecutor("schedule_meeting", AppConfig.CALENDAR_URL, mapper));
        register(new HttpToolExecutor("send_email", AppConfig.EMAIL_URL, mapper));
        register(new HttpToolExecutor("create_task", AppConfig.TASKS_URL, mapper));

        log.info("Registered {} tool executors: {}", executors.size(), executors.keySet());
    }

    /**
     * Register a custom executor (useful for testing or adding new tools).
     */
    public void register(ToolExecutor executor) {
        executors.put(executor.toolName(), executor);
    }

    /**
     * Returns the executor for the given tool name, or null if not found.
     */
    public ToolExecutor get(String toolName) {
        return executors.get(toolName);
    }

    public boolean has(String toolName) {
        return executors.containsKey(toolName);
    }
}
