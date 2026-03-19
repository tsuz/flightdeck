package io.flightdeck.toolexec.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.flightdeck.toolexec.config.AppConfig;
import io.flightdeck.toolexec.executor.mock.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry that maps tool names to their executor implementations.
 * Uses mock executors when {@code MOCK_MODE=true} (default),
 * or HTTP executors that call external services when {@code MOCK_MODE=false}.
 */
public class ToolExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ToolExecutorRegistry.class);

    private final Map<String, ToolExecutor> executors = new HashMap<>();

    public ToolExecutorRegistry(ObjectMapper mapper) {
        if (AppConfig.MOCK_MODE) {
            log.info("Running in MOCK mode");
            register(new MockWebSearchExecutor());
            register(new MockLookupContactsExecutor());
            register(new MockScheduleMeetingExecutor());
            register(new MockSendEmailExecutor());
            register(new MockCreateTaskExecutor());
        } else {
            log.info("Running in HTTP mode");
            register(new HttpToolExecutor("web_search", AppConfig.WEB_SEARCH_URL, mapper));
            register(new HttpToolExecutor("lookup_contacts", AppConfig.CONTACTS_URL, mapper));
            register(new HttpToolExecutor("schedule_meeting", AppConfig.CALENDAR_URL, mapper));
            register(new HttpToolExecutor("send_email", AppConfig.EMAIL_URL, mapper));
            register(new HttpToolExecutor("create_task", AppConfig.TASKS_URL, mapper));
        }

        log.info("Registered {} tool executors: {}", executors.size(), executors.keySet());
    }

    public void register(ToolExecutor executor) {
        executors.put(executor.toolName(), executor);
    }

    public ToolExecutor get(String toolName) {
        return executors.get(toolName);
    }

    public boolean has(String toolName) {
        return executors.containsKey(toolName);
    }
}
