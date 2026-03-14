package io.axon.toolexec.executor.mock;

import io.axon.toolexec.executor.ToolExecutor;

import java.util.Map;
import java.util.UUID;

public class MockCreateTaskExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "create_task";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        return Map.of(
                "task_id", UUID.randomUUID().toString(),
                "title", input.getOrDefault("title", "Untitled Task"),
                "description", input.getOrDefault("description", ""),
                "due_date", input.getOrDefault("due_date", ""),
                "priority", input.getOrDefault("priority", "medium"),
                "status", "created"
        );
    }
}
