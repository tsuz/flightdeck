package io.flightdeck.toolexec.executor.mock;

import io.flightdeck.toolexec.executor.ToolExecutor;

import java.util.Map;
import java.util.UUID;

public class MockScheduleMeetingExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "schedule_meeting";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        return Map.of(
                "meeting_id", UUID.randomUUID().toString(),
                "title", input.getOrDefault("title", "Untitled Meeting"),
                "participants", input.getOrDefault("participants", java.util.List.of()),
                "start_time", input.getOrDefault("start_time", ""),
                "duration_minutes", input.getOrDefault("duration_minutes", 30),
                "status", "scheduled",
                "calendar_link", "https://calendar.example.com/meeting/" + UUID.randomUUID().toString().substring(0, 8)
        );
    }
}
