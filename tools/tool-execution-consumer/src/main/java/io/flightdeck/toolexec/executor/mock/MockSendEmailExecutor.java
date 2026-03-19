package io.flightdeck.toolexec.executor.mock;

import io.flightdeck.toolexec.executor.ToolExecutor;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class MockSendEmailExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "send_email";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        return Map.of(
                "message_id", UUID.randomUUID().toString(),
                "to", input.getOrDefault("to", ""),
                "subject", input.getOrDefault("subject", ""),
                "status", "sent",
                "sent_at", Instant.now().toString()
        );
    }
}
