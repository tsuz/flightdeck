package io.flightdeck.toolexec.executor.mock;

import io.flightdeck.toolexec.executor.ToolExecutor;

import java.util.List;
import java.util.Map;

public class MockLookupContactsExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "lookup_contacts";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        String name = (String) input.getOrDefault("name", "Unknown");
        String email = (String) input.getOrDefault("email", "");

        String resolvedName = !name.isBlank() ? name : "John Doe";
        String resolvedEmail = !email.isBlank() ? email : resolvedName.toLowerCase().replace(" ", ".") + "@example.com";

        return Map.of(
                "contacts", List.of(
                        Map.of(
                                "name", resolvedName,
                                "email", resolvedEmail,
                                "phone", "+1-555-0123",
                                "department", "Engineering",
                                "title", "Software Engineer"
                        )
                ),
                "total_found", 1
        );
    }
}
