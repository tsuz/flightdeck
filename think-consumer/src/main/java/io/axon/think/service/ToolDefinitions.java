package io.axon.think.service;

import java.util.List;
import java.util.Map;

/**
 * Defines the tools available to the Claude agent.
 * Each tool is described as a JSON-compatible map matching the Anthropic tool schema.
 */
public final class ToolDefinitions {

    private ToolDefinitions() {}

    /**
     * Returns the list of tool definitions to include in Claude API requests.
     * Add or remove tools here as the agent's capabilities evolve.
     */
    public static List<Map<String, Object>> getTools() {
        return List.of(
                webSearchTool(),
                lookupContactsTool(),
                scheduleMeetingTool(),
                sendEmailTool(),
                createTaskTool()
        );
    }

    private static Map<String, Object> webSearchTool() {
        return Map.of(
                "name", "web_search",
                "description", "Search the web for current information on a topic.",
                "input_schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "query", Map.of(
                                        "type", "string",
                                        "description", "The search query"
                                )
                        ),
                        "required", List.of("query")
                )
        );
    }

    private static Map<String, Object> lookupContactsTool() {
        return Map.of(
                "name", "lookup_contacts",
                "description", "Look up contact information for a person by name or email.",
                "input_schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "name", Map.of(
                                        "type", "string",
                                        "description", "The name of the person to look up"
                                ),
                                "email", Map.of(
                                        "type", "string",
                                        "description", "The email of the person to look up"
                                )
                        ),
                        "required", List.of()
                )
        );
    }

    private static Map<String, Object> scheduleMeetingTool() {
        return Map.of(
                "name", "schedule_meeting",
                "description", "Schedule a meeting with participants at a given time.",
                "input_schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "title", Map.of(
                                        "type", "string",
                                        "description", "Meeting title"
                                ),
                                "participants", Map.of(
                                        "type", "array",
                                        "items", Map.of("type", "string"),
                                        "description", "List of participant emails"
                                ),
                                "start_time", Map.of(
                                        "type", "string",
                                        "description", "ISO 8601 start time"
                                ),
                                "duration_minutes", Map.of(
                                        "type", "integer",
                                        "description", "Duration in minutes"
                                )
                        ),
                        "required", List.of("title", "participants", "start_time", "duration_minutes")
                )
        );
    }

    private static Map<String, Object> sendEmailTool() {
        return Map.of(
                "name", "send_email",
                "description", "Send an email to a recipient.",
                "input_schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "to", Map.of(
                                        "type", "string",
                                        "description", "Recipient email address"
                                ),
                                "subject", Map.of(
                                        "type", "string",
                                        "description", "Email subject line"
                                ),
                                "body", Map.of(
                                        "type", "string",
                                        "description", "Email body content"
                                )
                        ),
                        "required", List.of("to", "subject", "body")
                )
        );
    }

    private static Map<String, Object> createTaskTool() {
        return Map.of(
                "name", "create_task",
                "description", "Create a task or to-do item.",
                "input_schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "title", Map.of(
                                        "type", "string",
                                        "description", "Task title"
                                ),
                                "description", Map.of(
                                        "type", "string",
                                        "description", "Task description"
                                ),
                                "due_date", Map.of(
                                        "type", "string",
                                        "description", "Due date in ISO 8601 format"
                                ),
                                "priority", Map.of(
                                        "type", "string",
                                        "enum", List.of("low", "medium", "high"),
                                        "description", "Task priority"
                                )
                        ),
                        "required", List.of("title")
                )
        );
    }
}
