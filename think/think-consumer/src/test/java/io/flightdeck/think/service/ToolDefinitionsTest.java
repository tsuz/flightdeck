package io.flightdeck.think.service;

import io.flightdeck.think.config.AppConfig;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link ToolDefinitions} — loading tools from TOOLS_JSON_FILE.
 *
 * <p>The surefire plugin sets TOOLS_JSON_FILE to src/test/resources/test-tools.json.
 */
class ToolDefinitionsTest {

    @Test
    @DisplayName("TOOLS_JSON_FILE env var is set for tests")
    void toolsJsonFileIsSet() {
        assertThat(AppConfig.TOOLS_JSON_FILE).isNotBlank();
    }

    @Test
    @DisplayName("Loads tools from TOOLS_JSON_FILE")
    void loadsToolsFromFile() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        assertThat(tools).isNotEmpty();
    }

    @Test
    @DisplayName("Correct number of tools loaded")
    void correctToolCount() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        assertThat(tools).hasSize(2);
    }

    @Test
    @DisplayName("Tool names are extracted correctly")
    void toolNamesExtracted() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        List<String> names = tools.stream()
                .map(t -> (String) t.get("name"))
                .toList();
        assertThat(names).containsExactly("test_tool_a", "test_tool_b");
    }

    @Test
    @DisplayName("Tool descriptions are extracted correctly")
    void toolDescriptionsExtracted() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        assertThat(tools.get(0).get("description")).isEqualTo("A test tool for unit tests.");
        assertThat(tools.get(1).get("description")).isEqualTo("Another test tool.");
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("Tool input_schema is extracted correctly")
    void toolInputSchemaExtracted() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        Map<String, Object> schema = (Map<String, Object>) tools.get(0).get("input_schema");
        assertThat(schema).containsKey("type");
        assertThat(schema).containsKey("properties");
        assertThat(schema).containsKey("required");
        assertThat(schema.get("type")).isEqualTo("object");
    }

    @Test
    @DisplayName("Extra fields (prompt_context, category) are not included in API output")
    void extraFieldsExcluded() {
        List<Map<String, Object>> tools = ToolDefinitions.getTools();
        for (Map<String, Object> tool : tools) {
            assertThat(tool).containsOnlyKeys("name", "description", "input_schema");
        }
    }

    @Test
    @DisplayName("Results are cached — same instance returned on subsequent calls")
    void resultsCached() {
        List<Map<String, Object>> first = ToolDefinitions.getTools();
        List<Map<String, Object>> second = ToolDefinitions.getTools();
        assertThat(first).isSameAs(second);
    }
}
