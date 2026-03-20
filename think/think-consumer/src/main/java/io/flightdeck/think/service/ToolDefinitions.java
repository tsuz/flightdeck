package io.flightdeck.think.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flightdeck.think.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads tool definitions from the file specified by {@code TOOLS_JSON_FILE} env var.
 *
 * <p>If {@code TOOLS_JSON_FILE} is not set, the agent runs with no tools.
 * If set but the file does not exist, startup fails with an error.
 *
 * <p>Each entry in the JSON file may contain both Claude API schema fields
 * ({@code name}, {@code description}, {@code input_schema}) and extra metadata
 * ({@code prompt_context}, {@code category}). This class extracts only the
 * API-relevant fields.
 */
public final class ToolDefinitions {

    private static final Logger log = LoggerFactory.getLogger(ToolDefinitions.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static List<Map<String, Object>> cachedTools;

    private ToolDefinitions() {}

    /**
     * Returns the list of tool definitions formatted for the Claude API.
     * Loaded once and cached.
     */
    public static synchronized List<Map<String, Object>> getTools() {
        if (cachedTools == null) {
            cachedTools = loadTools();
        }
        return cachedTools;
    }

    private static List<Map<String, Object>> loadTools() {
        List<Map<String, Object>> rawTools = loadRawTools();
        List<Map<String, Object>> apiTools = new ArrayList<>();

        for (Map<String, Object> raw : rawTools) {
            Map<String, Object> tool = new LinkedHashMap<>();
            tool.put("name", raw.get("name"));
            tool.put("description", raw.get("description"));
            tool.put("input_schema", raw.get("input_schema"));
            apiTools.add(tool);
        }

        if (apiTools.isEmpty()) {
            log.info("No tool definitions loaded — agent will run without tools");
        } else {
            log.info("Loaded {} tool definitions: {}",
                    apiTools.size(),
                    apiTools.stream().map(t -> (String) t.get("name")).toList());
        }

        return apiTools;
    }

    private static List<Map<String, Object>> loadRawTools() {
        String toolsFile = AppConfig.TOOLS_JSON_FILE;

        if (toolsFile == null || toolsFile.isBlank()) {
            log.info("TOOLS_JSON_FILE is not set — running with no tool definitions");
            return List.of();
        }

        Path path = Path.of(toolsFile);
        if (!Files.exists(path)) {
            throw new IllegalStateException(
                    "TOOLS_JSON_FILE not found: " + path.toAbsolutePath());
        }
        try {
            return MAPPER.readValue(path.toFile(),
                    new TypeReference<List<Map<String, Object>>>() {});
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to parse TOOLS_JSON_FILE: " + path.toAbsolutePath(), e);
        }
    }
}
