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
     * Loaded once from TOOLS_JSON_FILE and cached.
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

        log.info("Loaded {} tool definitions from {}: {}",
                apiTools.size(),
                AppConfig.TOOLS_JSON_FILE,
                apiTools.stream().map(t -> (String) t.get("name")).toList());

        return apiTools;
    }

    private static List<Map<String, Object>> loadRawTools() {
        Path path = Path.of(AppConfig.TOOLS_JSON_FILE);
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
