package io.flightdeck.think.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads tool definitions from {@code tools.json} — the single source of truth
 * shared with the prompts/Qdrant seed pipeline.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>{@code TOOLS_JSON_PATH} env var (absolute path)</li>
 *   <li>{@code tools.json} on the classpath</li>
 * </ol>
 *
 * <p>Each entry in tools.json contains both the Claude API schema fields
 * ({@code name}, {@code description}, {@code input_schema}) and the RAG
 * prompt context ({@code prompt_context}, {@code category}). This class
 * extracts only the API-relevant fields.
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
            // Extract only fields needed by Claude API
            Map<String, Object> tool = new LinkedHashMap<>();
            tool.put("name", raw.get("name"));
            tool.put("description", raw.get("description"));
            tool.put("input_schema", raw.get("input_schema"));
            apiTools.add(tool);
        }

        log.info("Loaded {} tool definitions: {}",
                apiTools.size(),
                apiTools.stream().map(t -> (String) t.get("name")).toList());

        return apiTools;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> loadRawTools() {
        // 1. Try external file path from env
        String externalPath = System.getenv("TOOLS_JSON_PATH");
        if (externalPath != null && !externalPath.isBlank()) {
            Path path = Path.of(externalPath);
            if (Files.exists(path)) {
                try {
                    log.info("Loading tools from external path: {}", path);
                    return MAPPER.readValue(path.toFile(),
                            new TypeReference<List<Map<String, Object>>>() {});
                } catch (IOException e) {
                    log.error("Failed to load tools from {}: {}", path, e.getMessage());
                }
            }
        }

        // 2. Try classpath
        try (InputStream is = ToolDefinitions.class.getResourceAsStream("/tools.json")) {
            if (is != null) {
                log.info("Loading tools from classpath: tools.json");
                return MAPPER.readValue(is,
                        new TypeReference<List<Map<String, Object>>>() {});
            }
        } catch (IOException e) {
            log.error("Failed to load tools from classpath: {}", e.getMessage());
        }

        log.warn("No tools.json found — running with no tools");
        return List.of();
    }
}
