package io.flightdeck.toolexec.executor;

import java.util.Map;

/**
 * Contract for executing a single tool function.
 * Each tool (web_search, send_email, etc.) provides its own implementation.
 */
public interface ToolExecutor {

    /**
     * @return the tool name this executor handles (must match {@code ToolUseItem.name()})
     */
    String toolName();

    /**
     * Executes the tool with the given input parameters.
     *
     * @param input the tool input arguments from the LLM
     * @return a result map to be serialized into the {@code ToolUseResult.result} field
     * @throws Exception on execution failure
     */
    Map<String, Object> execute(Map<String, Object> input) throws Exception;
}
