package io.axon.toolexec.executor.mock;

import io.axon.toolexec.executor.ToolExecutor;

import java.util.List;
import java.util.Map;

public class MockWebSearchExecutor implements ToolExecutor {

    @Override
    public String toolName() {
        return "web_search";
    }

    @Override
    public Map<String, Object> execute(Map<String, Object> input) {
        String query = (String) input.getOrDefault("query", "");
        return Map.of(
                "results", List.of(
                        Map.of(
                                "title", "Search result for: " + query,
                                "url", "https://example.com/result1",
                                "snippet", "This is a mock search result for the query: " + query
                        ),
                        Map.of(
                                "title", "Another result for: " + query,
                                "url", "https://example.com/result2",
                                "snippet", "Additional information related to: " + query
                        )
                ),
                "total_results", 2
        );
    }
}
