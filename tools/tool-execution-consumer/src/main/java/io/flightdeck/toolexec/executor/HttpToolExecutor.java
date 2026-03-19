package io.flightdeck.toolexec.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.flightdeck.toolexec.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Base executor that delegates tool execution to an external HTTP service.
 * Each tool maps to a specific REST endpoint.
 */
public class HttpToolExecutor implements ToolExecutor {

    private static final Logger log = LoggerFactory.getLogger(HttpToolExecutor.class);

    private final String name;
    private final String endpointUrl;
    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    public HttpToolExecutor(String name, String endpointUrl, ObjectMapper mapper) {
        this.name = name;
        this.endpointUrl = endpointUrl;
        this.mapper = mapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public String toolName() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> execute(Map<String, Object> input) throws Exception {
        String body = mapper.writeValueAsString(input);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpointUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(AppConfig.HTTP_TIMEOUT_SECONDS))
                .build();

        log.info("Calling {} at {}", name, endpointUrl);

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return mapper.readValue(response.body(), Map.class);
        } else {
            throw new RuntimeException(String.format(
                    "Tool %s returned HTTP %d: %s", name, response.statusCode(), response.body()));
        }
    }
}
