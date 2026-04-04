package io.flightdeck.think;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.flightdeck.think.config.AppConfig;
import io.flightdeck.think.model.ThinkResponse;
import io.flightdeck.think.service.ClaudeApiService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Standalone CLI runner for testing the Claude API call without Kafka.
 * <p>
 * Usage: {@code CLAUDE_API_KEY=sk-... java -cp target/think-consumer-0.1.0-SNAPSHOT.jar io.flightdeck.think.StandaloneRunner}
 * <p>
 * Or pass a prompt as args: {@code ... StandaloneRunner "What is the capital of France?"}
 */
public class StandaloneRunner {

    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        if (AppConfig.CLAUDE_API_KEY.isBlank()) {
            System.err.println("ERROR: CLAUDE_API_KEY environment variable is not set.");
            System.err.println("Usage: CLAUDE_API_KEY=sk-... java -cp target/think-consumer-0.1.0-SNAPSHOT.jar io.flightdeck.think.StandaloneRunner");
            System.exit(1);
        }

        System.out.println("Claude API Standalone Runner");
        System.out.println("  Model:         " + AppConfig.CLAUDE_MODEL);
        System.out.println("  Tools file:    " + AppConfig.TOOLS_JSON_FILE);
        System.out.println();

        ClaudeApiService claudeApi = new ClaudeApiService(mapper);

        // If args provided, use as prompt; otherwise interactive mode
        if (args.length > 0) {
            String prompt = String.join(" ", args);
            sendAndPrint(claudeApi, mapper, prompt);
        } else {
            interactiveMode(claudeApi, mapper);
        }
    }

    private static void interactiveMode(ClaudeApiService claudeApi, ObjectMapper mapper) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Interactive mode — type your prompt and press Enter. Type 'quit' to exit.");
        System.out.println();

        while (true) {
            System.out.print("> ");
            if (!scanner.hasNextLine()) break;

            String prompt = scanner.nextLine().trim();
            if (prompt.equalsIgnoreCase("quit") || prompt.equalsIgnoreCase("exit")) {
                break;
            }
            if (prompt.isEmpty()) continue;

            sendAndPrint(claudeApi, mapper, prompt);
            System.out.println();
        }

        System.out.println("Goodbye!");
    }

    private static void sendAndPrint(ClaudeApiService claudeApi, ObjectMapper mapper, String prompt) {
        try {
            System.out.println("Sending prompt to Claude...");
            System.out.println();

            String systemPrompt = "You are a helpful AI assistant. Be concise and helpful.";

            List<Map<String, Object>> messages = List.of(
                    new LinkedHashMap<>(Map.of("role", "user", "content", prompt))
            );

            ThinkResponse response = claudeApi.call(systemPrompt, messages, "standalone", "cli-user");

            // Print response content
            if (response.lastInputResponse() != null) {
                for (var msg : response.lastInputResponse()) {
                    System.out.println(msg.content());
                }
            }

            // Print metadata
            System.out.println();
            System.out.printf("--- tokens: %d in / %d out | cost: $%.6f | end_turn: %s ---%n",
                    response.thinkInputTokens(),
                    response.thinkOutputTokens(),
                    response.thinkCost(),
                    response.endTurn());

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }
}
