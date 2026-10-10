package io.flightdeck.think.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live round trip against the real Claude API (spends a few cents). Skipped unless
 * RUN_LIVE_CLAUDE_TESTS=true. Needs CLAUDE_API_KEY, and CLAUDE_MODEL / CLAUDE_THINKING /
 * CLAUDE_EFFORT set the way the deployment sets them, e.g.:
 *
 * <pre>
 * RUN_LIVE_CLAUDE_TESTS=true CLAUDE_API_KEY=... CLAUDE_MODEL=claude-haiku-5-5 \
 *   CLAUDE_THINKING=adaptive CLAUDE_EFFORT=high mvn test -Dtest=ClaudeApiThinkingLiveTest
 * </pre>
 *
 * Verifies the multi-turn tool loop: call 1 returns tool_use (+ thinking), the stored
 * assistant turn is replayed with the tool result, and call 2 is accepted and ends the turn.
 */
@EnabledIfEnvironmentVariable(named = "RUN_LIVE_CLAUDE_TESTS", matches = "true")
class ClaudeApiThinkingLiveTest {

    @Test
    @DisplayName("tool_use turn followed by tool_result turn is accepted with thinking configured")
    @SuppressWarnings("unchecked")
    void toolLoopRoundTrip() {
        ClaudeApiService svc = new ClaudeApiService(new ObjectMapper());
        String system = "You are a test agent. Always use the provided tools when asked.";
        MessageInput user = new MessageInput("live", "u", "user",
                "Work out how many days there are between 2024-02-17 and 2025-03-09 (inclusive of the start date, "
                        + "exclusive of the end), double-check the leap day, then call test_tool_a with param1 set to "
                        + "that number as a string, and finally tell me what the tool returned.",
                "2026-01-01T00:00:00Z", null);

        List<Map<String, Object>> msgs1 = ClaudeApiService.toClaudeMessages(List.of(), user);
        ThinkResponse r1 = svc.call(system, msgs1, "live", "u");
        assertThat(r1.endTurn()).as("first call should stop for tool_use").isFalse();
        assertThat(r1.toolUses()).isNotEmpty();

        MessageInput assistant = r1.lastInputResponse().get(0);
        List<Map<String, Object>> blocks = (List<Map<String, Object>>) assistant.content();
        boolean thinking = blocks.stream().anyMatch(b -> "thinking".equals(b.get("type"))
                || "redacted_thinking".equals(b.get("type")));
        System.out.println("LIVE: model=" + io.flightdeck.think.config.AppConfig.CLAUDE_MODEL
                + " thinking=" + io.flightdeck.think.config.AppConfig.CLAUDE_THINKING
                + " effort=" + io.flightdeck.think.config.AppConfig.CLAUDE_EFFORT
                + " thinkingBlockReturned=" + thinking
                + " blockTypes=" + blocks.stream().map(b -> b.get("type")).toList());

        String toolUseId = r1.toolUses().get(0).toolUseId();
        MessageInput toolResult = new MessageInput("live", "u", "tool",
                List.of(Map.of("tool_use_id", toolUseId, "result", Map.of("echo", "hello-from-tool"))),
                "2026-01-01T00:00:01Z", null);

        List<Map<String, Object>> msgs2 = ClaudeApiService.toClaudeMessages(List.of(user, assistant), toolResult);
        ThinkResponse r2 = svc.call(system, msgs2, "live", "u");   // 400s if thinking blocks are mishandled

        assertThat(r2.endTurn()).isTrue();
        assertThat(r2.lastInputResponse()).isNotEmpty();
        assertThat(r2.lastInputResponse().get(0).contentAsString()).isNotBlank();
        System.out.println("LIVE: final reply=" + r2.lastInputResponse().get(0).contentAsString());
    }

    /** Diagnostic control: what the API does if the thinking block is dropped on replay. */
    @Test
    @DisplayName("control: replay WITHOUT the thinking block (prints the outcome, no assertion)")
    @SuppressWarnings("unchecked")
    void controlReplayWithoutThinking() {
        ClaudeApiService svc = new ClaudeApiService(new ObjectMapper());
        String system = "You are a test agent. Always use the provided tools when asked.";
        MessageInput user = new MessageInput("live", "u", "user",
                "Work out how many days there are between 2024-02-17 and 2025-03-09 (inclusive of the start date, "
                        + "exclusive of the end), double-check the leap day, then call test_tool_a with param1 set to "
                        + "that number as a string, and finally tell me what the tool returned.",
                "2026-01-01T00:00:00Z", null);
        ThinkResponse r1 = svc.call(system, ClaudeApiService.toClaudeMessages(List.of(), user), "live", "u");
        List<Map<String, Object>> blocks = (List<Map<String, Object>>) r1.lastInputResponse().get(0).content();
        List<Map<String, Object>> stripped = blocks.stream()
                .filter(b -> !"thinking".equals(b.get("type")) && !"redacted_thinking".equals(b.get("type"))).toList();
        MessageInput assistant = new MessageInput("live", "u", "assistant", stripped, "2026-01-01T00:00:00Z", null);
        MessageInput toolResult = new MessageInput("live", "u", "tool",
                List.of(Map.of("tool_use_id", r1.toolUses().get(0).toolUseId(), "result", Map.of("echo", "ok"))),
                "2026-01-01T00:00:01Z", null);
        try {
            svc.call(system, ClaudeApiService.toClaudeMessages(List.of(user, assistant), toolResult), "live", "u");
            System.out.println("CONTROL: replay without thinking block was ACCEPTED");
        } catch (RuntimeException e) {
            System.out.println("CONTROL: replay without thinking block was REJECTED: " + e.getMessage());
        }
    }
}
