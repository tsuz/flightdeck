package io.flightdeck.think.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link ClaudeApiService#buildSystemBlocks} — verifies the PROMPT_CACHING
 * flag controls whether a {@code cache_control} breakpoint is attached to the system prompt.
 */
class ClaudeApiServiceTest {

    private static final String SYSTEM_PROMPT = "You are a helpful assistant.";

    @Test
    @DisplayName("PROMPT_CACHING=true attaches an ephemeral cache_control breakpoint")
    void cachingEnabledAddsCacheControl() {
        List<Map<String, Object>> blocks = ClaudeApiService.buildSystemBlocks(SYSTEM_PROMPT, true);

        assertThat(blocks).hasSize(1);
        Map<String, Object> block = blocks.get(0);
        assertThat(block)
                .containsEntry("type", "text")
                .containsEntry("text", SYSTEM_PROMPT)
                .containsKey("cache_control");
        assertThat(block.get("cache_control")).isEqualTo(Map.of("type", "ephemeral"));
    }

    @Test
    @DisplayName("PROMPT_CACHING=false omits cache_control entirely")
    void cachingDisabledOmitsCacheControl() {
        List<Map<String, Object>> blocks = ClaudeApiService.buildSystemBlocks(SYSTEM_PROMPT, false);

        assertThat(blocks).hasSize(1);
        Map<String, Object> block = blocks.get(0);
        assertThat(block)
                .containsEntry("type", "text")
                .containsEntry("text", SYSTEM_PROMPT)
                .doesNotContainKey("cache_control");
    }

    @Test
    @DisplayName("Serialized JSON includes cache_control when enabled, excludes it when disabled")
    void serializedJsonReflectsFlag() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String enabled = mapper.writeValueAsString(
                ClaudeApiService.buildSystemBlocks(SYSTEM_PROMPT, true));
        assertThat(enabled)
                .contains("\"cache_control\"")
                .contains("\"ephemeral\"");

        String disabled = mapper.writeValueAsString(
                ClaudeApiService.buildSystemBlocks(SYSTEM_PROMPT, false));
        assertThat(disabled).doesNotContain("cache_control");
    }

    // ── thinking / effort ────────────────────────────────────────────────────

    @Test
    @DisplayName("blank CLAUDE_THINKING / CLAUDE_EFFORT leave the request unchanged")
    void blankReasoningConfigOmitsFields() {
        Map<String, Object> body = new LinkedHashMap<>();
        ClaudeApiService.applyReasoningConfig(body, "", "");
        assertThat(body).doesNotContainKeys("thinking", "output_config");
    }

    @Test
    @DisplayName("adaptive thinking + effort are added to the request")
    void adaptiveThinkingAndEffortAdded() {
        Map<String, Object> body = new LinkedHashMap<>();
        ClaudeApiService.applyReasoningConfig(body, "adaptive", "high");
        assertThat(body.get("thinking")).isEqualTo(Map.of("type", "adaptive"));
        assertThat(body.get("output_config")).isEqualTo(Map.of("effort", "high"));
    }

    @Test
    @DisplayName("thinking blocks in a tool_use response are kept verbatim, in order, and replayed on the next call")
    @SuppressWarnings("unchecked")
    void thinkingBlocksSurviveToolUseRoundTrip() throws Exception {
        String response = """
                {"stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":20},
                 "content":[
                   {"type":"thinking","thinking":"","signature":"SIG123"},
                   {"type":"text","text":"Let me escalate."},
                   {"type":"tool_use","id":"toolu_1","name":"escalate_to_human","input":{"reason":"refund"}}
                 ]}""";
        ClaudeApiService svc = new ClaudeApiService(new ObjectMapper());
        ThinkResponse r = svc.parseResponse(response, "s1", "u1");

        assertThat(r.endTurn()).isFalse();
        MessageInput assistant = r.lastInputResponse().get(0);
        List<Map<String, Object>> blocks = (List<Map<String, Object>>) assistant.content();
        assertThat(blocks).extracting(b -> b.get("type"))
                .containsExactly("thinking", "text", "tool_use");
        assertThat(blocks.get(0)).containsEntry("signature", "SIG123");

        // Next request: history contains the assistant turn (with thinking) + tool result.
        MessageInput toolResult = new MessageInput("s1", "u1", "tool",
                List.of(Map.of("tool_use_id", "toolu_1", "result", Map.of("status", "escalated"))),
                "2026-01-01T00:00:00Z", null);
        List<Map<String, Object>> msgs = ClaudeApiService.toClaudeMessages(List.of(assistant), toolResult);
        JsonNode json = new ObjectMapper().valueToTree(msgs);
        assertThat(json.get(0).get("content").get(0).get("type").asText()).isEqualTo("thinking");
        assertThat(json.get(0).get("content").get(0).get("signature").asText()).isEqualTo("SIG123");
        assertThat(json.get(1).get("content").get(0).get("type").asText()).isEqualTo("tool_result");
    }

    @Test
    @DisplayName("text-only responses with thinking store only the text (thinking is not persisted)")
    @SuppressWarnings("unchecked")
    void textOnlyResponseDropsThinking() {
        String response = """
                {"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1},
                 "content":[{"type":"thinking","thinking":"","signature":"S"},{"type":"text","text":"Hi!"}]}""";
        ThinkResponse r = new ClaudeApiService(new ObjectMapper()).parseResponse(response, "s1", "u1");
        assertThat(r.lastInputResponse()).hasSize(1);
        assertThat(r.lastInputResponse().get(0).content()).isEqualTo("Hi!");
    }

    @Test
    @DisplayName("compaction summary input omits thinking blocks")
    void summaryOmitsThinking() {
        String out = ClaudeApiService.contentForSummary(List.of(
                Map.of("type", "thinking", "thinking", "", "signature", "SIGSIGSIG"),
                Map.of("type", "text", "text", "hello")));
        assertThat(out).contains("hello").doesNotContain("SIGSIGSIG");
    }
}
