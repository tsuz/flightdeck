package io.flightdeck.think.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

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
}
