package io.flightdeck.think.service;

import io.flightdeck.think.model.MessageInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A user message whose content is a block list (image url reference + text) must
 * reach Claude untouched and be flattened to a placeholder for Gemini, never
 * falling back to List.toString(). Compaction flattening is covered in
 * io.flightdeck.think.consumer.CompactionImageBlocksTest.
 */
class ImageContentBlocksTest {

    private static final List<Map<String, Object>> IMAGE_AND_TEXT = List.of(
            Map.of("type", "image", "source", Map.of("type", "url", "url", "https://storage.example.com/shot.png")),
            Map.of("type", "text", "text", "this is what I see when I press Start"));

    private static MessageInput msg(String role, Object content) {
        return new MessageInput("s1", "u1", role, content, "2026-10-10T00:00:00Z", null);
    }

    @Test
    @DisplayName("Claude: image+text block list is passed through as the user message content")
    void claudePassesBlocksThrough() {
        List<MessageInput> history = List.of(
                msg("user", "hello"),
                msg("assistant", "Hi! What happens when you press Start?"));
        List<Map<String, Object>> messages =
                ClaudeApiService.toClaudeMessages(history, msg("user", IMAGE_AND_TEXT));

        assertThat(messages).hasSize(3);
        Map<String, Object> last = messages.get(2);
        assertThat(last).containsEntry("role", "user");
        assertThat(last.get("content")).isEqualTo(IMAGE_AND_TEXT);
    }

    @Test
    @DisplayName("Claude: an image turn that lands in history is replayed as blocks on later turns")
    void claudeReplaysBlocksFromHistory() {
        List<MessageInput> history = List.of(
                msg("user", IMAGE_AND_TEXT),
                msg("assistant", "I can see the permission error."));
        List<Map<String, Object>> messages =
                ClaudeApiService.toClaudeMessages(history, msg("user", "so what do I do?"));

        assertThat(messages).hasSize(3);
        assertThat(messages.get(0).get("content")).isEqualTo(IMAGE_AND_TEXT);
        assertThat(messages.get(2).get("content")).isEqualTo("so what do I do?");
    }

    @Test
    @DisplayName("Dead image references are swapped for text placeholders on retry, nothing else changes")
    void stripImageBlocksReplacesOnlyImages() {
        List<Map<String, Object>> messages = ClaudeApiService.toClaudeMessages(
                List.of(msg("user", IMAGE_AND_TEXT), msg("assistant", "I see it.")),
                msg("user", "and now?"));
        assertThat(ClaudeApiService.hasImageBlocks(messages)).isTrue();

        List<Map<String, Object>> stripped = ClaudeApiService.stripImageBlocks(messages);
        assertThat(ClaudeApiService.hasImageBlocks(stripped)).isFalse();
        assertThat(stripped.get(0).get("content")).isEqualTo(List.of(
                Map.of("type", "text", "text", ClaudeApiService.IMAGE_UNAVAILABLE),
                Map.of("type", "text", "text", "this is what I see when I press Start")));
        assertThat(stripped.get(1).get("content")).isEqualTo("I see it.");
        assertThat(stripped.get(2).get("content")).isEqualTo("and now?");
        // original untouched
        assertThat(ClaudeApiService.hasImageBlocks(messages)).isTrue();
    }

    @Test
    @DisplayName("Gemini flattening emits a placeholder with the reference instead of List.toString()")
    void geminiFlattensImageBlocks() {
        String text = GeminiApiService.flattenUserBlocks(IMAGE_AND_TEXT);
        assertThat(text)
                .contains("[image attached: https://storage.example.com/shot.png]")
                .contains("this is what I see when I press Start")
                .doesNotContain("{type=");
    }
}
