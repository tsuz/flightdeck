package io.flightdeck.think.consumer;

import io.flightdeck.think.model.MessageInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Compaction flattens a user image+text block list without losing either part. */
class CompactionImageBlocksTest {

    private static final List<Map<String, Object>> IMAGE_AND_TEXT = List.of(
            Map.of("type", "image", "source", Map.of("type", "url", "url", "https://storage.example.com/shot.png")),
            Map.of("type", "text", "text", "this is what I see when I press Start"));

    private static MessageInput msg(String role, Object content) {
        return new MessageInput("s1", "u1", role, content, "2026-10-10T00:00:00Z", null);
    }

    @Test
    @DisplayName("Compaction flattening keeps the text and records the image as a placeholder")
    void compactionFlattensImageToPlaceholder() {
        List<Map<String, Object>> plain = ThinkConsumer.toPlainTextMessages(List.of(
                msg("user", IMAGE_AND_TEXT),
                msg("assistant", "Looks like a mic permission issue.")));

        assertThat(plain).hasSize(2);
        String userText = (String) plain.get(0).get("content");
        assertThat(userText).contains("[image attached]")
                .contains("this is what I see when I press Start")
                .doesNotContain("source=");
    }

}
