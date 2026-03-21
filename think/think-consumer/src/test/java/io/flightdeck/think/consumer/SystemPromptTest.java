package io.flightdeck.think.consumer;

import org.junit.jupiter.api.*;


import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SYSTEM_PROMPT_FILE loading logic.
 */
class SystemPromptTest {

    @Test
    @DisplayName("Returns default prompt when file path is null or blank")
    void nullFile_returnsDefault() {
        String result = ThinkConsumer.loadSystemPromptFromFile(null);
        assertThat(result).contains("intelligent AI assistant");

        String result2 = ThinkConsumer.loadSystemPromptFromFile("  ");
        assertThat(result2).contains("intelligent AI assistant");
    }

    @Test
    @DisplayName("Loads custom prompt from file")
    void customFile_loadsContent() {
        String path = "src/test/resources/test-system-prompt.txt";
        String result = ThinkConsumer.loadSystemPromptFromFile(path);
        assertThat(result).isEqualTo("You are a Kafka operations assistant. Always use your tools.");
        assertThat(result).doesNotContain("intelligent AI assistant");
    }

    @Test
    @DisplayName("Throws when file is specified but does not exist")
    void missingFile_throws() {
        assertThatThrownBy(() -> ThinkConsumer.loadSystemPromptFromFile("/nonexistent/prompt.txt"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SYSTEM_PROMPT_FILE not found");
    }
}
