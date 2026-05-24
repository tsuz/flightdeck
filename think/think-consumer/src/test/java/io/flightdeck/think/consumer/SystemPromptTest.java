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

    @Test
    @DisplayName("System prompt containing special characters is preserved literally (issue #23)")
    void specialChars_inPrompt_doesNotThrow() {
        String basePrompt = "Apply a 10% discount when asked.";

        String withoutMemoir = ThinkConsumer.buildSystemPrompt(basePrompt, null);
        assertThat(withoutMemoir).contains("10% discount");

        String withMemoir = ThinkConsumer.buildSystemPrompt(basePrompt, "user likes 50% off coupons");
        assertThat(withMemoir)
                .contains("10% discount")
                .contains("50% off coupons");

        // Sequences that String.format would interpret as conversions/specifiers
        // must survive unchanged now that the prompt is concatenated, not formatted.
        assertThat(ThinkConsumer.buildSystemPrompt("Print %s and %d and %n literally.", null))
                .contains("%s and %d and %n literally.");
        assertThat(ThinkConsumer.buildSystemPrompt("A 100%% bonus", "memoir with %s token"))
                .contains("100%% bonus")
                .contains("memoir with %s token");

        // Other characters that are easy to mishandle in string plumbing.
        assertThat(ThinkConsumer.buildSystemPrompt("Use a backslash \\ and a dollar $ sign.", null))
                .contains("backslash \\ and a dollar $ sign.");
        assertThat(ThinkConsumer.buildSystemPrompt("Braces {0} {1} and brackets [x] stay.", null))
                .contains("Braces {0} {1} and brackets [x] stay.");
        assertThat(ThinkConsumer.buildSystemPrompt("Quotes \"q\" 'a' and emoji 🚀 stay.", null))
                .contains("Quotes \"q\" 'a' and emoji 🚀 stay.");
    }
}
