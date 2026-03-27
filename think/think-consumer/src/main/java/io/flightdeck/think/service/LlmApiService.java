package io.flightdeck.think.service;

import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;

import java.util.List;
import java.util.Map;

/**
 * Abstraction over LLM provider APIs (Claude, Gemini, etc.).
 */
public interface LlmApiService {

    /**
     * Sends conversation history and tools to the LLM and returns a ThinkResponse.
     */
    ThinkResponse call(String systemPrompt,
                       List<Map<String, Object>> messages,
                       String sessionId,
                       String userId);

    /**
     * Converts internal MessageInput history into the provider-specific message format.
     */
    List<Map<String, Object>> toApiMessages(List<MessageInput> history, MessageInput latestInput);
}
