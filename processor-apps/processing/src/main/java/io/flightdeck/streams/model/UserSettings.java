package io.flightdeck.streams.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-user settings stored in the compacted {@code user-settings} topic,
 * keyed by user_id. Joined into {@link FullSessionContext} by the
 * {@code EnrichInputMessageProcessor} when {@code ENABLE_USER_SETTING=true}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record UserSettings(
        @JsonProperty("system_prompt") String systemPrompt,
        @JsonProperty("updated_at")    String updatedAt
) {}
