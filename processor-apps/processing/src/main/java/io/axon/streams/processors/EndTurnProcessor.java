package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MessageInput;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.model.UserResponse;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <h2>End Turn Processor</h2>
 *
 * <p>Implements the <em>"End turn to user response"</em> transform node in the
 * architecture diagram — the terminal step that converts a completed LLM turn
 * into a clean {@link UserResponse} delivered to the user-facing layer.
 *
 * <h3>Topology</h3>
 * <pre>
 *   think-request-response  (KStream)
 *         │
 *         ▼  filter: endTurn == true  AND  tool_uses is empty
 *         │
 *         ▼  mapValues: ThinkResponse → UserResponse
 *              ├─ Concatenate all assistant message content
 *              ├─ Carry session_id, user_id, token counts, cost
 *              └─ Tag source_agent for multi-agent traceability
 *         │
 *         ▼
 *   message-output  (consumed by the user-facing / API layer)
 * </pre>
 *
 * <h3>Filter logic</h3>
 * A {@link ThinkResponse} qualifies for end-turn delivery when <em>both</em>
 * conditions are true:
 * <ol>
 *   <li>{@code endTurn == true} — the LLM explicitly signalled it has finished.</li>
 *   <li>{@code tool_uses} is null or empty — there are no outstanding tool calls
 *       to be resolved first.  If tool calls are present the record is handled
 *       by {@code ExtractToolUseItemsProcessor} instead and must not be delivered
 *       to the user prematurely.</li>
 * </ol>
 *
 * <h3>Content assembly</h3>
 * A single {@link ThinkResponse} can carry multiple {@link MessageInput} entries
 * (e.g. a chain-of-thought block followed by the final answer).  Only messages
 * with {@code role == "assistant"} are concatenated into the outbound content;
 * internal reasoning or tool-result messages are stripped.
 */
public class EndTurnProcessor {

    private static final Logger log = LoggerFactory.getLogger(EndTurnProcessor.class);

    /** Role value that identifies LLM-generated content within a ThinkResponse. */
    static final String ROLE_ASSISTANT = "assistant";

    /** Separator used when concatenating multiple assistant message segments. */
    static final String CONTENT_SEPARATOR = "\n\n";

    /** Default source-agent tag when the ThinkResponse carries no agent identifier. */
    static final String DEFAULT_AGENT = "agent-1";

    public static void register(StreamsBuilder builder) {

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        thinkStream
                // ── Step 1: filter — only fully-resolved end-turn responses ──
                .filter((sessionId, response) -> {
                    if (response == null) return false;

                    boolean isEndTurn   = response.endTurn();
                    boolean noToolCalls = response.toolUses() == null
                                      || response.toolUses().isEmpty();

                    if (isEndTurn && !noToolCalls) {
                        log.warn("[{}] endTurn=true but {} tool_uses still present — skipping end-turn path",
                                sessionId, response.toolUses().size());
                    }

                    return isEndTurn && noToolCalls;
                })

                // ── Step 2: transform ThinkResponse → UserResponse ───────────
                .mapValues((sessionId, response) -> {
                    UserResponse userResponse = toUserResponse(sessionId, response);
                    log.info("[{}] End-turn → message-output  content_len={} cost=${}",
                            sessionId,
                            userResponse.content().length(),
                            String.format("%.6f", userResponse.cost()));
                    return userResponse;
                })

                // ── Step 3: publish to message-output ────────────────────────
                .to(Topics.MESSAGE_OUTPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(UserResponse.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Package-private transform — extracted for direct unit testing
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Transforms a {@link ThinkResponse} into a {@link UserResponse}.
     *
     * <p>Content assembly rules:
     * <ul>
     *   <li>Only messages with {@code role == "assistant"} contribute to content.</li>
     *   <li>Multiple assistant segments are joined with {@link #CONTENT_SEPARATOR}.</li>
     *   <li>If no assistant messages are present, content defaults to an empty string
     *       rather than failing — guards against malformed upstream payloads.</li>
     * </ul>
     */
    static UserResponse toUserResponse(String sessionId, ThinkResponse response) {
        String content = assembleContent(response.messages());

        String sourceAgent = (response.messages() != null)
                ? response.messages().stream()
                        .filter(m -> m.metadata() != null && m.metadata().containsKey("agent"))
                        .map(m -> String.valueOf(m.metadata().get("agent")))
                        .findFirst()
                        .orElse(DEFAULT_AGENT)
                : DEFAULT_AGENT;

        return new UserResponse(
                sessionId,
                response.userId(),
                content,
                1,                          // this response represents one LLM call
                response.inputTokens(),
                response.outputTokens(),
                response.cost(),
                sourceAgent,
                Instant.now().toString()
        );
    }

    /**
     * Concatenates all assistant-role message content from the list.
     * Returns an empty string if the list is null, empty, or contains no
     * assistant messages.
     */
    static String assembleContent(List<MessageInput> messages) {
        if (messages == null || messages.isEmpty()) return "";

        return messages.stream()
                .filter(m -> ROLE_ASSISTANT.equals(m.role()))
                .map(MessageInput::content)
                .filter(c -> c != null && !c.isBlank())
                .collect(Collectors.joining(CONTENT_SEPARATOR));
    }
}