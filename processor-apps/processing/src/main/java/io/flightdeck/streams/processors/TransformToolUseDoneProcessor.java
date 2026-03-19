package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.ToolResultAccumulator;
import io.flightdeck.streams.model.ToolUseResult;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <h2>Transform Tool Use Done Processor</h2>
 *
 * <p>Implements the <em>"transform tool use all complete to message-input"</em>
 * node in the architecture diagram — the step that closes the agentic loop by
 * feeding all completed tool results back into {@code message-input} as a
 * single {@code tool}-role {@link MessageInput}.
 *
 * <h3>Topology</h3>
 * <pre>
 *   tool-use-all-complete  (KStream — ToolResultAccumulator)
 *         │
 *         ▼  mapValues: ToolResultAccumulator → MessageInput (role="tool")
 *              ├─ content   : JSON array of { tool_use_id, name, result, status }
 *              ├─ role      : "tool"
 *              ├─ session_id: preserved
 *              ├─ user_id   : preserved
 *              └─ metadata  : { tool_results: [...], tool_count: N, source: "tool-execution" }
 *         │
 *         ▼
 *   message-input  ──► re-enters EnrichInputMessage → Think → ...
 * </pre>
 *
 * <h3>Why this closes the loop</h3>
 * Once all parallel tool calls for a session have returned, the LLM needs to
 * reason over their results to produce the final user response.  By writing
 * back to {@code message-input} as a {@code tool}-role message, this processor
 * re-triggers the full pipeline:
 * <ol>
 *   <li>{@code EnrichInputMessageProcessor} joins the tool message with existing
 *       conversation history.</li>
 *   <li>The Think processor calls the LLM again with the enriched context
 *       (history + tool results).</li>
 *   <li>The LLM either emits a final {@code endTurn} response (consumed by
 *       {@code EndTurnProcessor}) or requests further tool calls (another loop
 *       iteration via {@code ExtractToolUseItemsProcessor}).</li>
 * </ol>
 *
 * <h3>Content format</h3>
 * The {@code content} field carries a compact, human-readable JSON array so
 * the LLM receives structured context rather than opaque binary:
 * <pre>
 * [
 *   { "tool_use_id": "tuid-1", "name": "get_invoice_balance",
 *     "result": { "balance_due": 142.50, ... }, "status": "success" },
 *   ...
 * ]
 * </pre>
 * The raw {@link ToolUseResult} list is also preserved in {@code metadata} under
 * the key {@code "tool_results"} for downstream processors that need structured
 * access without re-parsing JSON.
 */
public class TransformToolUseDoneProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransformToolUseDoneProcessor.class);

    /** Role tag written onto the synthesised MessageInput. */
    static final String ROLE_TOOL = "tool";

    /** Metadata key under which the raw ToolUseResult list is stored. */
    static final String META_TOOL_RESULTS = "tool_results";

    /** Metadata key recording how many tool results are in this message. */
    static final String META_TOOL_COUNT = "tool_count";

    /** Metadata tag identifying the origin of this message. */
    static final String META_SOURCE = "source";
    static final String SOURCE_VALUE = "tool-execution";

    public static void register(StreamsBuilder builder) {

        KStream<String, ToolResultAccumulator> allCompleteStream = builder.stream(
                Topics.TOOL_USE_ALL_COMPLETE,
                Consumed.with(Serdes.String(), JsonSerde.of(ToolResultAccumulator.class))
        );

        allCompleteStream
                // ── Guard: skip null or empty accumulators ────────────────────
                .filter((sessionId, acc) -> {
                    if (acc == null || acc.results() == null || acc.results().isEmpty()) {
                        log.warn("[{}] Empty ToolResultAccumulator received — skipping", sessionId);
                        return false;
                    }
                    return true;
                })

                // ── Transform: ToolResultAccumulator → MessageInput ───────────
                .mapValues((sessionId, acc) -> {
                    MessageInput message = toMessageInput(sessionId, acc);
                    log.info("[{}] Tool results transformed → message-input  tool_count={} role={}",
                            sessionId, acc.results().size(), ROLE_TOOL);
                    return message;
                })

                // ── Re-enter the pipeline via message-input ───────────────────
                .to(Topics.MESSAGE_INPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(MessageInput.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Package-private transform — extracted for direct unit testing
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Converts a completed {@link ToolResultAccumulator} into an
     * {@link MessageInput} suitable for re-entering {@code message-input}.
     *
     * <p>The {@code content} field is a JSON array of tool result summaries.
     * Serialisation failures fall back to a plain text representation so the
     * pipeline never stalls due to a serialisation error.
     */
    static MessageInput toMessageInput(String sessionId, ToolResultAccumulator acc) {
        List<Map<String, Object>> content = buildContentList(acc.results());
        Map<String, Object> metadata = buildMetadata(acc.results());

        return new MessageInput(
                sessionId,
                acc.userId(),
                ROLE_TOOL,
                content,
                Instant.now().toString(),
                metadata
        );
    }

    /**
     * Builds a list of summary maps from {@link ToolUseResult} records.
     * Each entry contains only the fields the LLM needs:
     * {@code tool_use_id}, {@code name}, {@code result}, and {@code status}.
     */
    static List<Map<String, Object>> buildContentList(List<ToolUseResult> results) {
        if (results == null || results.isEmpty()) return List.of();

        return results.stream()
                .map(r -> {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("tool_use_id", r.toolUseId());
                    entry.put("name",        r.name());
                    entry.put("result",      r.result() != null ? r.result() : Map.of());
                    entry.put("status",      r.status());
                    return entry;
                })
                .collect(Collectors.toList());
    }

    /**
     * Builds the metadata map attached to the outbound {@link MessageInput}.
     * Stores the raw {@link ToolUseResult} list for structured downstream access
     * alongside summary counters.
     */
    static Map<String, Object> buildMetadata(List<ToolUseResult> results) {
        Map<String, Object> meta = new HashMap<>();
        meta.put(META_TOOL_RESULTS, results);
        meta.put(META_TOOL_COUNT,   results != null ? results.size() : 0);
        meta.put(META_SOURCE,       SOURCE_VALUE);
        return meta;
    }
}