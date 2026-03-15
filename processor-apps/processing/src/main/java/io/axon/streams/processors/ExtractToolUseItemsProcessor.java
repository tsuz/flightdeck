package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.model.ToolUseItem;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <h2>Extract Tool-Use Items Processor</h2>
 *
 * <p>Reads every {@code ThinkResponse} off {@code think-request-response}.
 * If the response contains one or more {@code tool_uses}:
 * <ol>
 *   <li>Filter out responses whose {@code tool_uses} list is null or empty
 *       (those go straight to {@code end-turn} / {@code message-output}).</li>
 *   <li>Fan-out: emit one {@link ToolUseItem} per entry onto {@code tool-use},
 *       keyed by {@code session_id} so partitioning stays consistent.</li>
 *   <li>Route malformed items (missing {@code tool_use_id} or {@code name})
 *       to the dead-letter queue {@code tool-use-dlq}.</li>
 * </ol>
 *
 * <p>Mirrors the "extract tool use items" beige node in the architecture diagram,
 * including the annotation:
 * <em>"filter if tool-use items is non empty, then split into each message"</em>.
 */
public class ExtractToolUseItemsProcessor {

    private static final Logger log = LoggerFactory.getLogger(ExtractToolUseItemsProcessor.class);

    /**
     * Registers the topology fragment onto the provided {@link StreamsBuilder}.
     * Call this from your main topology builder class.
     */
    public static void register(StreamsBuilder builder) {

        // ── Source: LLM responses ────────────────────────────────────────────
        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        // ── Step 1: filter — keep only responses that carry tool-use blocks ──
        KStream<String, ThinkResponse> withToolUses = thinkStream
                .filter((sessionId, response) -> {
                    if (response == null) return false;
                    List<ToolUseItem> items = response.toolUses();
                    boolean hasTools = items != null && !items.isEmpty();
                    if (!hasTools) {
                        log.debug("[{}] No tool-use items — skipping fan-out (end-turn path)", sessionId);
                    }
                    return hasTools;
                });

        // ── Step 2: flat-map — one ToolUseItem record per tool call ──────────
        //   Sets totalTools on each item from the list size so downstream
        //   aggregation knows how many results to expect.
        KStream<String, ToolUseItem> flatItems = withToolUses
                .flatMapValues((sessionId, response) -> {
                    List<ToolUseItem> items = response.toolUses();
                    int totalTools = items.size();
                    log.info("[{}] Fanning out {} tool-use item(s)", sessionId, totalTools);
                    return items.stream()
                            .map(item -> new ToolUseItem(
                                    item.toolUseId(),
                                    item.toolId(),
                                    item.name(),
                                    item.input(),
                                    item.sessionId(),
                                    totalTools,
                                    item.timestamp()))
                            .toList();
                });
                        
        // ── Step 3: branch — valid items vs dead-letter ───────────────────────
        //   Valid   → tool_use_id AND name are both present
        //   Invalid → missing either field; route to DLQ for inspection

        Map<String, KStream<String, ToolUseItem>> branches = flatItems
                .split(Named.as("tool-use-branch-"))
                .branch(
                        (sessionId, item) -> isValid(sessionId, item),
                        Branched.as("valid")
                )
                .defaultBranch(Branched.as("invalid"));

        KStream<String, ToolUseItem> validItems   = branches.get("tool-use-branch-valid");
        KStream<String, ToolUseItem> invalidItems = branches.get("tool-use-branch-invalid");

        // ── Sink: valid items → tool-use topic ───────────────────────────────
        //   Key = session_id  (preserves partition co-location with downstream
        //   aggregators that also key by session_id)
        validItems
                .peek((sessionId, item) ->
                        log.info("[{}] → tool-use  tool_use_id={} name={}",
                                sessionId, item.toolUseId(), item.name()))
                .to(Topics.TOOL_USE,
                        Produced.with(Serdes.String(), JsonSerde.of(ToolUseItem.class)));

        // ── Sink: invalid items → dead-letter queue ───────────────────────────
        invalidItems
                .peek((sessionId, item) ->
                        log.warn("[{}] → tool-use-dlq  malformed item: {}", sessionId, item))
                .to(Topics.TOOL_USE_DLQ,
                        Produced.with(Serdes.String(), JsonSerde.of(ToolUseItem.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * A {@link ToolUseItem} is valid when it carries both a {@code tool_use_id}
     * and a {@code name} — the minimum fields required for downstream tool
     * execution and result correlation.
     */
    private static boolean isValid(String sessionId, ToolUseItem item) {
        if (item == null) {
            log.warn("[{}] Null ToolUseItem — routing to DLQ", sessionId);
            return false;
        }
        boolean valid = item.toolUseId() != null && !item.toolUseId().isBlank()
                     && item.name()      != null && !item.name().isBlank();
        if (!valid) {
            log.warn("[{}] ToolUseItem missing tool_use_id or name — routing to DLQ: {}", sessionId, item);
        }
        return valid;
    }
}