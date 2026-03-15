package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.ToolResultAccumulator;
import io.axon.streams.model.ToolUseResult;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <h2>Aggregate Tool Execution Result Processor</h2>
 *
 * <p>Reads from {@code tool-use-result} and accumulates results per session.
 * Each {@link ToolUseResult} carries {@code total_tools} indicating how many
 * parallel tool calls were requested. When the accumulated count equals
 * {@code total_tools}, the complete {@link ToolResultAccumulator} is emitted
 * to {@code tool-use-all-complete}.
 *
 * <h3>Topology</h3>
 * <pre>
 *   tool-use-result ──► [accumulate per session_id]
 *                              │
 *                              │  results.size() == total_tools?
 *                              │       YES ──► emit + reset store
 *                              │
 *                              ▼
 *                      tool-use-all-complete
 * </pre>
 */
public class AggregateToolExecutionResultProcessor {

    private static final Logger log = LoggerFactory.getLogger(AggregateToolExecutionResultProcessor.class);

    /** RocksDB store: session_id → {@link ToolResultAccumulator}. */
    public static final String ACCUMULATOR_STORE = "tool-result-accumulator-store";

    public static void register(StreamsBuilder builder) {

        StoreBuilder<KeyValueStore<String, ToolResultAccumulator>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ACCUMULATOR_STORE),
                        Serdes.String(),
                        JsonSerde.of(ToolResultAccumulator.class)
                );
        builder.addStateStore(storeBuilder);

        KStream<String, ToolUseResult> resultStream = builder.stream(
                Topics.TOOL_USE_RESULT,
                Consumed.with(Serdes.String(), JsonSerde.of(ToolUseResult.class))
        );

        resultStream
                .process(
                        (ProcessorSupplier<String, ToolUseResult,
                                           String, ToolResultAccumulator>) ResultAccumulatorProcessor::new,
                        ACCUMULATOR_STORE
                )
                .peek((sessionId, acc) ->
                        log.info("[{}] → tool-use-all-complete  results={} expected={}",
                                sessionId,
                                acc.results().size(),
                                acc.expectedCount()))
                .to(Topics.TOOL_USE_ALL_COMPLETE,
                        Produced.with(Serdes.String(), JsonSerde.of(ToolResultAccumulator.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Inner Processor
    // ─────────────────────────────────────────────────────────────────────────

    static class ResultAccumulatorProcessor
            implements Processor<String, ToolUseResult, String, ToolResultAccumulator> {

        private ProcessorContext<String, ToolResultAccumulator> context;
        private KeyValueStore<String, ToolResultAccumulator> accumulatorStore;

        @Override
        public void init(ProcessorContext<String, ToolResultAccumulator> ctx) {
            this.context = ctx;
            this.accumulatorStore = ctx.getStateStore(ACCUMULATOR_STORE);
        }

        @Override
        public void process(Record<String, ToolUseResult> record) {
            String        sessionId = record.key();
            ToolUseResult result    = record.value();

            if (sessionId == null || result == null) {
                log.warn("Null key or value — skipping");
                return;
            }

            // Read expected count from the result itself
            int expectedCount = result.totalTools();

            // Read or initialise the accumulator for this session
            ToolResultAccumulator current = accumulatorStore.get(sessionId);
            if (current == null) {
                current = ToolResultAccumulator.empty(sessionId, result.sessionId(), expectedCount);
            }

            // Guard: do not process if already emitted for this cycle
            if (current.emitted()) {
                log.warn("[{}] Result arrived after emission — ignoring tool_use_id={}",
                        sessionId, result.toolUseId());
                return;
            }

            // Deduplicate by tool_use_id — skip if we already have a result for this tool call
            boolean isDuplicate = current.results() != null && current.results().stream()
                    .anyMatch(r -> r.toolUseId() != null && r.toolUseId().equals(result.toolUseId()));
            if (isDuplicate) {
                log.info("[{}] Duplicate tool_use_id={} — skipping", sessionId, result.toolUseId());
                return;
            }

            // Append the new result
            List<ToolUseResult> updatedResults = append(current.results(), result);

            ToolResultAccumulator updated = new ToolResultAccumulator(
                    sessionId,
                    current.userId(),
                    expectedCount > 0 ? expectedCount : current.expectedCount(),
                    updatedResults,
                    false,
                    Instant.now().toString()
            );

            log.info("[{}] Tool result accumulated — received={}/{} tool_use_id={}",
                    sessionId,
                    updatedResults.size(),
                    updated.expectedCount(),
                    result.toolUseId());

            // Check completion
            if (updated.isComplete()) {
                log.info("[{}] All {} tool results received — emitting tool-use-all-complete",
                        sessionId, updated.expectedCount());

                ToolResultAccumulator emitted = new ToolResultAccumulator(
                        updated.sessionId(), updated.userId(), updated.expectedCount(),
                        updated.results(), true, updated.timestamp());

                accumulatorStore.put(sessionId, emitted);
                context.forward(new Record<>(sessionId, emitted, record.timestamp()));

                // Reset store for the next think-cycle on this session
                accumulatorStore.delete(sessionId);

            } else {
                accumulatorStore.put(sessionId, updated);
            }
        }

        private static List<ToolUseResult> append(List<ToolUseResult> existing,
                                                   ToolUseResult incoming) {
            List<ToolUseResult> list = new ArrayList<>();
            if (existing != null) list.addAll(existing);
            list.add(incoming);
            return Collections.unmodifiableList(list);
        }
    }
}
