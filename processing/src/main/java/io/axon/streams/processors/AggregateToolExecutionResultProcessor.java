package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.ThinkResponse;
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
 * <p>Implements the <em>"Tool Result Aggregation"</em> beige node in the
 * architecture diagram, including the diagram annotation:
 * <em>"Emit Next when array count equals number of expected tools for session id"</em>.
 *
 * <h3>Topology</h3>
 * <pre>
 *   think-request-response ──► [Fragment 1: register expected count]
 *                                      │
 *                                      ▼
 *                              expected-count-store
 *                              (session_id → int)
 *
 *   tool-use-result ──► [Fragment 2: accumulate + check completion]
 *                                      │
 *                              ┌───────┴────────────────────────┐
 *                              │  read expected-count-store     │
 *                              │  append to accumulator-store   │
 *                              │                                │
 *                              │  results.size() == expected?   │
 *                              │       YES ──► emit             │
 *                              │               + reset stores   │
 *                              └────────────────────────────────┘
 *                                      │ (when complete)
 *                                      ▼
 *                              tool-use-all-complete
 * </pre>
 *
 * <h3>Two-store design</h3>
 * <ul>
 *   <li><b>{@value EXPECTED_COUNT_STORE}</b> — Written by Fragment 1 when a
 *       {@link ThinkResponse} arrives carrying tool-use blocks.  Records the
 *       number of tool results the session must collect before emitting.</li>
 *   <li><b>{@value ACCUMULATOR_STORE}</b> — Written by Fragment 2 as each
 *       {@link ToolUseResult} arrives.  Holds the growing result list until
 *       completion.</li>
 * </ul>
 *
 * <h3>One-shot emission guard</h3>
 * The {@link ToolResultAccumulator#emitted()} flag prevents a second emission
 * if a duplicate or late result arrives after the complete event has already
 * been forwarded.  The stores are reset (tombstoned) immediately after
 * emission so they do not grow unboundedly across many think-cycles.
 *
 * <h3>Missing expected count</h3>
 * If a result arrives for a session whose expected count has not yet been
 * registered (race condition or out-of-order delivery), the result is held
 * in the accumulator with {@code expectedCount = 0} and a warning is logged.
 * Once the expected count arrives via Fragment 1, Fragment 2 will re-check
 * completion on the <em>next</em> incoming result.  For true out-of-order
 * tolerance a reprocessing trigger could be added — left as a TODO.
 */
public class AggregateToolExecutionResultProcessor {

    private static final Logger log = LoggerFactory.getLogger(AggregateToolExecutionResultProcessor.class);

    /** RocksDB store: session_id → expected number of tool results (int serialised as String). */
    public static final String EXPECTED_COUNT_STORE = "tool-expected-count-store";

    /** RocksDB store: session_id → {@link ToolResultAccumulator}. */
    public static final String ACCUMULATOR_STORE    = "tool-result-accumulator-store";

    public static void register(StreamsBuilder builder) {
        registerExpectedCountCapture(builder);
        registerResultAccumulation(builder);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fragment 1 — Capture expected tool count from ThinkResponse
    // ─────────────────────────────────────────────────────────────────────────

    private static void registerExpectedCountCapture(StreamsBuilder builder) {

        StoreBuilder<KeyValueStore<String, String>> countStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EXPECTED_COUNT_STORE),
                        Serdes.String(),
                        Serdes.String()          // count stored as String for simplicity
                );
        builder.addStateStore(countStoreBuilder);

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        thinkStream
                .filter((sid, r) -> r != null
                        && r.toolUses() != null
                        && !r.toolUses().isEmpty())
                .process(
                        (ProcessorSupplier<String, ThinkResponse, Void, Void>) () ->
                                new Processor<>() {
                                    private KeyValueStore<String, String> countStore;

                                    @Override
                                    public void init(ProcessorContext<Void, Void> ctx) {
                                        countStore = ctx.getStateStore(EXPECTED_COUNT_STORE);
                                    }

                                    @Override
                                    public void process(Record<String, ThinkResponse> record) {
                                        String sessionId    = record.key();
                                        ThinkResponse resp  = record.value();
                                        int expected        = resp.toolUses().size();

                                        countStore.put(sessionId, String.valueOf(expected));

                                        log.info("[{}] Registered expected tool count = {}",
                                                sessionId, expected);
                                    }
                                },
                        EXPECTED_COUNT_STORE
                );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fragment 2 — Accumulate results and emit when all have arrived
    // ─────────────────────────────────────────────────────────────────────────

    private static void registerResultAccumulation(StreamsBuilder builder) {

        StoreBuilder<KeyValueStore<String, ToolResultAccumulator>> accStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ACCUMULATOR_STORE),
                        Serdes.String(),
                        JsonSerde.of(ToolResultAccumulator.class)
                );
        builder.addStateStore(accStoreBuilder);

        KStream<String, ToolUseResult> resultStream = builder.stream(
                Topics.TOOL_USE_RESULT,
                Consumed.with(Serdes.String(), JsonSerde.of(ToolUseResult.class))
        );

        resultStream
                .process(
                        (ProcessorSupplier<String, ToolUseResult,
                                           String, ToolResultAccumulator>) ResultAccumulatorProcessor::new,
                        EXPECTED_COUNT_STORE, ACCUMULATOR_STORE
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
    // Inner Processor implementation — package-private for testing
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Stateful processor that accumulates {@link ToolUseResult} records per
     * session and forwards the completed {@link ToolResultAccumulator} to the
     * downstream sink when all expected results have arrived.
     */
    static class ResultAccumulatorProcessor
            implements Processor<String, ToolUseResult, String, ToolResultAccumulator> {

        private ProcessorContext<String, ToolResultAccumulator> context;
        private KeyValueStore<String, String>               expectedCountStore;
        private KeyValueStore<String, ToolResultAccumulator> accumulatorStore;

        @Override
        public void init(ProcessorContext<String, ToolResultAccumulator> ctx) {
            this.context           = ctx;
            this.expectedCountStore = ctx.getStateStore(EXPECTED_COUNT_STORE);
            this.accumulatorStore   = ctx.getStateStore(ACCUMULATOR_STORE);
        }

        @Override
        public void process(Record<String, ToolUseResult> record) {
            String        sessionId = record.key();
            ToolUseResult result    = record.value();

            if (result == null) {
                log.warn("[{}] Null ToolUseResult received — skipping", sessionId);
                return;
            }

            // ── Read expected count from store (may be 0 if not yet registered) ──
            int expectedCount = readExpectedCount(sessionId);

            // ── Read or initialise the accumulator for this session ───────────
            ToolResultAccumulator current = accumulatorStore.get(sessionId);
            if (current == null) {
                current = ToolResultAccumulator.empty(sessionId, result.sessionId(), expectedCount);
            }

            // ── Guard: do not process if already emitted for this cycle ──────
            if (current.emitted()) {
                log.warn("[{}] Result arrived after emission already fired — ignoring tool_use_id={}",
                        sessionId, result.toolUseId());
                return;
            }

            // ── Append the new result ─────────────────────────────────────────
            List<ToolUseResult> updatedResults = append(current.results(), result);

            ToolResultAccumulator updated = new ToolResultAccumulator(
                    sessionId,
                    current.userId(),
                    expectedCount > 0 ? expectedCount : current.expectedCount(),
                    updatedResults,
                    false,
                    Instant.now().toString()
            );

            log.info("[{}] Tool result accumulated — received={} expected={}  tool_use_id={}",
                    sessionId,
                    updatedResults.size(),
                    updated.expectedCount(),
                    result.toolUseId());

            // ── Check completion ──────────────────────────────────────────────
            if (updated.isComplete()) {
                log.info("[{}] All {} tool results received — emitting tool-use-all-complete",
                        sessionId, updated.expectedCount());

                // Mark as emitted and persist before forwarding
                ToolResultAccumulator emitted = new ToolResultAccumulator(
                        updated.sessionId(), updated.userId(), updated.expectedCount(),
                        updated.results(), true, updated.timestamp());

                accumulatorStore.put(sessionId, emitted);
                context.forward(new Record<>(sessionId, emitted, record.timestamp()));

                // Reset both stores for the next think-cycle on this session
                resetStores(sessionId);

            } else {
                // Not yet complete — persist the updated accumulator
                accumulatorStore.put(sessionId, updated);
            }
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private int readExpectedCount(String sessionId) {
            String raw = expectedCountStore.get(sessionId);
            if (raw == null) {
                log.warn("[{}] Expected count not yet registered — holding result", sessionId);
                return 0;
            }
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException e) {
                log.error("[{}] Corrupt expected count '{}' — treating as 0", sessionId, raw);
                return 0;
            }
        }

        private void resetStores(String sessionId) {
            expectedCountStore.delete(sessionId);
            accumulatorStore.delete(sessionId);
            log.debug("[{}] Stores reset after emission", sessionId);
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