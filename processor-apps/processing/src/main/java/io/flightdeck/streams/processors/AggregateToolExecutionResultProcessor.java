package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.model.ToolResultAccumulator;
import io.flightdeck.streams.model.ToolResultAccumulator.ExpectedTool;
import io.flightdeck.streams.model.ToolUseItem;
import io.flightdeck.streams.model.ToolUseResult;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <h2>Aggregate Tool Execution Result Processor</h2>
 *
 * <p>Collects tool results per session and emits {@code tool-use-all-complete}
 * once every tool the LLM requested has a result — whether produced
 * synchronously by a tool consumer or asynchronously via the
 * {@code /api/tool/response} callback.
 *
 * <h3>Two inputs, one store</h3>
 * <pre>
 *   think-request-response ─┐ (seed: expected tool_use_ids + deadline)
 *                           ├─► [accumulate per session_id] ─► tool-use-all-complete
 *   tool-use-result ────────┘ (append + complete check)
 * </pre>
 * Both streams are keyed by {@code session_id} and merged into a single
 * stateful processor, so it alone owns the accumulator store and the timeout
 * punctuator. The two source streams may interleave in any order; the processor
 * handles results that arrive before their seed (count-based fallback) and a
 * seed that completes a turn whose results all arrived first.
 *
 * <h3>Async timeout</h3>
 * A wall-clock punctuator scans for accumulators past {@code deadlineMs}. For
 * those it synthesises {@code status:"error"} results for every still-missing
 * tool — guaranteeing the downstream tool message carries a result for every
 * {@code tool_use} block (a hard requirement of the Claude API) — then emits
 * complete. Emitted entries become short-lived tombstones so late/duplicate
 * callbacks are ignored rather than starting a fresh accumulator.
 */
public class AggregateToolExecutionResultProcessor {

    private static final Logger log = LoggerFactory.getLogger(AggregateToolExecutionResultProcessor.class);

    /** RocksDB store: session_id → {@link ToolResultAccumulator}. */
    public static final String ACCUMULATOR_STORE = "tool-result-accumulator-store";

    /** How long to wait for all results (incl. async callbacks) before timing out. */
    static final long ASYNC_TOOL_TIMEOUT_MS =
            envLong("ASYNC_TOOL_TIMEOUT_MS", 300_000L);

    /** How often the wall-clock punctuator scans for timed-out / expired entries. */
    static final long PUNCTUATE_INTERVAL_MS =
            envLong("TOOL_AGG_PUNCTUATE_INTERVAL_MS", 15_000L);

    /** How long an emitted tombstone is retained to absorb late/duplicate results. */
    static final long TOMBSTONE_TTL_MS =
            envLong("TOOL_AGG_TOMBSTONE_TTL_MS", 60_000L);

    public static void register(StreamsBuilder builder, KStream<String, ThinkResponse> thinkStream) {

        StoreBuilder<KeyValueStore<String, ToolResultAccumulator>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ACCUMULATOR_STORE),
                        Serdes.String(),
                        JsonSerde.of(ToolResultAccumulator.class)
                );
        builder.addStateStore(storeBuilder);

        // Seed events: each think response that requested tools.
        KStream<String, AggEvent> seedStream = thinkStream
                .filter((sessionId, response) ->
                        response != null
                                && response.toolUses() != null
                                && !response.toolUses().isEmpty())
                .mapValues(AggEvent::seed);

        // Result events: every tool result (sync producers + async callbacks).
        KStream<String, AggEvent> resultStream = builder.stream(
                        Topics.TOOL_USE_RESULT,
                        Consumed.with(Serdes.String(), JsonSerde.of(ToolUseResult.class)))
                .mapValues(AggEvent::result);

        seedStream.merge(resultStream)
                .process(
                        (ProcessorSupplier<String, AggEvent,
                                           String, ToolResultAccumulator>) AccumulatorProcessor::new,
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
    // Internal merged-event wrapper (never hits a topic — no serde needed)
    // ─────────────────────────────────────────────────────────────────────────

    enum Kind { SEED, RESULT }

    record AggEvent(Kind kind, ThinkResponse seed, ToolUseResult result) {
        static AggEvent seed(ThinkResponse r)   { return new AggEvent(Kind.SEED, r, null); }
        static AggEvent result(ToolUseResult r) { return new AggEvent(Kind.RESULT, null, r); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Processor
    // ─────────────────────────────────────────────────────────────────────────

    static class AccumulatorProcessor
            implements Processor<String, AggEvent, String, ToolResultAccumulator> {

        private ProcessorContext<String, ToolResultAccumulator> context;
        private KeyValueStore<String, ToolResultAccumulator> store;

        @Override
        public void init(ProcessorContext<String, ToolResultAccumulator> ctx) {
            this.context = ctx;
            this.store = ctx.getStateStore(ACCUMULATOR_STORE);
            ctx.schedule(Duration.ofMillis(PUNCTUATE_INTERVAL_MS),
                    PunctuationType.WALL_CLOCK_TIME, this::sweep);
        }

        @Override
        public void process(Record<String, AggEvent> record) {
            String key = record.key();
            AggEvent event = record.value();
            if (key == null || event == null) {
                log.warn("Null key or event — skipping");
                return;
            }
            if (event.kind() == Kind.SEED) {
                handleSeed(key, event.seed(), record.timestamp());
            } else {
                handleResult(key, event.result(), record.timestamp());
            }
        }

        // ── Seed: record the expected tool set and the timeout deadline ────────
        private void handleSeed(String sessionId, ThinkResponse response, long ts) {
            ToolResultAccumulator current = store.get(sessionId);

            List<ExpectedTool> expected = new ArrayList<>();
            for (ToolUseItem item : response.toolUses()) {
                expected.add(new ExpectedTool(item.toolUseId(), item.name()));
            }

            if (current != null && current.emitted()) {
                // Tombstone from a previous turn. If this seed names the same
                // tools it is a redelivery — ignore. Otherwise it opens the next
                // turn on this session: start fresh, dropping the old results.
                Set<String> seedIds = expected.stream()
                        .map(ExpectedTool::toolUseId).collect(Collectors.toSet());
                if (knownIds(current).containsAll(seedIds)) {
                    return;
                }
                current = null;
            }

            long now = System.currentTimeMillis();
            long deadline = (current != null && current.deadlineMs() > 0)
                    ? current.deadlineMs()
                    : now + ASYNC_TOOL_TIMEOUT_MS;
            List<ToolUseResult> existingResults =
                    (current != null && current.results() != null) ? current.results() : List.of();

            ToolResultAccumulator seeded = new ToolResultAccumulator(
                    sessionId,
                    response.userId(),
                    expected.size(),
                    existingResults,
                    Collections.unmodifiableList(expected),
                    deadline,
                    false,
                    Instant.now().toString());

            log.info("[{}] Seeded expected tool set — expected={} alreadyHave={} deadlineInMs={}",
                    sessionId, expected.size(), existingResults.size(), deadline - now);

            // Results may all have arrived before the seed — check completion now.
            if (seeded.isComplete()) {
                emitComplete(sessionId, seeded, ts);
            } else {
                store.put(sessionId, seeded);
            }
        }

        // ── Result: append and check completion ────────────────────────────────
        private void handleResult(String sessionId, ToolUseResult result, long ts) {
            if (result == null) {
                log.warn("[{}] Null result — skipping", sessionId);
                return;
            }

            ToolResultAccumulator current = store.get(sessionId);
            long now = System.currentTimeMillis();

            if (current != null && current.emitted()) {
                // Tombstone from a completed turn. A result whose tool_use_id was
                // part of that turn is a late/duplicate callback — ignore it. A
                // result with a new tool_use_id belongs to the next turn.
                if (knownIds(current).contains(result.toolUseId())) {
                    log.warn("[{}] Late/duplicate result for completed turn — ignoring tool_use_id={}",
                            sessionId, result.toolUseId());
                    return;
                }
                current = null;
            }

            if (current == null) {
                // First result of a turn arriving before its seed: start a
                // count-based accumulator (the seed merges in the expected set later).
                current = new ToolResultAccumulator(
                        sessionId, result.sessionId(), result.totalTools(),
                        List.of(), List.of(), now + ASYNC_TOOL_TIMEOUT_MS, false, null);
            }

            boolean duplicate = current.results() != null && current.results().stream()
                    .anyMatch(r -> r.toolUseId() != null && r.toolUseId().equals(result.toolUseId()));
            if (duplicate) {
                log.info("[{}] Duplicate tool_use_id={} — skipping", sessionId, result.toolUseId());
                return;
            }

            List<ToolUseResult> updatedResults = append(current.results(), result);
            int expectedCount = (current.expected() != null && !current.expected().isEmpty())
                    ? current.expected().size()
                    : Math.max(result.totalTools(), current.expectedCount());

            ToolResultAccumulator updated = new ToolResultAccumulator(
                    sessionId,
                    current.userId(),
                    expectedCount,
                    updatedResults,
                    current.expected(),
                    current.deadlineMs() > 0 ? current.deadlineMs() : now + ASYNC_TOOL_TIMEOUT_MS,
                    false,
                    Instant.now().toString());

            log.info("[{}] Tool result accumulated — received={}/{} tool_use_id={}",
                    sessionId, updatedResults.size(), updated.expectedCount(), result.toolUseId());

            if (updated.isComplete()) {
                emitComplete(sessionId, updated, ts);
            } else {
                store.put(sessionId, updated);
            }
        }

        // ── Wall-clock sweep: time out stale turns, expire tombstones ──────────
        private void sweep(long now) {
            List<String> toExpire = new ArrayList<>();
            List<ToolResultAccumulator> toTimeout = new ArrayList<>();

            try (KeyValueIterator<String, ToolResultAccumulator> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, ToolResultAccumulator> kv = it.next();
                    ToolResultAccumulator acc = kv.value;
                    if (acc == null) continue;

                    if (acc.emitted()) {
                        // Tombstone: deadlineMs doubles as the tombstone expiry.
                        if (acc.deadlineMs() > 0 && now >= acc.deadlineMs()) {
                            toExpire.add(kv.key);
                        }
                    } else if (acc.deadlineMs() > 0 && now >= acc.deadlineMs()) {
                        toTimeout.add(acc);
                    }
                }
            }

            // Mutate the store outside the iterator.
            for (String key : toExpire) {
                store.delete(key);
                log.debug("[{}] Tombstone expired — removed", key);
            }
            for (ToolResultAccumulator acc : toTimeout) {
                ToolResultAccumulator filled = fillMissingWithErrors(acc, now);
                log.warn("[{}] Async tool timeout — synthesising {} error result(s), emitting complete",
                        acc.sessionId(), filled.results().size() - acc.results().size());
                emitComplete(acc.sessionId(), filled, now);
            }
        }

        /** Adds a {@code status:"error"} result for every expected tool still missing. */
        private ToolResultAccumulator fillMissingWithErrors(ToolResultAccumulator acc, long now) {
            List<ToolUseResult> results = new ArrayList<>(
                    acc.results() != null ? acc.results() : List.of());
            for (ExpectedTool missing : acc.missing()) {
                results.add(new ToolUseResult(
                        acc.sessionId(),
                        missing.toolUseId(),
                        null,
                        missing.name(),
                        Map.of("error", "async tool did not respond within "
                                + ASYNC_TOOL_TIMEOUT_MS + "ms", "reason", "timeout"),
                        ASYNC_TOOL_TIMEOUT_MS,
                        "error",
                        acc.expectedCount(),
                        Instant.now().toString()));
            }
            return new ToolResultAccumulator(
                    acc.sessionId(), acc.userId(), acc.expectedCount(),
                    Collections.unmodifiableList(results), acc.expected(),
                    acc.deadlineMs(), false, Instant.now().toString());
        }

        /** Forwards the completed accumulator and leaves a short-lived tombstone behind. */
        private void emitComplete(String sessionId, ToolResultAccumulator acc, long ts) {
            log.info("[{}] All tool results in — emitting tool-use-all-complete (count={})",
                    sessionId, acc.results().size());

            ToolResultAccumulator completed = new ToolResultAccumulator(
                    acc.sessionId(), acc.userId(), acc.expectedCount(),
                    acc.results(), acc.expected(), acc.deadlineMs(),
                    true, Instant.now().toString());
            context.forward(new Record<>(sessionId, completed, ts));

            // Keep an emitted=true tombstone, retaining the completed turn's
            // tool_use_ids so a late/duplicate result for THIS turn is ignored,
            // while a result with a new tool_use_id still opens the next turn.
            // deadlineMs is reused as the tombstone expiry.
            ToolResultAccumulator tombstone = new ToolResultAccumulator(
                    acc.sessionId(), acc.userId(), acc.expectedCount(),
                    acc.results(), acc.expected(),
                    System.currentTimeMillis() + TOMBSTONE_TTL_MS,
                    true, Instant.now().toString());
            store.put(sessionId, tombstone);
        }

        /** All tool_use_ids associated with a turn — from collected results and the seeded expected set. */
        private static Set<String> knownIds(ToolResultAccumulator acc) {
            Set<String> ids = new HashSet<>();
            if (acc.results() != null) {
                for (ToolUseResult r : acc.results()) {
                    if (r.toolUseId() != null) ids.add(r.toolUseId());
                }
            }
            if (acc.expected() != null) {
                for (ExpectedTool e : acc.expected()) {
                    if (e.toolUseId() != null) ids.add(e.toolUseId());
                }
            }
            return ids;
        }

        private static List<ToolUseResult> append(List<ToolUseResult> existing, ToolUseResult incoming) {
            List<ToolUseResult> list = new ArrayList<>();
            if (existing != null) list.addAll(existing);
            list.add(incoming);
            return Collections.unmodifiableList(list);
        }
    }

    private static long envLong(String key, long defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return defaultValue;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid {}={} — using default {}", key, v, defaultValue);
            return defaultValue;
        }
    }
}
