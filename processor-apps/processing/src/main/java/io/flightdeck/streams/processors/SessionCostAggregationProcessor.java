package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.SessionCost;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * <h2>Session Cost Aggregation Processor</h2>
 *
 * <p>Implements the <em>"Aggregate cost per conversation"</em> beige node
 * visible in the architecture diagram, sitting between
 * {@code think-request-response} and {@code session-cost}.
 *
 * <h3>Topology fragment</h3>
 * <pre>
 *   think-request-response  (KStream)
 *       │
 *       ▼  groupByKey(session_id)
 *   aggregate()
 *       ├─ llm_calls            += 1
 *       ├─ total_input_tokens   += response.inputTokens
 *       ├─ total_output_tokens  += response.outputTokens
 *       └─ estimated_cost_usd   += response.cost
 *       │
 *       ▼  Materialized KTable  ("session-cost-store")
 *       │
 *       ▼  toStream()
 *   session-cost  (compacted changelog topic)
 * </pre>
 *
 * <h3>Pricing model</h3>
 * The processor trusts the {@code cost} field already present on each
 * {@link ThinkResponse} (set by the upstream Think processor which has
 * access to the exact token pricing at call time).  This keeps pricing
 * logic in one place and makes the aggregator a pure accumulator.
 * A {@link #estimateCost} helper is also provided for cases where the
 * upstream cost field is zero or absent — it applies Sonnet 4 list pricing
 * as a safe fallback.
 *
 * <h3>Tombstone note</h3>
 * The diagram annotates <em>"Emit Tombstone when aggregated"</em>.
 * A tombstone (null-value record) is emitted on {@code session-cost} when a
 * session is explicitly closed by sending a {@link ThinkResponse} whose
 * {@code endTurn} flag is {@code true} AND whose {@code cost} is exactly
 * {@code -1.0} (the sentinel close signal).  Downstream consumers can use
 * this to evict session state from their own stores.
 */
public class SessionCostAggregationProcessor {

    private static final Logger log = LoggerFactory.getLogger(SessionCostAggregationProcessor.class);

    /** Name of the persistent RocksDB store backing the session-cost KTable. */
    public static final String SESSION_COST_STORE = "session-cost-store";

    /**
     * Fallback pricing constants (USD per million tokens) — Claude Sonnet 4 list price.
     * Only applied when {@link ThinkResponse#cost()} is 0.
     */
    static final double INPUT_COST_PER_M_TOKENS  = 3.0;
    static final double OUTPUT_COST_PER_M_TOKENS = 15.0;

    /** Sentinel cost value that signals a session-close / tombstone event. */
    static final double SESSION_CLOSE_SENTINEL = -1.0;

    public static void register(StreamsBuilder builder, KStream<String, ThinkResponse> thinkStream) {

        // ── Separate close signals from normal LLM responses ─────────────────
        // KStream.branch() was removed in Kafka Streams 4.x; use split() + Branched instead.
        // Named.as("split-") provides a prefix; the full map key becomes "split-" + branch name.
        Map<String, KStream<String, ThinkResponse>> branches = thinkStream
                .split(Named.as("split-"))
                .branch(
                        (sid, r) -> r != null && r.cost() == SESSION_CLOSE_SENTINEL,
                        Branched.as("close-signals")
                )
                .branch(
                        (sid, r) -> r != null,
                        Branched.as("normal-responses")
                )
                .noDefaultBranch();

        KStream<String, ThinkResponse> closeSignals    = branches.get("split-close-signals");
        KStream<String, ThinkResponse> normalResponses = branches.get("split-normal-responses");

        // ── Aggregate normal responses into a running SessionCost ─────────────
        KTable<String, SessionCost> costTable = normalResponses
                .groupByKey(Grouped.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)))
                .aggregate(
                        // Initialiser
                        () -> SessionCost.zero("unknown", null),

                        // Aggregator
                        (sessionId, response, current) -> {
                            double callCost = response.cost() > 0
                                    ? response.cost()
                                    : estimateCost(response.inputTokens(), response.outputTokens());

                            SessionCost updated = new SessionCost(
                                    sessionId,
                                    resolveUserId(current.userId(), response.userId()),
                                    current.llmCalls()          + 1,
                                    current.totalInputTokens()  + response.inputTokens(),
                                    current.totalOutputTokens() + response.outputTokens(),
                                    current.estimatedCostUsd()  + callCost,
                                    Instant.now().toString()
                            );

                            log.info("[{}] Cost updated — calls={} input_tok={} output_tok={} total_usd=${}",
                                    sessionId,
                                    updated.llmCalls(),
                                    updated.totalInputTokens(),
                                    updated.totalOutputTokens(),
                                    String.format("%.6f", updated.estimatedCostUsd()));

                            return updated;
                        },

                        // Materialized persistent store
                        Materialized.<String, SessionCost>as(
                                Stores.persistentKeyValueStore(SESSION_COST_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.of(SessionCost.class))
                );

        // ── Publish running totals to session-cost topic ──────────────────────
        costTable
                .toStream()
                .peek((sid, cost) -> log.debug("[{}] → {} usd=${}",
                        sid, Topics.SESSION_COST,
                        cost != null ? String.format("%.6f", cost.estimatedCostUsd()) : "null"))
                .to(Topics.SESSION_COST,
                        Produced.with(Serdes.String(), JsonSerde.of(SessionCost.class)));

        // ── Emit tombstone on session close ───────────────────────────────────
        // A null value on a compacted topic signals downstream consumers to
        // delete the key — standard Kafka tombstone pattern.
        closeSignals
                .peek((sid, r) -> log.info("[{}] Session closed — emitting tombstone on {}",
                        sid, Topics.SESSION_COST))
                .mapValues(r -> (SessionCost) null)   // explicit null = tombstone
                .to(Topics.SESSION_COST,
                        Produced.with(Serdes.String(), JsonSerde.of(SessionCost.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Package-private helpers (also used by tests)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Estimates USD cost from raw token counts using Sonnet 4 list pricing.
     * Applied only when the upstream {@link ThinkResponse#cost()} field is zero.
     */
    static double estimateCost(int inputTokens, int outputTokens) {
        return (inputTokens  / 1_000_000.0) * INPUT_COST_PER_M_TOKENS
             + (outputTokens / 1_000_000.0) * OUTPUT_COST_PER_M_TOKENS;
    }

    /**
     * Returns {@code incoming} if non-blank, otherwise preserves
     * {@code existing} to maintain user identity across turns.
     */
    static String resolveUserId(String existing, String incoming) {
        return (incoming != null && !incoming.isBlank()) ? incoming : existing;
    }
}