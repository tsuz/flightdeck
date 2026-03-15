package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MessageInput;
import io.axon.streams.model.SessionContext;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <h2>Accumulate Message Context Processor</h2>
 *
 * <p>Implements two connected topology fragments that together build and serve
 * the per-session conversation history shown as the <em>message-context</em>
 * KTable node in the architecture diagram.
 *
 * <h3>Fragment 1 — Aggregate (think-request-response → message-context KTable)</h3>
 * <pre>
 *   think-request-response
 *       │
 *       ▼  groupByKey (session_id)
 *   aggregate ──► SessionContext  (growing history list)
 *       │
 *       ▼  toStream
 *   message-context  (compacted changelog topic + queryable RocksDB store)
 * </pre>
 * Every {@link ThinkResponse} is appended to the running {@link SessionContext}
 * for that {@code session_id}, tracking full cost, LLM call count, and the
 * complete conversation history.
 *
 * <h3>Fragment 2 — Join (message-input ⋈ message-context → enriched-message-input)</h3>
 * <pre>
 *   message-input (KStream)
 *       │
 *       ▼  leftJoin with message-context KTable
 *   enriched-message-input ──► consumed by Think processor
 * </pre>
 * A left-join is used so that the very first user message of a brand-new session
 * (no history yet) still produces output with an empty history list, rather than
 * being silently dropped.
 */
public class AccumulateSessionContextProcessor {

    private static final Logger log = LoggerFactory.getLogger(AccumulateSessionContextProcessor.class);

    /** Name of the persistent RocksDB state store backing the context KTable. */
    public static final String MESSAGE_CONTEXT_STORE      = "message-context-store";
    public static final String MESSAGE_CONTEXT_JOIN_STORE = "message-context-join-store";

    public static void register(StreamsBuilder builder) {
        registerAggregation(builder);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fragment 1 — Aggregate ThinkResponses into a per-session SessionContext
    // ─────────────────────────────────────────────────────────────────────────

    private static KTable<String, SessionContext> registerAggregation(StreamsBuilder builder) {

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        KTable<String, SessionContext> contextTable = thinkStream
                .filter((key, value) -> key != null && value != null)
                .groupByKey(Grouped.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)))
                .aggregate(
                        // Initialiser — called once when a session_id is seen for the first time
                        () -> SessionContext.empty("unknown"),

                        // Aggregator — append each ThinkResponse's messages into the history
                        (sessionId, response, current) -> {
                            List<MessageInput> updatedHistory = appendMessages(
                                    current.history(), response.messages());

                            double updatedCost  = current.cost()     + response.cost();
                            int    updatedCalls = current.llmCalls() + 1;

                            List<MessageInput> latestMessages =
                                    response.messages() != null
                                    ? Collections.unmodifiableList(response.messages())
                                    : List.of();

                            SessionContext updated = new SessionContext(
                                    sessionId,
                                    resolveUserId(current.userId(), response.userId()),
                                    updatedCost,
                                    updatedCalls,
                                    latestMessages,
                                    updatedHistory,
                                    Instant.now().toString()
                            );

                            log.info("[{}] Context updated — turn={} cost=${} history={}",
                                    sessionId, updatedCalls,
                                    String.format("%.4f", updatedCost),
                                    updatedHistory.size());

                            return updated;
                        },

                        // Materialise into a named, persistent, queryable store
                        Materialized.<String, SessionContext>as(
                                Stores.persistentKeyValueStore(MESSAGE_CONTEXT_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.of(SessionContext.class))
                );

        // Publish the changelog so external consumers can observe session state
        contextTable
                .toStream()
                .peek((sid, ctx) -> log.debug("[{}] → {} history_size={}",
                        sid, Topics.SESSION_CONTEXT,
                        ctx != null ? ctx.history().size() : 0))
                .to(Topics.SESSION_CONTEXT,
                        Produced.with(Serdes.String(), JsonSerde.of(SessionContext.class)));

        return contextTable;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Package-private helpers (also used by tests)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Immutably appends {@code newMessages} after {@code history}.
     * Null-safe on both inputs.
     */
    static List<MessageInput> appendMessages(
            List<MessageInput> history,
            List<MessageInput> newMessages) {

        List<MessageInput> combined = new ArrayList<>();
        if (history     != null) combined.addAll(history);
        if (newMessages != null) combined.addAll(newMessages);
        return Collections.unmodifiableList(combined);
    }

    /**
     * Returns {@code incoming} if non-blank, otherwise falls back to
     * {@code existing} to preserve user identity across turns.
     */
    static String resolveUserId(String existing, String incoming) {
        return (incoming != null && !incoming.isBlank()) ? incoming : existing;
    }
}