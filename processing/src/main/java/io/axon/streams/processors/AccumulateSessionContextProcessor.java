package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MessageInput;
import io.axon.streams.model.FullMessageContext;
import io.axon.streams.model.MessageContext;
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
 *   aggregate ──► MessageContext  (growing history list)
 *       │
 *       ▼  toStream
 *   message-context  (compacted changelog topic + queryable RocksDB store)
 * </pre>
 * Every {@link ThinkResponse} is appended to the running {@link MessageContext}
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
        KTable<String, MessageContext> contextTable = registerAggregation(builder);
        registerJoin(builder, contextTable);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fragment 1 — Aggregate ThinkResponses into a per-session MessageContext
    // ─────────────────────────────────────────────────────────────────────────

    private static KTable<String, MessageContext> registerAggregation(StreamsBuilder builder) {

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        KTable<String, MessageContext> contextTable = thinkStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)))
                .aggregate(
                        // Initialiser — called once when a session_id is seen for the first time
                        () -> MessageContext.empty("unknown"),

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

                            MessageContext updated = new MessageContext(
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
                        Materialized.<String, MessageContext>as(
                                Stores.persistentKeyValueStore(MESSAGE_CONTEXT_STORE))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerde.of(MessageContext.class))
                );

        // Publish the changelog so external consumers can observe session state
        contextTable
                .toStream()
                .peek((sid, ctx) -> log.debug("[{}] → {} history_size={}",
                        sid, Topics.SESSION_CONTEXT,
                        ctx != null ? ctx.history().size() : 0))
                .to(Topics.SESSION_CONTEXT,
                        Produced.with(Serdes.String(), JsonSerde.of(MessageContext.class)));

        return contextTable;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fragment 2 — Left-join incoming user messages with the history KTable
    // ─────────────────────────────────────────────────────────────────────────

    private static void registerJoin(StreamsBuilder builder,
                                     KTable<String, MessageContext> contextTable) {

        KStream<String, MessageInput> inputStream = builder.stream(
                Topics.MESSAGE_INPUT,
                Consumed.with(Serdes.String(), JsonSerde.of(MessageInput.class))
        );

        inputStream
                .leftJoin(
                        contextTable,
                        (userMsg, ctx) -> {
                            // ctx is null on the very first turn of a new session
                            List<MessageInput> history =
                                    (ctx != null && ctx.history() != null)
                                    ? ctx.history()
                                    : List.of();

                            String userId = userMsg.userId() != null
                                    ? userMsg.userId()
                                    : (ctx != null ? ctx.userId() : null);

                            FullMessageContext full = new FullMessageContext(
                                    userMsg.sessionId(),
                                    userId,
                                    history,
                                    userMsg,
                                    Instant.now().toString()
                            );

                            log.info("[{}] FullMessageContext built — history={} firstTurn={}",
                                    userMsg.sessionId(), history.size(), ctx == null);

                            return full;
                        },
                        Joined.with(
                                Serdes.String(),
                                JsonSerde.of(MessageInput.class),
                                JsonSerde.of(MessageContext.class)
                        )
                )
                .peek((sid, full) ->
                        log.debug("[{}] → {}", sid, Topics.ENRICHED_MESSAGE_INPUT))
                .to(Topics.ENRICHED_MESSAGE_INPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(FullMessageContext.class)));
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