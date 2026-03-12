package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.AgentMessage;
import io.axon.streams.model.FullMessageContext;
import io.axon.streams.model.MessageContext;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * <h2>Enrich Input Message Processor</h2>
 *
 * <p>Implements the <em>"Join"</em> node in the architecture diagram — the
 * step labelled <em>"Join historical conversation with latest input"</em>
 * that sits between {@code message-input} and {@code full-message-context}.
 *
 * <h3>Topology</h3>
 * <pre>
 *   message-input  (KStream — raw user turn, keyed by session_id)
 *         │
 *         │   leftJoin
 *         │◄──────────────  message-context  (KTable — accumulated history)
 *         │
 *         ▼
 *   full-message-context  (KStream — history + latest input, consumed by Think)
 * </pre>
 *
 * <h3>Why a left join?</h3>
 * On the very first message of a brand-new session the {@code message-context}
 * KTable has no entry yet.  A left join ensures the record is still forwarded
 * downstream with an empty history list rather than being silently dropped,
 * so the Think processor always receives something to act on.
 *
 * <h3>Relationship to AccumulateMessageContextProcessor</h3>
 * {@link AccumulateMessageContextProcessor} owns the KTable — it builds and
 * maintains {@code message-context} by aggregating {@link io.axon.streams.model.ThinkResponse}
 * records.  This processor is a pure read-side consumer of that table; it never
 * writes to it.  Keeping the two concerns separate mirrors the diagram topology
 * exactly and makes each processor independently testable.
 */
public class EnrichInputMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(EnrichInputMessageProcessor.class);

    /**
     * Name of the local KTable store used on the join side.
     * Distinct from {@link AccumulateMessageContextProcessor#MESSAGE_CONTEXT_STORE}
     * so the two stores do not collide inside the same topology.
     */
    public static final String ENRICH_CONTEXT_STORE = "enrich-message-context-store";

    public static void register(StreamsBuilder builder) {

        // ── Left side: incoming user messages ────────────────────────────────
        KStream<String, AgentMessage> inputStream = builder.stream(
                Topics.MESSAGE_INPUT,
                Consumed.with(Serdes.String(), JsonSerde.of(AgentMessage.class))
        );

        // ── Right side: conversation history KTable (built by AccumulateMessageContextProcessor)
        // Read the message-context topic as a KTable with its own local state store.
        KTable<String, MessageContext> contextTable = builder.table(
                Topics.MESSAGE_CONTEXT,
                Consumed.with(Serdes.String(), JsonSerde.of(MessageContext.class)),
                Materialized.<String, MessageContext>as(
                        Stores.persistentKeyValueStore(ENRICH_CONTEXT_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(MessageContext.class))
        );

        // ── Left join: enrich each incoming message with its session history ──
        inputStream
                .leftJoin(
                        contextTable,
                        EnrichInputMessageProcessor::enrich,
                        Joined.with(
                                Serdes.String(),
                                JsonSerde.of(AgentMessage.class),
                                JsonSerde.of(MessageContext.class)
                        )
                )
                .peek((sessionId, full) ->
                        log.info("[{}] Enriched — history_size={} first_turn={}",
                                sessionId,
                                full.history().size(),
                                full.history().isEmpty()))
                .to(Topics.FULL_MESSAGE_CONTEXT,
                        Produced.with(Serdes.String(), JsonSerde.of(FullMessageContext.class)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Join value mapper — extracted for direct unit testing
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Merges an incoming {@link AgentMessage} with the session's current
     * {@link MessageContext} (which may be {@code null} on the first turn).
     *
     * <p>Rules:
     * <ul>
     *   <li>History is taken from {@code context.history()} or empty if null.</li>
     *   <li>{@code userId} is taken from the message; falls back to the context
     *       value if the message carries none (e.g. scheduler-triggered inputs).</li>
     *   <li>{@code sessionId} is always taken from the message — it is the
     *       canonical key for this record.</li>
     * </ul>
     */
    static FullMessageContext enrich(AgentMessage message, MessageContext context) {
        List<AgentMessage> history = (context != null && context.history() != null)
                ? context.history()
                : List.of();

        String userId = (message.userId() != null && !message.userId().isBlank())
                ? message.userId()
                : (context != null ? context.userId() : null);

        return new FullMessageContext(
                message.sessionId(),
                userId,
                history,
                message,
                Instant.now().toString()
        );
    }
}