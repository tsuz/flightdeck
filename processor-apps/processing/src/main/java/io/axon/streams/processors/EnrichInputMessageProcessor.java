package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MessageInput;
import io.axon.streams.model.FullSessionContext;
import io.axon.streams.model.SessionContext;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * <h2>Enrich Input Message Processor</h2>
 *
 * <h3>Topology</h3>
 * <pre>
 *   message-input  (KStream — raw user turn, keyed by session_id)
 *         │
 *         │   leftJoin
 *         │◄──────────────  session-context  (KTable — accumulated history)
 *         │
 *         │   leftJoin
 *         │◄──────────────  memoir-context   (KTable — long-term memoir, shared)
 *         │
 *         ▼
 *   enriched-message-input  (KStream — history + memoir + latest input)
 * </pre>
 */
public class EnrichInputMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(EnrichInputMessageProcessor.class);

    /**
     * @param memoirTable   shared KTable for memoir-context (keyed by userId)
     * @param contextTable  shared KTable for session-context (keyed by sessionId)
     */
    public static void register(StreamsBuilder builder,
                                KTable<String, String> memoirTable,
                                KTable<String, SessionContext> contextTable) {

        // ── Left side: incoming user messages ────────────────────────────────
        KStream<String, MessageInput> inputStream = builder.stream(
                Topics.MESSAGE_INPUT,
                Consumed.with(Serdes.String(), JsonSerde.of(MessageInput.class))
        );

        // ── Re-key by session_id from the message value ──────────────────────
        KStream<String, MessageInput> keyedStream = inputStream
                .selectKey((key, msg) -> msg.sessionId());

        // ── Join: message ⟕ session-context (⟕ memoir-context if enabled) ──
        KStream<String, FullSessionContext> enriched = keyedStream
                .leftJoin(
                        contextTable,
                        EnrichInputMessageProcessor::enrichWithContext,
                        Joined.with(
                                Serdes.String(),
                                JsonSerde.of(MessageInput.class),
                                JsonSerde.of(SessionContext.class)
                        )
                );

        // If memoir is enabled, re-key by userId, join with memoir, re-key back
        if (memoirTable != null) {
            enriched = enriched
                    .selectKey((sessionId, full) ->
                            full.userId() != null ? full.userId() : sessionId)
                    .leftJoin(
                            memoirTable,
                            EnrichInputMessageProcessor::enrichWithMemoir,
                            Joined.with(
                                    Serdes.String(),
                                    JsonSerde.of(FullSessionContext.class),
                                    Serdes.String()
                            )
                    )
                    .selectKey((userId, full) -> full.sessionId());
        }

        enriched
                .peek((sessionId, full) ->
                        log.info("[{}] Enriched — history_size={} has_memoir={} first_turn={}",
                                sessionId,
                                full.history().size(),
                                full.memoirContext() != null,
                                full.history().isEmpty()))
                .to(Topics.ENRICHED_MESSAGE_INPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(FullSessionContext.class)));
    }

    /**
     * First join: merge incoming message with session history.
     */
    static FullSessionContext enrichWithContext(MessageInput message, SessionContext context) {
        List<MessageInput> history = (context != null && context.history() != null)
                ? context.history()
                : List.of();

        String userId = (message.userId() != null && !message.userId().isBlank())
                ? message.userId()
                : (context != null ? context.userId() : null);

        return new FullSessionContext(
                message.sessionId(),
                userId,
                history,
                message,
                null,
                Instant.now().toString()
        );
    }

    /**
     * Second join: attach memoir context to the already-enriched session context.
     */
    static FullSessionContext enrichWithMemoir(FullSessionContext enriched, String memoir) {
        return new FullSessionContext(
                enriched.sessionId(),
                enriched.userId(),
                enriched.history(),
                enriched.latestInput(),
                memoir,
                enriched.timestamp()
        );
    }
}
