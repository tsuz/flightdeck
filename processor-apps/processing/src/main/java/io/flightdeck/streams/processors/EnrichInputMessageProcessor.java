package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.FullSessionContext;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * <h2>Enrich Input Message Processor</h2>
 *
 * <h3>Topology</h3>
 * <pre>
 *   message-input  (KStream — raw user turn, keyed by session_id)
 *         │
 *         │   leftJoin
 *         │◄──────────────  think-request-response (KTable — previous turn's full state)
 *         │
 *         │   leftJoin
 *         │◄──────────────  memoir-context   (KTable — long-term memoir, shared)
 *         │
 *         ▼
 *   enriched-message-input  (KStream — history + memoir + latest input)
 * </pre>
 *
 * <p>History is reconstructed from the previous ThinkResponse:
 * {@code previousMessages + [lastInputMessage] + lastInputResponse}.
 */
public class EnrichInputMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(EnrichInputMessageProcessor.class);

    /**
     * @param memoirTable      shared KTable for memoir-context (keyed by userId)
     * @param thinkTable       shared KTable for think-request-response (keyed by sessionId)
     */
    public static void register(StreamsBuilder builder,
                                KTable<String, String> memoirTable,
                                KTable<String, ThinkResponse> thinkTable) {

        // ── Left side: incoming user messages ────────────────────────────────
        KStream<String, MessageInput> inputStream = builder.stream(
                Topics.MESSAGE_INPUT,
                Consumed.with(Serdes.String(), JsonSerde.of(MessageInput.class))
        );

        // ── Re-key by session_id from the message value ──────────────────────
        KStream<String, MessageInput> keyedStream = inputStream
                .selectKey((key, msg) -> msg.sessionId());

        // ── Join: message ⟕ think-request-response (build history from previous turn)
        KStream<String, FullSessionContext> enriched = keyedStream
                .leftJoin(
                        thinkTable,
                        EnrichInputMessageProcessor::enrichWithThinkResponse,
                        Joined.with(
                                Serdes.String(),
                                JsonSerde.of(MessageInput.class),
                                JsonSerde.of(ThinkResponse.class)
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
     * Build history from the previous ThinkResponse:
     * {@code previousMessages + [lastInputMessage] + lastInputResponse}.
     */
    static FullSessionContext enrichWithThinkResponse(MessageInput message, ThinkResponse prevResponse) {
        List<MessageInput> history;

        if (prevResponse == null) {
            history = List.of();
        } else {
            history = new ArrayList<>();
            if (prevResponse.previousMessages() != null) {
                history.addAll(prevResponse.previousMessages());
            }
            if (prevResponse.lastInputMessage() != null) {
                history.add(prevResponse.lastInputMessage());
            }
            if (prevResponse.lastInputResponse() != null) {
                history.addAll(prevResponse.lastInputResponse());
            }
        }

        String userId = (message.userId() != null && !message.userId().isBlank())
                ? message.userId()
                : (prevResponse != null ? prevResponse.userId() : null);

        return new FullSessionContext(
                message.sessionId(),
                userId,
                (prevResponse != null) ? prevResponse.totalSessionCost() : null,
                history,
                message,
                null,
                Instant.now().toString()
        );
    }

    /**
     * Join: attach memoir context to the already-enriched session context.
     */
    static FullSessionContext enrichWithMemoir(FullSessionContext enriched, String memoir) {
        return new FullSessionContext(
                enriched.sessionId(),
                enriched.userId(),
                enriched.cost(),
                enriched.history(),
                enriched.latestInput(),
                memoir,
                enriched.timestamp()
        );
    }
}
