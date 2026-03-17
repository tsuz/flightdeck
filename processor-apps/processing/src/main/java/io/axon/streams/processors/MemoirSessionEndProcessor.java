package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MemoirSessionEnd;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Joins three sources on session ID when a session ends:
 *
 * <pre>
 *   session-end            (KStream) — triggers the join
 *   memoir-context          (KTable)  — shared, created in AxonStreamsApp
 *   think-request-response  (KTable)  — shared, created in AxonStreamsApp
 *         │
 *         ▼
 *   memoir-context-session-end  (KStream — joined snapshot)
 *         │
 *         ▼
 *   think-request-response  (tombstone — deletes the entry)
 * </pre>
 */
public class MemoirSessionEndProcessor {

    private static final Logger log = LoggerFactory.getLogger(MemoirSessionEndProcessor.class);

    /**
     * @param memoirTable shared KTable created in AxonStreamsApp
     * @param thinkTable  shared KTable derived from think-request-response in AxonStreamsApp
     */
    public static void register(StreamsBuilder builder,
                                KTable<String, String> memoirTable,
                                KTable<String, ThinkResponse> thinkTable) {

        // ── KStream: session-end events (triggers the join) ─────────────────
        KStream<String, String> sessionEnd = builder.stream(
                Topics.SESSION_END,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── Three-way join: session-end ⟕ memoir-context ⟕ think-response ──
        KStream<String, MemoirSessionEnd> joined = sessionEnd
                .leftJoin(
                        memoirTable,
                        (endVal, memoir) -> memoir != null ? memoir : "",
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .leftJoin(
                        thinkTable,
                        MemoirSessionEndProcessor::buildSnapshot,
                        Joined.with(Serdes.String(), Serdes.String(), JsonSerde.of(ThinkResponse.class))
                )
                .peek((sessionId, snapshot) ->
                        log.info("[{}] Memoir session-end snapshot: has_memoir={} has_response={}",
                                sessionId,
                                snapshot.memoirContext() != null,
                                snapshot.thinkResponse() != null));

        // ── Output joined snapshot ──────────────────────────────────────────
        joined.to(
                Topics.MEMOIR_CONTEXT_SESSION_END,
                Produced.with(Serdes.String(), JsonSerde.of(MemoirSessionEnd.class))
        );

        // ── Tombstone the think-request-response entry ──────────────────────
        joined.mapValues(v -> (ThinkResponse) null)
                .to(
                        Topics.THINK_REQUEST_RESPONSE,
                        Produced.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
                );

        log.info("MemoirSessionEndProcessor registered");
    }

    /**
     * Combines the memoir context string and the last ThinkResponse into a
     * {@link MemoirSessionEnd} snapshot. Both sides may be null (left join).
     */
    static MemoirSessionEnd buildSnapshot(String memoirCtx, ThinkResponse response) {
        String sessionId = response != null ? response.sessionId() : null;
        return new MemoirSessionEnd(
                sessionId,
                memoirCtx,
                response,
                Instant.now().toString()
        );
    }
}
