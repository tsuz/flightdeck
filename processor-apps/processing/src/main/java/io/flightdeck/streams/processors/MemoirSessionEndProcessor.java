package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MemoirSessionEnd;
import io.flightdeck.streams.model.MessageInput;
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
 * Joins three sources when a session ends:
 *
 * <pre>
 *   session-end              (KStream)  — triggers the join
 *   think-request-response   (KTable)   — last turn's state (contains full history)
 *   memoir-context           (KTable)   — long-term user memoir (keyed by user_id)
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

    public static void register(StreamsBuilder builder,
                                KTable<String, String> memoirTable,
                                KTable<String, ThinkResponse> thinkTable) {

        // ── KStream: session-end events (triggers the join) ─────────────────
        KStream<String, String> sessionEnd = builder.stream(
                Topics.SESSION_END,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── Step 1: join session-end ⟕ think-request-response (both keyed by session_id)
        KStream<String, ThinkResponse> withThink = sessionEnd
                .leftJoin(
                        thinkTable,
                        (endVal, think) -> think,
                        Joined.with(Serdes.String(), Serdes.String(), JsonSerde.of(ThinkResponse.class))
                );

        // ── Tombstone the think-request-response entry (keyed by session_id) ─
        withThink.mapValues(v -> (ThinkResponse) null)
                .to(
                        Topics.THINK_REQUEST_RESPONSE,
                        Produced.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
                );

        // ── Step 2: re-key by userId, join with memoir-context (keyed by userId) ─
        KStream<String, MemoirSessionEnd> joined = withThink
                .filter((sessionId, think) -> think != null)
                .selectKey((sessionId, think) ->
                        think.userId() != null ? think.userId() : sessionId)
                .leftJoin(
                        memoirTable,
                        MemoirSessionEndProcessor::buildSnapshot,
                        Joined.with(Serdes.String(), JsonSerde.of(ThinkResponse.class), Serdes.String())
                )
                .peek((userId, snapshot) -> {
                    int historySize = snapshot.thinkResponse() != null
                            && snapshot.thinkResponse().lastInputResponse() != null
                            ? snapshot.thinkResponse().lastInputResponse().size() : 0;
                    log.info("[{}] Memoir session-end snapshot: has_memoir={} response_size={}",
                            userId,
                            snapshot.memoirContext() != null && !snapshot.memoirContext().isEmpty(),
                            historySize);
                });

        // ── Output joined snapshot ──────────────────────────────────────────
        joined.to(
                Topics.MEMOIR_CONTEXT_SESSION_END,
                Produced.with(Serdes.String(), JsonSerde.of(MemoirSessionEnd.class))
        );

        log.info("MemoirSessionEndProcessor registered");
    }

    /**
     * Builds a {@link MemoirSessionEnd} snapshot from the last ThinkResponse and memoir.
     * Reconstructs full history from previousMessages + lastInputMessage + lastInputResponse
     * and wraps it in a ThinkResponse for the memoir updater.
     */
    static MemoirSessionEnd buildSnapshot(ThinkResponse think, String memoirCtx) {
        if (think == null) {
            return new MemoirSessionEnd(null, null, memoirCtx != null ? memoirCtx : "",
                    null, Instant.now().toString());
        }

        // Reconstruct full history for the memoir updater
        List<MessageInput> fullHistory = new ArrayList<>();
        if (think.previousMessages() != null) fullHistory.addAll(think.previousMessages());
        if (think.lastInputMessage() != null) fullHistory.add(think.lastInputMessage());
        if (think.lastInputResponse() != null) fullHistory.addAll(think.lastInputResponse());

        ThinkResponse asResponse = new ThinkResponse(
                think.sessionId(), think.userId(), think.cost(), think.prevSessionCost(),
                0, 0,
                fullHistory,     // previousMessages = full history for memoir
                null, null,      // lastInputMessage, lastInputResponse not needed for memoir
                null, true, false, 0, 0, 0.0, Instant.now().toString());

        return new MemoirSessionEnd(
                think.sessionId(),
                think.userId(),
                memoirCtx != null ? memoirCtx : "",
                asResponse,
                Instant.now().toString()
        );
    }
}
