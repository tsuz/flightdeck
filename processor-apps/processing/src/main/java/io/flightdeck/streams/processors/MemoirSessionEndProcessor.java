package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MemoirSessionEnd;
import io.flightdeck.streams.model.SessionContext;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Joins three sources when a session ends:
 *
 * <pre>
 *   session-end        (KStream)  — triggers the join
 *   session-context    (KTable)   — full conversation history (keyed by session_id)
 *   memoir-context     (KTable)   — long-term user memoir (keyed by user_id)
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
     * @param memoirTable   shared KTable for memoir-context (keyed by userId)
     * @param contextTable  shared KTable for session-context (keyed by sessionId)
     * @param thinkTable    shared KTable for think-request-response (keyed by sessionId)
     */
    public static void register(StreamsBuilder builder,
                                KTable<String, String> memoirTable,
                                KTable<String, SessionContext> contextTable,
                                KTable<String, ThinkResponse> thinkTable) {

        // ── KStream: session-end events (triggers the join) ─────────────────
        KStream<String, String> sessionEnd = builder.stream(
                Topics.SESSION_END,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── Step 1: join session-end ⟕ session-context (both keyed by session_id)
        //    This gives us the full conversation history + userId
        KStream<String, SessionContext> withContext = sessionEnd
                .leftJoin(
                        contextTable,
                        (endVal, ctx) -> ctx,
                        Joined.with(Serdes.String(), Serdes.String(), JsonSerde.of(SessionContext.class))
                );

        // ── Tombstone the think-request-response entry (keyed by session_id) ─
        withContext.mapValues(v -> (ThinkResponse) null)
                .to(
                        Topics.THINK_REQUEST_RESPONSE,
                        Produced.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
                );

        // ── Step 2: re-key by userId, join with memoir-context (keyed by userId) ─
        KStream<String, MemoirSessionEnd> joined = withContext
                .filter((sessionId, ctx) -> ctx != null)
                .selectKey((sessionId, ctx) ->
                        ctx.userId() != null ? ctx.userId() : sessionId)
                .leftJoin(
                        memoirTable,
                        MemoirSessionEndProcessor::buildSnapshot,
                        Joined.with(Serdes.String(), JsonSerde.of(SessionContext.class), Serdes.String())
                )
                .peek((userId, snapshot) ->
                        log.info("[{}] Memoir session-end snapshot: has_memoir={} history_size={}",
                                userId,
                                snapshot.memoirContext() != null && !snapshot.memoirContext().isEmpty(),
                                snapshot.thinkResponse() != null
                                        ? snapshot.thinkResponse().messages() != null
                                                ? snapshot.thinkResponse().messages().size() : 0
                                        : 0));

        // ── Output joined snapshot ──────────────────────────────────────────
        joined.to(
                Topics.MEMOIR_CONTEXT_SESSION_END,
                Produced.with(Serdes.String(), JsonSerde.of(MemoirSessionEnd.class))
        );

        log.info("MemoirSessionEndProcessor registered");
    }

    /**
     * Builds a {@link MemoirSessionEnd} snapshot from session context and memoir.
     * Converts the {@link SessionContext} history into a synthetic {@link ThinkResponse}
     * so the memoir updater sees the full conversation.
     */
    static MemoirSessionEnd buildSnapshot(SessionContext ctx, String memoirCtx) {
        String sessionId = ctx != null ? ctx.sessionId() : null;
        String userId = ctx != null ? ctx.userId() : null;

        // Wrap the full history into a ThinkResponse for the memoir updater
        ThinkResponse asResponse = ctx != null
                ? new ThinkResponse(
                        ctx.sessionId(), ctx.userId(), ctx.cost(), null,
                        0, 0, ctx.history(), null, true,
                        Instant.now().toString())
                : null;

        return new MemoirSessionEnd(
                sessionId,
                userId,
                memoirCtx != null ? memoirCtx : "",
                asResponse,
                Instant.now().toString()
        );
    }
}
