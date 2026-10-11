package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.SessionStatus;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.model.UserResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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
import java.util.List;

/**
 * <h2>Session Concurrency Guard Processor</h2>
 *
 * <p>Admission control placed <em>before</em> {@link EnrichInputMessageProcessor}.
 * It rejects a new user message for a session whose previous turn is still in
 * progress, and lets the system's own tool-result continuation through.
 *
 * <h3>Why</h3>
 * History for the next LLM call is rebuilt as
 * {@code previousMessages + [lastInputMessage] + lastInputResponse}
 * (see {@link EnrichInputMessageProcessor}). When the previous turn issued tools,
 * {@code lastInputResponse} ends with an assistant message carrying
 * {@code tool_use} blocks. The matching {@code tool_result} blocks arrive as a
 * {@code message-input} record with {@code role:"tool"}. If a {@code role:"user"}
 * message is appended <em>instead</em>, the LLM sees a {@code tool_use} that is
 * not immediately followed by its {@code tool_result} and returns a 400 — killing
 * the turn. This guard prevents that by dropping the premature user message.
 *
 * <h3>Topology</h3>
 * <pre>
 *   message-input ─(re-key by session_id)─┐
 *                                          ├─►[guard]─┬─► admitted-message-input ─►[enrich]─►…
 *   think-request-response (KStream) ──────┘   │      │
 *                      owns: session-status-store    └─(⋈ reply-to)─► message-output  (rejection)
 * </pre>
 *
 * <p>Admitted messages are re-published to {@code admitted-message-input}, which
 * {@link EnrichInputMessageProcessor} consumes in place of {@code message-input}.
 * The intermediate topic keeps the guard a self-contained fragment and gives the
 * admitted stream a natural session_id partitioning for downstream joins.
 * Dedupe/admission state is solely the session-keyed {@code session-status-store}.
 *
 * <p>Rejections are left-joined with the per-session reply-to route before
 * publication, mirroring {@link EndTurnProcessor}: a multi-agent caller whose
 * message is rejected fails fast through its HTTP callback (and the one-shot
 * route is tombstoned on delivery by the OutputConsumer) instead of hanging
 * until its own timeout — which would also leave a stale route that the
 * in-flight turn's reply could be mis-delivered to.
 *
 * <h3>Busy lifecycle (per session)</h3>
 * <ul>
 *   <li>A {@code role:"user"} message when <b>not busy</b> → admitted; session
 *       marked busy. When <b>busy</b> → rejected (a {@code status:"rejected"}
 *       {@link UserResponse} is emitted to {@code message-output}).</li>
 *   <li>A {@code role:"tool"} continuation → always admitted; activity refreshed.
 *       Never rejected.</li>
 *   <li>A {@code think-request-response} that is a clean end-turn
 *       ({@code endTurn} with no tool calls) → busy cleared (turn complete).
 *       Otherwise busy is kept and activity refreshed (tools still pending).</li>
 *   <li>A wall-clock punctuator force-clears any session idle longer than
 *       {@link #SESSION_BUSY_TTL_MS} — the safety net that unwedges a session
 *       whose turn crashed or whose end-turn signal was lost, so a session can
 *       never be locked permanently.</li>
 * </ul>
 */
public class SessionConcurrencyGuardProcessor {

    private static final Logger log = LoggerFactory.getLogger(SessionConcurrencyGuardProcessor.class);

    /** RocksDB store: session_id → {@link SessionStatus}. */
    public static final String STATUS_STORE = "session-status-store";

    /** Role that marks the system's own tool-result continuation — never rejected. */
    static final String ROLE_TOOL = "tool";

    /** Content delivered to the caller when a message is rejected for being mid-turn. */
    static final String REJECTION_CONTENT =
            "The agent is still processing your previous request. Please wait for it to finish before sending another message.";

    /** Status flag stamped onto a rejection {@link UserResponse}. */
    static final String STATUS_REJECTED = "rejected";

    /**
     * Idle timeout after which a busy session is force-cleared. Must exceed the
     * longest legitimate quiet stretch within a turn — the async tool timeout
     * ({@code ASYNC_TOOL_TIMEOUT_MS}, default 300s) — plus margin.
     */
    static final long SESSION_BUSY_TTL_MS =
            envLong("SESSION_BUSY_TTL_MS", 600_000L);

    /** How often the wall-clock punctuator scans for idle/wedged sessions. */
    static final long PUNCTUATE_INTERVAL_MS =
            envLong("SESSION_GUARD_PUNCTUATE_INTERVAL_MS", 15_000L);

    /**
     * Registers the guard: consumes {@code message-input} and produces the
     * admitted subset (keyed by session_id) to {@code admitted-message-input},
     * plus a rejection to {@code message-output} for anything dropped.
     */
    public static void register(StreamsBuilder builder,
                                KStream<String, ThinkResponse> thinkStream,
                                KTable<String, String> replyToTable) {

        StoreBuilder<KeyValueStore<String, SessionStatus>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATUS_STORE),
                        Serdes.String(),
                        JsonSerde.of(SessionStatus.class)
                );
        builder.addStateStore(storeBuilder);

        // Incoming messages, re-keyed by session_id (mirrors EnrichInputMessageProcessor).
        KStream<String, GuardEvent> messageEvents = builder
                .stream(Topics.MESSAGE_INPUT,
                        Consumed.with(Serdes.String(), JsonSerde.of(MessageInput.class)))
                .selectKey((key, msg) -> msg.sessionId())
                .mapValues(GuardEvent::message);

        // Think responses drive the busy lifecycle (clear on clean end-turn).
        KStream<String, GuardEvent> thinkEvents = thinkStream
                .filter((sessionId, response) -> response != null)
                .mapValues(GuardEvent::think);

        KStream<String, GuardResult> processed = messageEvents
                .merge(thinkEvents)
                .process(
                        (ProcessorSupplier<String, GuardEvent,
                                           String, GuardResult>) GuardProcessor::new,
                        STATUS_STORE
                );

        // Admitted messages continue to enrichment via the intermediate topic.
        processed
                .filter((sessionId, result) -> result != null && result.admitted() != null)
                .mapValues(GuardResult::admitted)
                .to(Topics.ADMITTED_MESSAGE_INPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(MessageInput.class)));

        // Rejections are delivered back through the normal output channel, with
        // the session's reply-to route joined in (as EndTurnProcessor does) so a
        // multi-agent caller gets the rejection via its HTTP callback.
        processed
                .filter((sessionId, result) -> result != null && result.rejection() != null)
                .mapValues(GuardResult::rejection)
                .leftJoin(replyToTable, SessionConcurrencyGuardProcessor::withReplyRoute,
                        Joined.with(Serdes.String(), JsonSerde.of(UserResponse.class), Serdes.String()))
                .to(Topics.MESSAGE_OUTPUT,
                        Produced.with(Serdes.String(), JsonSerde.of(UserResponse.class)));
    }

    /**
     * Embeds the session's reply-to route (raw JSON from the reply-to table)
     * into a rejection. Null/absent/invalid route → the rejection passes through
     * unchanged and is delivered over the default (WebSocket) channel.
     */
    static UserResponse withReplyRoute(UserResponse rejection, String replyJson) {
        java.util.Map<String, Object> reply = EndTurnProcessor.parseReply(replyJson);
        if (reply == null) return rejection;
        return new UserResponse(
                rejection.sessionId(),
                rejection.userId(),
                rejection.content(),
                rejection.inputTokens(),
                rejection.outputTokens(),
                rejection.cost(),
                rejection.sourceAgent(),
                reply,
                rejection.status(),
                rejection.timestamp());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal merged-event wrapper (never hits a topic — no serde needed)
    // ─────────────────────────────────────────────────────────────────────────

    enum Kind { MESSAGE, THINK }

    record GuardEvent(Kind kind, MessageInput message, ThinkResponse think) {
        static GuardEvent message(MessageInput m) { return new GuardEvent(Kind.MESSAGE, m, null); }
        static GuardEvent think(ThinkResponse t)  { return new GuardEvent(Kind.THINK, null, t); }
    }

    /** Output wrapper: exactly one of {@code admitted} / {@code rejection} is set. */
    record GuardResult(MessageInput admitted, UserResponse rejection) {
        static GuardResult admit(MessageInput m)   { return new GuardResult(m, null); }
        static GuardResult reject(UserResponse u)  { return new GuardResult(null, u); }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Processor
    // ─────────────────────────────────────────────────────────────────────────

    static class GuardProcessor implements Processor<String, GuardEvent, String, GuardResult> {

        private ProcessorContext<String, GuardResult> context;
        private KeyValueStore<String, SessionStatus> store;

        @Override
        public void init(ProcessorContext<String, GuardResult> ctx) {
            this.context = ctx;
            this.store = ctx.getStateStore(STATUS_STORE);
            ctx.schedule(Duration.ofMillis(PUNCTUATE_INTERVAL_MS),
                    PunctuationType.WALL_CLOCK_TIME, this::sweep);
        }

        @Override
        public void process(Record<String, GuardEvent> record) {
            String key = record.key();
            GuardEvent event = record.value();
            if (key == null || event == null) {
                log.warn("Null key or event — skipping");
                return;
            }
            if (event.kind() == Kind.MESSAGE) {
                handleMessage(key, event.message(), record.timestamp());
            } else {
                handleThink(key, event.think());
            }
        }

        // ── Incoming message: admit or reject ──────────────────────────────────
        private void handleMessage(String sessionId, MessageInput msg, long ts) {
            if (msg == null) {
                log.warn("[{}] Null message — skipping", sessionId);
                return;
            }
            long now = System.currentTimeMillis();
            SessionStatus current = store.get(sessionId);

            // The system's own tool-result continuation is always admitted — it is
            // precisely the record that completes the pending tool_use blocks.
            if (ROLE_TOOL.equals(msg.role())) {
                if (current != null && current.busy()) {
                    store.put(sessionId, new SessionStatus(
                            true, current.turnStartedMs(), now, Instant.now().toString()));
                }
                context.forward(new Record<>(sessionId, GuardResult.admit(msg), ts));
                return;
            }

            // A new user (or scheduler-triggered) message.
            if (current != null && current.busy()) {
                log.warn("[{}] Rejecting message — turn in progress since {}ms ago (role={})",
                        sessionId, now - current.turnStartedMs(), msg.role());
                context.forward(new Record<>(sessionId, GuardResult.reject(rejection(msg)), ts));
                return;
            }

            // Admit and open a new turn.
            store.put(sessionId, new SessionStatus(true, now, now, Instant.now().toString()));
            log.info("[{}] Admitted message — turn started (role={})", sessionId, msg.role());
            context.forward(new Record<>(sessionId, GuardResult.admit(msg), ts));
        }

        // ── Think response: clear busy on clean end-turn, else keep busy ───────
        private void handleThink(String sessionId, ThinkResponse response) {
            if (response == null) return;
            long now = System.currentTimeMillis();

            boolean noToolCalls = response.toolUses() == null || response.toolUses().isEmpty();
            boolean cleanEndTurn = response.endTurn() && noToolCalls;

            if (cleanEndTurn) {
                if (store.get(sessionId) != null) {
                    store.delete(sessionId);
                    log.info("[{}] Turn complete — session marked idle", sessionId);
                }
                return;
            }

            // Tools still pending (or not yet end-of-turn): keep the session busy
            // and refresh activity so a long, legitimately-active turn is not swept.
            SessionStatus current = store.get(sessionId);
            long turnStarted = (current != null) ? current.turnStartedMs() : now;
            store.put(sessionId, new SessionStatus(true, turnStarted, now, Instant.now().toString()));
        }

        // ── Wall-clock sweep: force-clear wedged / idle sessions ───────────────
        private void sweep(long now) {
            List<String> toClear = new ArrayList<>();
            try (KeyValueIterator<String, SessionStatus> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, SessionStatus> kv = it.next();
                    SessionStatus s = kv.value;
                    if (s == null) continue;
                    if (now - s.lastActivityMs() > SESSION_BUSY_TTL_MS) {
                        toClear.add(kv.key);
                    }
                }
            }
            for (String key : toClear) {
                store.delete(key);
                log.warn("[{}] Session idle > {}ms — force-clearing busy state (turn likely crashed)",
                        key, SESSION_BUSY_TTL_MS);
            }
        }

        private static UserResponse rejection(MessageInput msg) {
            return new UserResponse(
                    msg.sessionId(),
                    msg.userId(),
                    REJECTION_CONTENT,
                    0, 0, 0.0,
                    null,            // source_agent — system-generated, not from an agent
                    null,            // reply_to — joined in downstream (withReplyRoute)
                    STATUS_REJECTED,
                    Instant.now().toString());
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
