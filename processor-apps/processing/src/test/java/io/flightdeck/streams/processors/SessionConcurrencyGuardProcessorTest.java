package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.model.ToolUseItem;
import io.flightdeck.streams.model.UserResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link SessionConcurrencyGuardProcessor}.
 *
 * Invariants under test:
 *   - First user message is admitted and opens a turn.
 *   - A second user message while the turn is in progress is rejected (and a
 *     status:"rejected" UserResponse is emitted to message-output).
 *   - The system's own role:"tool" continuation is always admitted, even mid-turn.
 *   - A clean end-turn think response clears busy so the next user message is admitted.
 *   - A think response that still carries tool calls keeps the session busy.
 *   - A session whose turn never completes is force-cleared after the idle TTL.
 *   - Sessions are isolated from one another.
 */
class SessionConcurrencyGuardProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, MessageInput>   messageInput;
    private TestInputTopic<String, ThinkResponse>  thinkInput;
    private TestInputTopic<String, String>         replyToInput;
    private TestOutputTopic<String, MessageInput>  admittedOutput;
    private TestOutputTopic<String, UserResponse>  rejectionOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));

        KTable<String, String> replyToTable = builder.table(
                Topics.REPLY_TO,
                Consumed.with(Serdes.String(), Serdes.String()));

        SessionConcurrencyGuardProcessor.register(builder, thinkStream, replyToTable);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-session-guard");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        // Anchor the mock wall clock so deadlines (System.currentTimeMillis())
        // line up with advanceWallClockTime().
        driver = new TopologyTestDriver(builder.build(), props, Instant.now());

        messageInput = driver.createInputTopic(
                Topics.MESSAGE_INPUT,
                Serdes.String().serializer(),
                JsonSerde.of(MessageInput.class).serializer());

        thinkInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        replyToInput = driver.createInputTopic(
                Topics.REPLY_TO,
                Serdes.String().serializer(),
                Serdes.String().serializer());

        admittedOutput = driver.createOutputTopic(
                Topics.ADMITTED_MESSAGE_INPUT,
                Serdes.String().deserializer(),
                JsonSerde.of(MessageInput.class).deserializer());

        rejectionOutput = driver.createOutputTopic(
                Topics.MESSAGE_OUTPUT,
                Serdes.String().deserializer(),
                JsonSerde.of(UserResponse.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Admit / reject ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("First user message is admitted; no rejection emitted")
    void firstMessage_admitted() {
        messageInput.pipeInput("s1", userMsg("s1", "u", "hello"));

        assertThat(admittedOutput.isEmpty()).isFalse();
        assertThat(admittedOutput.readRecord().value().content()).isEqualTo("hello");
        assertThat(rejectionOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Second user message while a turn is in progress is rejected")
    void secondMessage_whileBusy_rejected() {
        messageInput.pipeInput("s2", userMsg("s2", "u", "first"));
        admittedOutput.readRecord(); // first admitted

        messageInput.pipeInput("s2", userMsg("s2", "u", "second"));

        // The second message must NOT be admitted...
        assertThat(admittedOutput.isEmpty()).isTrue();
        // ...and a rejection notice must be emitted instead.
        assertThat(rejectionOutput.isEmpty()).isFalse();
        UserResponse rejection = rejectionOutput.readRecord().value();
        assertThat(rejection.sessionId()).isEqualTo("s2");
        assertThat(rejection.status()).isEqualTo("rejected");
        assertThat(rejection.cost()).isEqualTo(0.0);
        assertThat(rejection.content()).isNotBlank();
        // No reply-to route stored → delivered over the default (WebSocket) channel.
        assertThat(rejection.replyTo()).isNull();
    }

    @Test
    @DisplayName("Rejection for a multi-agent caller carries the session's reply-to route")
    void rejection_carriesReplyToRoute() {
        replyToInput.pipeInput("s2r", "{\"callbackService\":\"caller-agent\",\"token\":\"tok-1\"}");

        messageInput.pipeInput("s2r", userMsg("s2r", "u", "first"));
        admittedOutput.readRecord();

        messageInput.pipeInput("s2r", userMsg("s2r", "u", "second"));

        UserResponse rejection = rejectionOutput.readRecord().value();
        assertThat(rejection.status()).isEqualTo("rejected");
        assertThat(rejection.replyTo())
                .containsEntry("callbackService", "caller-agent")
                .containsEntry("token", "tok-1");
    }

    // ── Tool continuation is never rejected ──────────────────────────────────────

    @Test
    @DisplayName("role:tool continuation is admitted even while the turn is in progress")
    void toolContinuation_admittedWhileBusy() {
        messageInput.pipeInput("s3", userMsg("s3", "u", "do something"));
        admittedOutput.readRecord(); // user message admitted, session now busy

        // The pipeline's own tool-result continuation re-enters message-input.
        messageInput.pipeInput("s3", toolMsg("s3", "u"));

        assertThat(admittedOutput.isEmpty()).isFalse();
        assertThat(admittedOutput.readRecord().value().role()).isEqualTo("tool");
        // A continuation is not a rejection.
        assertThat(rejectionOutput.isEmpty()).isTrue();
    }

    // ── Busy lifecycle via think responses ───────────────────────────────────────

    @Test
    @DisplayName("Clean end-turn clears busy so the next user message is admitted")
    void cleanEndTurn_clearsBusy() {
        messageInput.pipeInput("s4", userMsg("s4", "u", "first"));
        admittedOutput.readRecord();

        // LLM finishes the turn with no outstanding tool calls.
        thinkInput.pipeInput("s4", endTurn("s4", "u"));

        // A follow-up user message is now admitted, not rejected.
        messageInput.pipeInput("s4", userMsg("s4", "u", "second"));
        assertThat(rejectionOutput.isEmpty()).isTrue();
        assertThat(admittedOutput.readRecord().value().content()).isEqualTo("second");
    }

    @Test
    @DisplayName("Think response that still carries tool calls keeps the session busy")
    void toolCallsPending_keepsBusy() {
        messageInput.pipeInput("s5", userMsg("s5", "u", "first"));
        admittedOutput.readRecord();

        // LLM responded with a tool_use — turn is NOT complete.
        thinkInput.pipeInput("s5", withTools("s5", "u",
                new ToolUseItem("t1", "lookup", "lookup", Map.of(), "s5", 0, TS)));

        // A new user message must still be rejected.
        messageInput.pipeInput("s5", userMsg("s5", "u", "second"));
        assertThat(admittedOutput.isEmpty()).isTrue();
        assertThat(rejectionOutput.readRecord().value().status()).isEqualTo("rejected");
    }

    @Test
    @DisplayName("A think response that opens a turn before any message keeps the session busy")
    void thinkWithoutPriorAdmit_marksBusy() {
        // Defensive: a think response (e.g. after a restart) with tools pending,
        // with no prior admit recorded, still locks the session.
        thinkInput.pipeInput("s5b", withTools("s5b", "u",
                new ToolUseItem("t1", "lookup", "lookup", Map.of(), "s5b", 0, TS)));

        messageInput.pipeInput("s5b", userMsg("s5b", "u", "barge in"));
        assertThat(admittedOutput.isEmpty()).isTrue();
        assertThat(rejectionOutput.readRecord().value().status()).isEqualTo("rejected");
    }

    // ── TTL safety net ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("A wedged turn is force-cleared after the idle TTL, unblocking the session")
    void wedgedTurn_forceClearedAfterTtl() {
        messageInput.pipeInput("s6", userMsg("s6", "u", "first"));
        admittedOutput.readRecord();

        // No end-turn ever arrives (think consumer crashed). Advance past the TTL.
        driver.advanceWallClockTime(Duration.ofMinutes(11));

        // The session is no longer busy: a new message is admitted.
        messageInput.pipeInput("s6", userMsg("s6", "u", "second"));
        assertThat(rejectionOutput.isEmpty()).isTrue();
        assertThat(admittedOutput.readRecord().value().content()).isEqualTo("second");
    }

    // ── Session isolation ──────────────────────────────────────────────────────

    @Test
    @DisplayName("A busy session does not block a different session")
    void sessionIsolation() {
        messageInput.pipeInput("A", userMsg("A", "u1", "a-first"));
        admittedOutput.readRecord();

        // Different session — must be admitted despite A being busy.
        messageInput.pipeInput("B", userMsg("B", "u2", "b-first"));
        assertThat(rejectionOutput.isEmpty()).isTrue();
        assertThat(admittedOutput.readRecord().value().sessionId()).isEqualTo("B");
    }

    @Test
    @DisplayName("Admitted record is keyed by session_id")
    void admittedKey_isSessionId() {
        messageInput.pipeInput("orig-key", userMsg("key-sess", "u", "hi"));
        assertThat(admittedOutput.readRecord().key()).isEqualTo("key-sess");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static MessageInput userMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "user", content, TS, Map.of());
    }

    private static MessageInput toolMsg(String sessionId, String userId) {
        return new MessageInput(sessionId, userId, "tool", "tool result", TS, Map.of());
    }

    private static ThinkResponse endTurn(String sessionId, String userId) {
        return new ThinkResponse(sessionId, userId, 0.0, 0.0, 0.0, 0, 0,
                List.of(), null, List.of(), List.of(), true, false, 0, 0, 0.0, TS);
    }

    private static ThinkResponse withTools(String sessionId, String userId, ToolUseItem... items) {
        return new ThinkResponse(sessionId, userId, 0.0, 0.0, 0.0, 0, 0,
                List.of(), null, List.of(), List.of(items), false, false, 0, 0, 0.0, TS);
    }
}
