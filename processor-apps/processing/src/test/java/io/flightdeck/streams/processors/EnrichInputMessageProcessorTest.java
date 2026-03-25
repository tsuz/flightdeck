package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.*;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link EnrichInputMessageProcessor}.
 *
 * The topology under test:
 *   message-input  (KStream)
 *       leftJoin
 *   message-context (KTable)
 *       ▼
 *   enriched-message-input (KStream)
 */
class EnrichInputMessageProcessorTest {

    private TopologyTestDriver driver;

    /** Drives raw user messages onto message-input */
    private TestInputTopic<String, MessageInput>   messageInput;

    /** Seeds the message-context KTable directly */
    private TestInputTopic<String, SessionContext>  contextInput;

    /** Captures the enriched FullSessionContext records */
    private TestOutputTopic<String, FullSessionContext> fullContextOutput;

    /** Seeds the memoir-context KTable */
    private TestInputTopic<String, String> memoirInput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create shared KTables (same as FlightDeckStreamsApp does)
        KTable<String, String> memoirTable = builder.table(
                Topics.MEMOIR_CONTEXT,
                Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, SessionContext> contextTable = builder.table(
                Topics.SESSION_CONTEXT,
                Consumed.with(Serdes.String(), JsonSerde.of(SessionContext.class)));
        KTable<String, SessionCost> sessionCostTable = builder.table(
                Topics.SESSION_COST,
                Consumed.with(Serdes.String(), JsonSerde.of(SessionCost.class)));

        EnrichInputMessageProcessor.register(builder, memoirTable, contextTable, sessionCostTable);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-enrich");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        messageInput = driver.createInputTopic(
                Topics.MESSAGE_INPUT,
                Serdes.String().serializer(),
                JsonSerde.of(MessageInput.class).serializer());

        // Seed the KTable by writing directly to the message-context topic
        contextInput = driver.createInputTopic(
                Topics.SESSION_CONTEXT,
                Serdes.String().serializer(),
                JsonSerde.of(SessionContext.class).serializer());

        memoirInput = driver.createInputTopic(
                Topics.MEMOIR_CONTEXT,
                Serdes.String().serializer(),
                Serdes.String().serializer());

        fullContextOutput = driver.createOutputTopic(
                Topics.ENRICHED_MESSAGE_INPUT,
                Serdes.String().deserializer(),
                JsonSerde.of(FullSessionContext.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Left-join behaviour ───────────────────────────────────────────────────

    @Test
    @DisplayName("First-ever user message (no prior context) produces empty history")
    void firstMessage_noContext_emptyHistory() {
        messageInput.pipeInput("sess-new", userMsg("sess-new", "user-Z", "Hello!"));

        FullSessionContext full = fullContextOutput.readRecord().value();

        assertThat(full.sessionId()).isEqualTo("sess-new");
        assertThat(full.userId()).isEqualTo("user-Z");
        assertThat(full.history()).isEmpty();
        assertThat(full.latestInput().content()).isEqualTo("Hello!");
    }

    @Test
    @DisplayName("Message with existing context carries the full history")
    void messageWithContext_includesHistory() {
        // Seed the KTable with two prior turns
        SessionContext ctx = context("sess-h", "user-A", List.of(
                assistantMsg("sess-h", "user-A", "First reply."),
                assistantMsg("sess-h", "user-A", "Second reply.")
        ));
        contextInput.pipeInput("sess-h", ctx);

        // Now a new user message arrives
        messageInput.pipeInput("sess-h", userMsg("sess-h", "user-A", "Follow-up?"));

        FullSessionContext full = fullContextOutput.readRecord().value();

        assertThat(full.history()).hasSize(2);
        assertThat(full.history()).extracting(MessageInput::content)
                .containsExactly("First reply.", "Second reply.");
        assertThat(full.latestInput().content()).isEqualTo("Follow-up?");
    }

    @Test
    @DisplayName("latestInput is always the incoming message, not part of history")
    void latestInput_isNotInHistory() {
        contextInput.pipeInput("sess-li", context("sess-li", "u", List.of(
                assistantMsg("sess-li", "u", "Prior turn."))));

        messageInput.pipeInput("sess-li", userMsg("sess-li", "u", "New question"));

        FullSessionContext full = fullContextOutput.readRecord().value();
        assertThat(full.history()).hasSize(1);
        assertThat(full.latestInput().content()).isEqualTo("New question");
        // New question must NOT appear in history
        assertThat(full.history()).extracting(MessageInput::content)
                .doesNotContain("New question");
    }

    // ── Key preservation ──────────────────────────────────────────────────────

    @Test
    @DisplayName("Output record key is the session_id from the incoming message")
    void outputKey_isSessionId() {
        messageInput.pipeInput("key-test", userMsg("key-test", "u", "hi"));
        assertThat(fullContextOutput.readRecord().key()).isEqualTo("key-test");
    }

    // ── userId resolution ─────────────────────────────────────────────────────

    @Test
    @DisplayName("userId is taken from the incoming message when present")
    void userId_fromMessage() {
        contextInput.pipeInput("sess-u1", context("sess-u1", "old-user", List.of()));
        messageInput.pipeInput("sess-u1", userMsg("sess-u1", "new-user", "hi"));

        assertThat(fullContextOutput.readRecord().value().userId()).isEqualTo("new-user");
    }

    @Test
    @DisplayName("userId falls back to context value when message carries none")
    void userId_fallsBackToContext() {
        contextInput.pipeInput("sess-u2", context("sess-u2", "ctx-user", List.of()));
        // Message has null userId (e.g. scheduler-triggered input)
        MessageInput schedulerMsg = new MessageInput("sess-u2", null, "user",
                "Scheduled trigger", TS, Map.of());
        messageInput.pipeInput("sess-u2", schedulerMsg);

        assertThat(fullContextOutput.readRecord().value().userId()).isEqualTo("ctx-user");
    }

    @Test
    @DisplayName("userId is null when neither message nor context provides one")
    void userId_nullWhenNoSource() {
        MessageInput msg = new MessageInput("sess-u3", null, "user", "hi", TS, Map.of());
        messageInput.pipeInput("sess-u3", msg);

        assertThat(fullContextOutput.readRecord().value().userId()).isNull();
    }

    // ── Session isolation ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Two sessions receive their own independent histories")
    void sessionIsolation() {
        contextInput.pipeInput("A", context("A", "u1", List.of(assistantMsg("A","u1","a1"), assistantMsg("A","u1","a2"))));
        contextInput.pipeInput("B", context("B", "u2", List.of(assistantMsg("B","u2","b1"))));

        messageInput.pipeInput("A", userMsg("A", "u1", "question-a"));
        messageInput.pipeInput("B", userMsg("B", "u2", "question-b"));

        List<TestRecord<String, FullSessionContext>> records = fullContextOutput.readRecordsToList();
        assertThat(records).hasSize(2);

        FullSessionContext forA = records.stream().filter(r -> "A".equals(r.key()))
                .findFirst().orElseThrow().value();
        FullSessionContext forB = records.stream().filter(r -> "B".equals(r.key()))
                .findFirst().orElseThrow().value();

        assertThat(forA.history()).hasSize(2);
        assertThat(forB.history()).hasSize(1);
    }

    // ── enrich() unit tests (pure function, no Kafka involved) ───────────────

    @Test
    @DisplayName("enrich(): null context produces empty history")
    void enrich_nullContext_emptyHistory() {
        MessageInput msg = userMsg("s", "u", "hello");
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithContext(msg, null);
        assertThat(result.history()).isEmpty();
        assertThat(result.latestInput()).isEqualTo(msg);
        assertThat(result.sessionId()).isEqualTo("s");
    }

    @Test
    @DisplayName("enrich(): context with null history is treated as empty")
    void enrich_contextWithNullHistory() {
        MessageInput msg = userMsg("s", "u", "hello");
        SessionContext ctx = new SessionContext("s", "u", 0.0, 0, List.of(), null, TS);
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithContext(msg, ctx);
        assertThat(result.history()).isEmpty();
    }

    @Test
    @DisplayName("enrich(): history from context is forwarded unchanged")
    void enrich_historyForwarded() {
        MessageInput msg = userMsg("s", "u", "new");
        MessageInput prior = assistantMsg("s", "u", "old");
        SessionContext ctx = context("s", "u", List.of(prior));
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithContext(msg, ctx);
        assertThat(result.history()).containsExactly(prior);
        assertThat(result.latestInput()).isEqualTo(msg);
    }

    @Test
    @DisplayName("enrich(): userId prefers message over context")
    void enrich_userIdFromMessage() {
        MessageInput msg = userMsg("s", "msg-user", "hi");
        SessionContext ctx = context("s", "ctx-user", List.of());
        assertThat(EnrichInputMessageProcessor.enrichWithContext(msg, ctx).userId()).isEqualTo("msg-user");
    }

    @Test
    @DisplayName("enrich(): userId falls back to context when message userId is blank")
    void enrich_userIdFallback() {
        MessageInput msg = new MessageInput("s", "  ", "user", "hi", TS, Map.of());
        SessionContext ctx = context("s", "ctx-user", List.of());
        assertThat(EnrichInputMessageProcessor.enrichWithContext(msg, ctx).userId()).isEqualTo("ctx-user");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static MessageInput userMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "user", content, TS, Map.of());
    }

    private static MessageInput assistantMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "assistant", content, TS, Map.of());
    }

    private static SessionContext context(String sessionId, String userId,
                                          List<MessageInput> history) {
        return new SessionContext(sessionId, userId, 0.0, history.size(),
                history.isEmpty() ? List.of() : List.of(history.get(history.size() - 1)),
                history, TS);
    }
}