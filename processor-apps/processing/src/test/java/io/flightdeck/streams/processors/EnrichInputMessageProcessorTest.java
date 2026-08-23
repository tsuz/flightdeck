package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.FullSessionContext;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.model.UserSettings;
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
 *   think-request-response (KTable)
 *       ▼
 *   enriched-message-input (KStream)
 */
class EnrichInputMessageProcessorTest {

    private TopologyTestDriver driver;

    /** Drives raw user messages onto message-input */
    private TestInputTopic<String, MessageInput>   messageInput;

    /** Seeds the think-request-response KTable directly */
    private TestInputTopic<String, ThinkResponse>  thinkInput;

    /** Captures the enriched FullSessionContext records */
    private TestOutputTopic<String, FullSessionContext> fullContextOutput;

    /** Seeds the memoir-context KTable */
    private TestInputTopic<String, String> memoirInput;

    /** Seeds the user-settings KTable */
    private TestInputTopic<String, UserSettings> userSettingsInput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create shared KTables (same as FlightDeckStreamsApp does)
        KTable<String, String> memoirTable = builder.table(
                Topics.MEMOIR_CONTEXT,
                Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, ThinkResponse> thinkTable = builder.table(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));
        KTable<String, UserSettings> userSettingsTable = builder.table(
                Topics.USER_SETTINGS,
                Consumed.with(Serdes.String(), JsonSerde.of(UserSettings.class)));
        EnrichInputMessageProcessor.register(builder, memoirTable, thinkTable, userSettingsTable);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-enrich");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        messageInput = driver.createInputTopic(
                Topics.MESSAGE_INPUT,
                Serdes.String().serializer(),
                JsonSerde.of(MessageInput.class).serializer());

        // Seed the KTable by writing directly to the think-request-response topic
        thinkInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        memoirInput = driver.createInputTopic(
                Topics.MEMOIR_CONTEXT,
                Serdes.String().serializer(),
                Serdes.String().serializer());

        userSettingsInput = driver.createInputTopic(
                Topics.USER_SETTINGS,
                Serdes.String().serializer(),
                JsonSerde.of(UserSettings.class).serializer());

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
    @DisplayName("Message with existing ThinkResponse carries the reconstructed history")
    void messageWithThinkResponse_includesHistory() {
        // Seed the KTable with a previous ThinkResponse
        ThinkResponse prevResponse = new ThinkResponse("sess-h", "user-A", 0.01, null, 0.01, 100, 50,
                List.of(assistantMsg("sess-h", "user-A", "First reply.")),
                userMsg("sess-h", "user-A", "Second question"),
                List.of(assistantMsg("sess-h", "user-A", "Second reply.")),
                List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("sess-h", prevResponse);

        // Now a new user message arrives
        messageInput.pipeInput("sess-h", userMsg("sess-h", "user-A", "Follow-up?"));

        FullSessionContext full = fullContextOutput.readRecord().value();

        // History = previousMessages + lastInputMessage + lastInputResponse
        assertThat(full.history()).hasSize(3);
        assertThat(full.history()).extracting(MessageInput::content)
                .containsExactly("First reply.", "Second question", "Second reply.");
        assertThat(full.latestInput().content()).isEqualTo("Follow-up?");
    }

    @Test
    @DisplayName("latestInput is always the incoming message, not part of history")
    void latestInput_isNotInHistory() {
        ThinkResponse prevResponse = new ThinkResponse("sess-li", "u", 0.01, null, 0.01, 100, 50,
                null, null,
                List.of(assistantMsg("sess-li", "u", "Prior turn.")),
                List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("sess-li", prevResponse);

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
        ThinkResponse prevResponse = new ThinkResponse("sess-u1", "old-user", 0.01, null, 0.01, 100, 50,
                null, null, null, List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("sess-u1", prevResponse);
        messageInput.pipeInput("sess-u1", userMsg("sess-u1", "new-user", "hi"));

        assertThat(fullContextOutput.readRecord().value().userId()).isEqualTo("new-user");
    }

    @Test
    @DisplayName("userId falls back to ThinkResponse value when message carries none")
    void userId_fallsBackToThinkResponse() {
        ThinkResponse prevResponse = new ThinkResponse("sess-u2", "ctx-user", 0.01, null, 0.01, 100, 50,
                null, null, null, List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("sess-u2", prevResponse);
        // Message has null userId (e.g. scheduler-triggered input)
        MessageInput schedulerMsg = new MessageInput("sess-u2", null, "user",
                "Scheduled trigger", TS, Map.of());
        messageInput.pipeInput("sess-u2", schedulerMsg);

        assertThat(fullContextOutput.readRecord().value().userId()).isEqualTo("ctx-user");
    }

    @Test
    @DisplayName("userId is null when neither message nor ThinkResponse provides one")
    void userId_nullWhenNoSource() {
        MessageInput msg = new MessageInput("sess-u3", null, "user", "hi", TS, Map.of());
        messageInput.pipeInput("sess-u3", msg);

        assertThat(fullContextOutput.readRecord().value().userId()).isNull();
    }

    // ── Session isolation ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Two sessions receive their own independent histories")
    void sessionIsolation() {
        ThinkResponse respA = new ThinkResponse("A", "u1", 0.01, null, 0.01, 100, 50,
                List.of(assistantMsg("A", "u1", "a1")),
                null,
                List.of(assistantMsg("A", "u1", "a2")),
                List.of(), true, false, 0, 0, 0.0, TS);
        ThinkResponse respB = new ThinkResponse("B", "u2", 0.01, null, 0.01, 100, 50,
                null, null,
                List.of(assistantMsg("B", "u2", "b1")),
                List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("A", respA);
        thinkInput.pipeInput("B", respB);

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

    // ── enrichWithThinkResponse() unit tests (pure function, no Kafka involved) ─

    @Test
    @DisplayName("enrichWithThinkResponse(): null ThinkResponse produces empty history")
    void enrich_nullThinkResponse_emptyHistory() {
        MessageInput msg = userMsg("s", "u", "hello");
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithThinkResponse(msg, null);
        assertThat(result.history()).isEmpty();
        assertThat(result.latestInput()).isEqualTo(msg);
        assertThat(result.sessionId()).isEqualTo("s");
    }

    @Test
    @DisplayName("enrichWithThinkResponse(): ThinkResponse with null fields is treated as empty history")
    void enrich_thinkResponseWithNullFields() {
        MessageInput msg = userMsg("s", "u", "hello");
        ThinkResponse resp = new ThinkResponse("s", "u", 0.0, null, 0.0, 0, 0,
                null, null, null, List.of(), true, false, 0, 0, 0.0, TS);
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithThinkResponse(msg, resp);
        assertThat(result.history()).isEmpty();
    }

    @Test
    @DisplayName("enrichWithThinkResponse(): history is reconstructed from ThinkResponse fields")
    void enrich_historyReconstructed() {
        MessageInput msg = userMsg("s", "u", "new");
        MessageInput prior = assistantMsg("s", "u", "old");
        MessageInput inputMsg = userMsg("s", "u", "question");
        MessageInput response = assistantMsg("s", "u", "answer");
        ThinkResponse resp = new ThinkResponse("s", "u", 0.01, null, 0.01, 100, 50,
                List.of(prior), inputMsg, List.of(response), List.of(), true, false, 0, 0, 0.0, TS);
        FullSessionContext result = EnrichInputMessageProcessor.enrichWithThinkResponse(msg, resp);
        assertThat(result.history()).containsExactly(prior, inputMsg, response);
        assertThat(result.latestInput()).isEqualTo(msg);
    }

    @Test
    @DisplayName("enrichWithThinkResponse(): userId prefers message over ThinkResponse")
    void enrich_userIdFromMessage() {
        MessageInput msg = userMsg("s", "msg-user", "hi");
        ThinkResponse resp = new ThinkResponse("s", "ctx-user", 0.01, null, 0.01, 100, 50,
                null, null, null, List.of(), true, false, 0, 0, 0.0, TS);
        assertThat(EnrichInputMessageProcessor.enrichWithThinkResponse(msg, resp).userId()).isEqualTo("msg-user");
    }

    @Test
    @DisplayName("enrichWithThinkResponse(): userId falls back to ThinkResponse when message userId is blank")
    void enrich_userIdFallback() {
        MessageInput msg = new MessageInput("s", "  ", "user", "hi", TS, Map.of());
        ThinkResponse resp = new ThinkResponse("s", "ctx-user", 0.01, null, 0.01, 100, 50,
                null, null, null, List.of(), true, false, 0, 0, 0.0, TS);
        assertThat(EnrichInputMessageProcessor.enrichWithThinkResponse(msg, resp).userId()).isEqualTo("ctx-user");
    }

    // ── User settings join ───────────────────────────────────────────────────

    @Test
    @DisplayName("Per-user system prompt from user-settings is joined into the context")
    void userSettings_systemPromptJoined() {
        userSettingsInput.pipeInput("user-S", new UserSettings("You are a pirate.", TS));

        messageInput.pipeInput("sess-s1", userMsg("sess-s1", "user-S", "hi"));

        FullSessionContext full = fullContextOutput.readRecord().value();
        assertThat(full.systemPrompt()).isEqualTo("You are a pirate.");
        assertThat(full.sessionId()).isEqualTo("sess-s1");
    }

    @Test
    @DisplayName("system_prompt is null when the user has no settings record")
    void userSettings_absent_systemPromptNull() {
        messageInput.pipeInput("sess-s2", userMsg("sess-s2", "user-without-settings", "hi"));

        assertThat(fullContextOutput.readRecord().value().systemPrompt()).isNull();
    }

    @Test
    @DisplayName("Tombstone on user-settings reverts the user to no override")
    void userSettings_tombstone_revertsToNull() {
        userSettingsInput.pipeInput("user-T", new UserSettings("Custom prompt.", TS));
        messageInput.pipeInput("sess-t1", userMsg("sess-t1", "user-T", "first"));
        assertThat(fullContextOutput.readRecord().value().systemPrompt()).isEqualTo("Custom prompt.");

        userSettingsInput.pipeInput("user-T", null);
        messageInput.pipeInput("sess-t1", userMsg("sess-t1", "user-T", "second"));
        assertThat(fullContextOutput.readRecord().value().systemPrompt()).isNull();
    }

    @Test
    @DisplayName("Settings are per-user: another user's prompt is not picked up")
    void userSettings_isolatedPerUser() {
        userSettingsInput.pipeInput("user-A", new UserSettings("Prompt for A.", TS));

        messageInput.pipeInput("sess-b", userMsg("sess-b", "user-B", "hi"));

        assertThat(fullContextOutput.readRecord().value().systemPrompt()).isNull();
    }

    @Test
    @DisplayName("Blank system_prompt is normalized to null (think-consumer treats blank as no override)")
    void userSettings_blankPrompt_normalizedToNull() {
        userSettingsInput.pipeInput("user-BL", new UserSettings("   ", TS));

        messageInput.pipeInput("sess-bl", userMsg("sess-bl", "user-BL", "hi"));

        assertThat(fullContextOutput.readRecord().value().systemPrompt()).isNull();
    }

    @Test
    @DisplayName("A message with no user_id cannot pick up settings via a colliding session_id")
    void userSettings_nullUserId_doesNotCollideWithSessionId() {
        userSettingsInput.pipeInput("user-C", new UserSettings("Prompt for C.", TS));

        // sessionId deliberately equals a real user_id: with no user_id on the
        // message the join key falls back to sessionId, so the guard is what
        // keeps user-C's prompt out of this turn.
        messageInput.pipeInput("user-C", userMsg("user-C", null, "hi"));

        FullSessionContext full = fullContextOutput.readRecord().value();
        assertThat(full.userId()).isNull();
        assertThat(full.systemPrompt()).isNull();
    }

    @Test
    @DisplayName("Memoir and system prompt can both be joined for the same user")
    void userSettings_andMemoir_bothJoined() {
        memoirInput.pipeInput("user-M", "Likes brevity.");
        userSettingsInput.pipeInput("user-M", new UserSettings("Custom base.", TS));

        messageInput.pipeInput("sess-m", userMsg("sess-m", "user-M", "hi"));

        FullSessionContext full = fullContextOutput.readRecord().value();
        assertThat(full.memoirContext()).isEqualTo("Likes brevity.");
        assertThat(full.systemPrompt()).isEqualTo("Custom base.");
    }

    // ── Cost from ThinkResponse ─────────────────────────────────────────────

    @Test
    @DisplayName("Cost flows from ThinkResponse.totalSessionCost into FullSessionContext.cost")
    void costFlowsFromThinkResponseTotalSessionCost() {
        // Seed the KTable with a ThinkResponse that has totalSessionCost=0.05
        ThinkResponse prevResponse = new ThinkResponse("sess-cost", "user-C", 0.05, null, 0.01, 100, 50,
                null, null,
                List.of(assistantMsg("sess-cost", "user-C", "Prior reply.")),
                List.of(), true, false, 0, 0, 0.0, TS);
        thinkInput.pipeInput("sess-cost", prevResponse);

        // Send a new message-input
        messageInput.pipeInput("sess-cost", userMsg("sess-cost", "user-C", "Follow-up?"));

        FullSessionContext full = fullContextOutput.readRecord().value();
        assertThat(full.cost()).isEqualTo(0.05);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static MessageInput userMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "user", content, TS, Map.of());
    }

    private static MessageInput assistantMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "assistant", content, TS, Map.of());
    }
}
