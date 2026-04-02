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
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.flightdeck.streams.processors.EndTurnProcessor.*;
import static org.assertj.core.api.Assertions.*;

class EndTurnProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ThinkResponse>  thinkInput;
    private TestOutputTopic<String, UserResponse>  messageOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));
        EndTurnProcessor.register(builder, thinkStream);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-end-turn");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        thinkInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        messageOutput = driver.createOutputTopic(
                Topics.MESSAGE_OUTPUT,
                Serdes.String().deserializer(),
                JsonSerde.of(UserResponse.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Filter: qualifying responses reach message-output ────────────────────

    @Test
    @DisplayName("endTurn=true with no tool calls is forwarded to message-output")
    void endTurn_noTools_isForwarded() {
        thinkInput.pipeInput("sess-1", endTurnResponse("sess-1", "user-A",
                List.of(assistantMsg("sess-1", "user-A", "Here is your answer.")),
                List.of()));

        assertThat(messageOutput.isEmpty()).isFalse();
        UserResponse response = messageOutput.readRecord().value();
        assertThat(response.sessionId()).isEqualTo("sess-1");
        assertThat(response.content()).isEqualTo("Here is your answer.");
    }

    @Test
    @DisplayName("endTurn=false is never forwarded to message-output")
    void notEndTurn_isDropped() {
        thinkInput.pipeInput("sess-2", midTurnResponse("sess-2", "user-B",
                List.of(assistantMsg("sess-2", "user-B", "Thinking...")),
                List.of()));

        assertThat(messageOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("endTurn=true WITH pending tool calls is NOT forwarded")
    void endTurn_withToolCalls_isDropped() {
        ToolUseItem pendingTool = new ToolUseItem("tuid-1", "tool_1",
                "get_invoice_balance", Map.of(), "sess-3", 1, TS);

        thinkInput.pipeInput("sess-3", endTurnResponse("sess-3", "user-C",
                List.of(assistantMsg("sess-3", "user-C", "Let me check...")),
                List.of(pendingTool)));

        assertThat(messageOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("endTurn=true with null tool_uses list is forwarded")
    void endTurn_nullToolUses_isForwarded() {
        ThinkResponse response = new ThinkResponse("sess-4", "user-D", 0.01, null,
                100, 50,
                List.of(assistantMsg("sess-4", "user-D", "Done.")),
                null, true, TS, null);

        thinkInput.pipeInput("sess-4", response);

        assertThat(messageOutput.isEmpty()).isFalse();
        assertThat(messageOutput.readRecord().value().content()).isEqualTo("Done.");
    }

    // ── Transform: UserResponse fields are correctly populated ───────────────

    @Test
    @DisplayName("UserResponse carries correct sessionId, userId, tokens and cost")
    void userResponse_fieldMapping() {
        thinkInput.pipeInput("sess-5", endTurnResponse("sess-5", "user-E",
                List.of(assistantMsg("sess-5", "user-E", "Final answer.")),
                List.of(), 0.0042, 200, 75));

        UserResponse r = messageOutput.readRecord().value();
        assertThat(r.sessionId()).isEqualTo("sess-5");
        assertThat(r.userId()).isEqualTo("user-E");
        assertThat(r.inputTokens()).isEqualTo(200);
        assertThat(r.outputTokens()).isEqualTo(75);
        assertThat(r.cost()).isCloseTo(0.0042, within(0.000001));
        assertThat(r.llmCalls()).isEqualTo(1);
    }

    @Test
    @DisplayName("Output record key is the session_id")
    void outputKey_isSessionId() {
        thinkInput.pipeInput("key-test", endTurnResponse("key-test", "u",
                List.of(assistantMsg("key-test", "u", "hi")), List.of()));

        assertThat(messageOutput.readRecord().key()).isEqualTo("key-test");
    }

    @Test
    @DisplayName("sourceAgent defaults to 'agent-1' when no agent metadata is present")
    void sourceAgent_defaultsToAgent1() {
        thinkInput.pipeInput("sess-6", endTurnResponse("sess-6", "u",
                List.of(assistantMsg("sess-6", "u", "hi")), List.of()));

        assertThat(messageOutput.readRecord().value().sourceAgent()).isEqualTo(DEFAULT_AGENT);
    }

    @Test
    @DisplayName("sourceAgent is read from message metadata when present")
    void sourceAgent_fromMetadata() {
        MessageInput msgWithAgent = new MessageInput("sess-7", "u", ROLE_ASSISTANT,
                "Hello", TS, Map.of("agent", "agent-2"));
        thinkInput.pipeInput("sess-7", endTurnResponse("sess-7", "u",
                List.of(msgWithAgent), List.of()));

        assertThat(messageOutput.readRecord().value().sourceAgent()).isEqualTo("agent-2");
    }

    // ── Content assembly ──────────────────────────────────────────────────────

    @Test
    @DisplayName("assembleContent: single assistant message returns its content")
    void assembleContent_single() {
        List<MessageInput> messages = List.of(assistantMsg("s", "u", "Hello world"));
        assertThat(assembleContent(messages)).isEqualTo("Hello world");
    }

    @Test
    @DisplayName("assembleContent: multiple assistant messages are joined with separator")
    void assembleContent_multiple() {
        List<MessageInput> messages = List.of(
                assistantMsg("s", "u", "Part one."),
                assistantMsg("s", "u", "Part two.")
        );
        assertThat(assembleContent(messages))
                .isEqualTo("Part one." + CONTENT_SEPARATOR + "Part two.");
    }

    @Test
    @DisplayName("assembleContent: non-assistant messages are excluded")
    void assembleContent_excludesNonAssistant() {
        List<MessageInput> messages = List.of(
                new MessageInput("s", "u", "user",   "User question",  TS, Map.of()),
                new MessageInput("s", "u", "tool",   "Tool result",    TS, Map.of()),
                assistantMsg("s", "u", "Assistant reply.")
        );
        assertThat(assembleContent(messages)).isEqualTo("Assistant reply.");
    }

    @Test
    @DisplayName("assembleContent: null message list returns empty string")
    void assembleContent_null() {
        assertThat(assembleContent(null)).isEmpty();
    }

    @Test
    @DisplayName("assembleContent: empty message list returns empty string")
    void assembleContent_empty() {
        assertThat(assembleContent(List.of())).isEmpty();
    }

    @Test
    @DisplayName("assembleContent: list with only non-assistant messages returns empty string")
    void assembleContent_noAssistantMessages() {
        List<MessageInput> messages = List.of(
                new MessageInput("s", "u", "user", "Question", TS, Map.of())
        );
        assertThat(assembleContent(messages)).isEmpty();
    }

    @Test
    @DisplayName("assembleContent: blank assistant content is excluded")
    void assembleContent_blankContentExcluded() {
        List<MessageInput> messages = List.of(
                assistantMsg("s", "u", "   "),          // blank — excluded
                assistantMsg("s", "u", "Real content")  // kept
        );
        assertThat(assembleContent(messages)).isEqualTo("Real content");
    }

    // ── toUserResponse() unit tests ───────────────────────────────────────────

    @Test
    @DisplayName("toUserResponse: content is assembled from assistant messages only")
    void toUserResponse_contentFromAssistant() {
        ThinkResponse resp = endTurnResponse("s", "u", List.of(
                new MessageInput("s", "u", "user",      "Question",   TS, Map.of()),
                assistantMsg("s", "u", "The answer.")
        ), List.of());

        UserResponse result = toUserResponse("s", resp);
        assertThat(result.content()).isEqualTo("The answer.");
    }

    @Test
    @DisplayName("toUserResponse: empty content when ThinkResponse has no messages")
    void toUserResponse_noMessages_emptyContent() {
        ThinkResponse resp = new ThinkResponse("s", "u", 0.0, null, 0, 0,
                List.of(), List.of(), true, TS, null);

        UserResponse result = toUserResponse("s", resp);
        assertThat(result.content()).isEmpty();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ThinkResponse endTurnResponse(String sessionId, String userId,
                                                  List<MessageInput> messages,
                                                  List<ToolUseItem> tools) {
        return endTurnResponse(sessionId, userId, messages, tools, 0.005, 150, 60);
    }

    private static ThinkResponse endTurnResponse(String sessionId, String userId,
                                                  List<MessageInput> messages,
                                                  List<ToolUseItem> tools,
                                                  double cost, int inputTokens, int outputTokens) {
        return new ThinkResponse(sessionId, userId, cost, null, inputTokens, outputTokens,
                messages, tools, true, TS, null);
    }

    private static ThinkResponse midTurnResponse(String sessionId, String userId,
                                                   List<MessageInput> messages,
                                                   List<ToolUseItem> tools) {
        return new ThinkResponse(sessionId, userId, 0.005, null, 150, 60,
                messages, tools, false, TS, null);
    }

    private static MessageInput assistantMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, ROLE_ASSISTANT, content, TS, Map.of());
    }
}