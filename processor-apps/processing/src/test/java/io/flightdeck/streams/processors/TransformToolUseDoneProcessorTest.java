package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.ToolResultAccumulator;
import io.flightdeck.streams.model.ToolUseResult;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.flightdeck.streams.processors.TransformToolUseDoneProcessor.*;
import static org.assertj.core.api.Assertions.*;

class TransformToolUseDoneProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ToolResultAccumulator> allCompleteInput;
    private TestOutputTopic<String, MessageInput>          messageOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        TransformToolUseDoneProcessor.register(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-transform-tool-done");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        allCompleteInput = driver.createInputTopic(
                Topics.TOOL_USE_ALL_COMPLETE,
                Serdes.String().serializer(),
                JsonSerde.of(ToolResultAccumulator.class).serializer());

        messageOutput = driver.createOutputTopic(
                Topics.MESSAGE_INPUT,
                Serdes.String().deserializer(),
                JsonSerde.of(MessageInput.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Core routing ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("Completed accumulator produces one MessageInput on message-input")
    void completedAccumulator_producesOneMessage() {
        allCompleteInput.pipeInput("sess-1", accumulator("sess-1", "user-A", List.of(
                result("sess-1", "t1", "get_balance", Map.of("balance", 142.50))
        )));

        assertThat(messageOutput.isEmpty()).isFalse();
        assertThat(messageOutput.readRecordsToList()).hasSize(1);
    }

    @Test
    @DisplayName("Output record is keyed by session_id")
    void outputKey_isSessionId() {
        allCompleteInput.pipeInput("sess-key", accumulator("sess-key", "u", List.of(
                result("sess-key", "t1", "tool_a", Map.of()))));

        assertThat(messageOutput.readRecord().key()).isEqualTo("sess-key");
    }

    // ── MessageInput fields ───────────────────────────────────────────────────

    @Test
    @DisplayName("Produced MessageInput has role='tool'")
    void agentMessage_roleIsTool() {
        allCompleteInput.pipeInput("sess-2", accumulator("sess-2", "u", List.of(
                result("sess-2", "t1", "tool_a", Map.of()))));

        assertThat(messageOutput.readRecord().value().role()).isEqualTo(ROLE_TOOL);
    }

    @Test
    @DisplayName("Produced MessageInput preserves session_id and user_id")
    void agentMessage_sessionAndUserPreserved() {
        allCompleteInput.pipeInput("sess-3", accumulator("sess-3", "user-X", List.of(
                result("sess-3", "t1", "tool_a", Map.of()))));

        MessageInput msg = messageOutput.readRecord().value();
        assertThat(msg.sessionId()).isEqualTo("sess-3");
        assertThat(msg.userId()).isEqualTo("user-X");
    }

    @Test
    @DisplayName("Content is a non-empty list containing tool names")
    void agentMessage_contentContainsToolNames() {
        allCompleteInput.pipeInput("sess-4", accumulator("sess-4", "u", List.of(
                result("sess-4", "t1", "get_invoice_balance", Map.of("due", 50.0)),
                result("sess-4", "t2", "send_reminder",       Map.of("sent", true))
        )));

        Object content = messageOutput.readRecord().value().content();
        assertThat(content).isInstanceOf(List.class);
        String contentStr = content.toString();
        assertThat(contentStr).contains("get_invoice_balance");
        assertThat(contentStr).contains("send_reminder");
    }

    @Test
    @DisplayName("Content includes tool_use_id, name, result, and status fields")
    void agentMessage_contentHasRequiredFields() {
        allCompleteInput.pipeInput("sess-5", accumulator("sess-5", "u", List.of(
                result("sess-5", "tuid-99", "my_tool", Map.of("key", "value"))
        )));

        Object content = messageOutput.readRecord().value().content();
        String contentStr = content.toString();
        assertThat(contentStr).contains("tuid-99");
        assertThat(contentStr).contains("my_tool");
        assertThat(contentStr).contains("success");
        assertThat(contentStr).contains("key");
    }

    // ── Metadata ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Metadata contains tool_count matching the number of results")
    void metadata_toolCount() {
        allCompleteInput.pipeInput("sess-6", accumulator("sess-6", "u", List.of(
                result("sess-6", "t1", "tool_a", Map.of()),
                result("sess-6", "t2", "tool_b", Map.of()),
                result("sess-6", "t3", "tool_c", Map.of())
        )));

        Map<String, Object> meta = messageOutput.readRecord().value().metadata();
        assertThat(meta.get(META_TOOL_COUNT)).isEqualTo(3);
    }

    @Test
    @DisplayName("Metadata source is 'tool-execution'")
    void metadata_source() {
        allCompleteInput.pipeInput("sess-7", accumulator("sess-7", "u", List.of(
                result("sess-7", "t1", "tool_a", Map.of()))));

        Map<String, Object> meta = messageOutput.readRecord().value().metadata();
        assertThat(meta.get(META_SOURCE)).isEqualTo(SOURCE_VALUE);
    }

    @Test
    @DisplayName("Metadata tool_results list contains the original ToolUseResult entries")
    void metadata_toolResultsPresent() {
        allCompleteInput.pipeInput("sess-8", accumulator("sess-8", "u", List.of(
                result("sess-8", "t1", "get_balance", Map.of("balance", 99.0)))));

        Map<String, Object> meta = messageOutput.readRecord().value().metadata();
        assertThat(meta).containsKey(META_TOOL_RESULTS);
        assertThat(meta.get(META_TOOL_RESULTS)).isInstanceOf(List.class);
    }

    // ── Filter: guard against empty/null input ───────────────────────────────

    @Test
    @DisplayName("Accumulator with empty results list is filtered and produces no output")
    void emptyResults_filtered() {
        allCompleteInput.pipeInput("sess-empty",
                new ToolResultAccumulator("sess-empty", "u", 0, List.of(), true, TS));

        assertThat(messageOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Null accumulator value produces no output")
    void nullAccumulator_filtered() {
        allCompleteInput.pipeInput("sess-null", (ToolResultAccumulator) null);
        assertThat(messageOutput.isEmpty()).isTrue();
    }

    // ── Pure function: buildContentList ────────────────────────────────────────

    @Test
    @DisplayName("buildContentList: null list returns empty list")
    void buildContentList_null() {
        assertThat(buildContentList(null)).isEmpty();
    }

    @Test
    @DisplayName("buildContentList: empty list returns empty list")
    void buildContentList_empty() {
        assertThat(buildContentList(List.of())).isEmpty();
    }

    @Test
    @DisplayName("buildContentList: single result produces list with one entry")
    void buildContentList_single() {
        List<Map<String, Object>> content = buildContentList(List.of(
                result("s", "t1", "get_balance", Map.of("balance", 50.0))));

        assertThat(content).hasSize(1);
        assertThat(content.get(0)).containsEntry("name", "get_balance");
        assertThat(content.get(0)).containsEntry("tool_use_id", "t1");
        assertThat(content.get(0)).containsEntry("status", "success");
    }

    @Test
    @DisplayName("buildContentList: multiple results appear as separate entries")
    void buildContentList_multiple() {
        List<Map<String, Object>> content = buildContentList(List.of(
                result("s", "t1", "tool_a", Map.of()),
                result("s", "t2", "tool_b", Map.of())));

        assertThat(content).hasSize(2);
        assertThat(content.get(0)).containsEntry("name", "tool_a");
        assertThat(content.get(1)).containsEntry("name", "tool_b");
    }

    // ── Pure function: buildMetadata ─────────────────────────────────────────

    @Test
    @DisplayName("buildMetadata: all three required keys are present")
    void buildMetadata_requiredKeys() {
        Map<String, Object> meta = buildMetadata(List.of(
                result("s", "t1", "tool_a", Map.of())));

        assertThat(meta).containsKeys(META_TOOL_RESULTS, META_TOOL_COUNT, META_SOURCE);
    }

    @Test
    @DisplayName("buildMetadata: tool_count equals list size")
    void buildMetadata_toolCount() {
        List<ToolUseResult> results = List.of(
                result("s", "t1", "a", Map.of()),
                result("s", "t2", "b", Map.of()));

        assertThat(buildMetadata(results).get(META_TOOL_COUNT)).isEqualTo(2);
    }

    // ── Pure function: toMessageInput ─────────────────────────────────────────

    @Test
    @DisplayName("toMessageInput: role is always 'tool'")
    void toMessageInput_role() {
        ToolResultAccumulator acc = accumulator("s", "u", List.of(
                result("s", "t1", "tool_a", Map.of())));
        assertThat(toMessageInput("s", acc).role()).isEqualTo(ROLE_TOOL);
    }

    @Test
    @DisplayName("toMessageInput: sessionId and userId are carried over")
    void toMessageInput_ids() {
        ToolResultAccumulator acc = accumulator("my-sess", "my-user", List.of(
                result("my-sess", "t1", "tool_a", Map.of())));
        MessageInput msg = toMessageInput("my-sess", acc);
        assertThat(msg.sessionId()).isEqualTo("my-sess");
        assertThat(msg.userId()).isEqualTo("my-user");
    }

    @Test
    @DisplayName("toMessageInput: content is non-empty for non-empty results")
    void toMessageInput_contentNonEmpty() {
        ToolResultAccumulator acc = accumulator("s", "u", List.of(
                result("s", "t1", "tool_x", Map.of("k", "v"))));
        Object content = toMessageInput("s", acc).content();
        assertThat(content).isInstanceOf(List.class);
        assertThat((List<?>) content).isNotEmpty();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ToolResultAccumulator accumulator(String sessionId, String userId,
                                                      List<ToolUseResult> results) {
        return new ToolResultAccumulator(sessionId, userId, results.size(),
                results, true, TS);
    }

    private static ToolUseResult result(String sessionId, String toolUseId,
                                         String name, Map<String, Object> resultData) {
        return new ToolUseResult(sessionId, toolUseId, name, resultData, 320L, "success", 1, TS);
    }
}
