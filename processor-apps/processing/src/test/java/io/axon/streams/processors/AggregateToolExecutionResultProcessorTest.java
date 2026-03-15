package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.*;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link AggregateToolExecutionResultProcessor}.
 *
 * Core invariant under test:
 *   tool-use-all-complete is emitted ONCE when results.size() == expectedCount
 */
class AggregateToolExecutionResultProcessorTest {

    private TopologyTestDriver driver;

    /** Seeds expected counts via think-request-response */
    private TestInputTopic<String, ThinkResponse>       thinkInput;

    /** Delivers arriving tool results */
    private TestInputTopic<String, ToolUseResult>       resultInput;

    /** Captures the complete-event output */
    private TestOutputTopic<String, ToolResultAccumulator> allCompleteOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregateToolExecutionResultProcessor.register(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-aggregate-tool-results");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        thinkInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        resultInput = driver.createInputTopic(
                Topics.TOOL_USE_RESULT,
                Serdes.String().serializer(),
                JsonSerde.of(ToolUseResult.class).serializer());

        allCompleteOutput = driver.createOutputTopic(
                Topics.TOOL_USE_ALL_COMPLETE,
                Serdes.String().deserializer(),
                JsonSerde.of(ToolResultAccumulator.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Core emission behaviour ───────────────────────────────────────────────

    @Test
    @DisplayName("Single expected tool: emits when one result arrives")
    void singleTool_emitsOnFirstResult() {
        thinkInput.pipeInput("sess-1", thinkResp("sess-1", "u",
                List.of(toolUseItem("tuid-1", "get_balance", "sess-1"))));

        resultInput.pipeInput("sess-1", toolResult("sess-1", "tuid-1", "get_balance"));

        assertThat(allCompleteOutput.isEmpty()).isFalse();

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.sessionId()).isEqualTo("sess-1");
        assertThat(acc.results()).hasSize(1);
        assertThat(acc.expectedCount()).isEqualTo(1);
        assertThat(acc.emitted()).isTrue();
    }

    @Test
    @DisplayName("Three expected tools: emits only after all three results arrive")
    void threeTools_emitsAfterAllThree() {
        thinkInput.pipeInput("sess-2", thinkResp("sess-2", "u", List.of(
                toolUseItem("t1", "tool_a", "sess-2"),
                toolUseItem("t2", "tool_b", "sess-2"),
                toolUseItem("t3", "tool_c", "sess-2")
        )));

        // First two results — should NOT emit yet
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t1", "tool_a"));
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t2", "tool_b"));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        // Third result — NOW it emits
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t3", "tool_c"));
        assertThat(allCompleteOutput.isEmpty()).isFalse();

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).hasSize(3);
        assertThat(acc.expectedCount()).isEqualTo(3);
        assertThat(acc.results()).extracting(ToolUseResult::toolUseId)
                .containsExactlyInAnyOrder("t1", "t2", "t3");
    }

    @Test
    @DisplayName("All results in the emitted accumulator carry the correct tool names")
    void emittedAccumulator_containsAllToolNames() {
        thinkInput.pipeInput("sess-3", thinkResp("sess-3", "u", List.of(
                toolUseItem("ta", "get_invoice", "sess-3"),
                toolUseItem("tb", "send_email",  "sess-3")
        )));

        resultInput.pipeInput("sess-3", toolResult("sess-3", "ta", "get_invoice"));
        resultInput.pipeInput("sess-3", toolResult("sess-3", "tb", "send_email"));

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).extracting(ToolUseResult::name)
                .containsExactlyInAnyOrder("get_invoice", "send_email");
    }

    // ── One-shot emission guard ───────────────────────────────────────────────

    @Test
    @DisplayName("Late duplicate result after completion does NOT trigger a second emission")
    void lateDuplicate_doesNotReEmit() {
        thinkInput.pipeInput("sess-4", thinkResp("sess-4", "u",
                List.of(toolUseItem("t1", "tool_x", "sess-4"))));

        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x"));
        allCompleteOutput.readRecord(); // consume the legitimate emission

        // Late duplicate
        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x"));

        assertThat(allCompleteOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Exactly one record emitted even when result count overshoots expected")
    void overshooting_emitsOnce() {
        thinkInput.pipeInput("sess-5", thinkResp("sess-5", "u",
                List.of(toolUseItem("t1", "tool_a", "sess-5"))));

        resultInput.pipeInput("sess-5", toolResult("sess-5", "t1", "tool_a"));
        resultInput.pipeInput("sess-5", toolResult("sess-5", "t1", "tool_a")); // extra

        assertThat(allCompleteOutput.readRecordsToList()).hasSize(1);
    }

    // ── Store reset after emission ─────────────────────────────────────────────

    @Test
    @DisplayName("After emission, stores are reset — a new think cycle starts fresh")
    void storeReset_newCycleBeginsClean() {
        // First think cycle: 1 tool
        thinkInput.pipeInput("sess-6", thinkResp("sess-6", "u",
                List.of(toolUseItem("t1", "tool_a", "sess-6"))));
        resultInput.pipeInput("sess-6", toolResult("sess-6", "t1", "tool_a"));
        allCompleteOutput.readRecord(); // drain first emission

        // Second think cycle on the SAME session: 2 tools
        thinkInput.pipeInput("sess-6", thinkResp("sess-6", "u", List.of(
                toolUseItem("t2", "tool_b", "sess-6"),
                toolUseItem("t3", "tool_c", "sess-6")
        )));

        resultInput.pipeInput("sess-6", toolResult("sess-6", "t2", "tool_b"));
        assertThat(allCompleteOutput.isEmpty()).isTrue();  // not yet

        resultInput.pipeInput("sess-6", toolResult("sess-6", "t3", "tool_c"));
        assertThat(allCompleteOutput.isEmpty()).isFalse(); // now complete

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).hasSize(2);
        assertThat(acc.expectedCount()).isEqualTo(2);
    }

    // ── Session isolation ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Two sessions accumulate independently with no cross-contamination")
    void sessionIsolation() {
        thinkInput.pipeInput("A", thinkResp("A", "u1",
                List.of(toolUseItem("a1", "tool_a", "A"))));
        thinkInput.pipeInput("B", thinkResp("B", "u2", List.of(
                toolUseItem("b1", "tool_b", "B"),
                toolUseItem("b2", "tool_c", "B")
        )));

        // Session A completes
        resultInput.pipeInput("A", toolResult("A", "a1", "tool_a"));
        // Session B gets only its first result
        resultInput.pipeInput("B", toolResult("B", "b1", "tool_b"));

        // Only A should have emitted
        List<TestRecord<String, ToolResultAccumulator>> records =
                allCompleteOutput.readRecordsToList();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("A");

        // Now complete B
        resultInput.pipeInput("B", toolResult("B", "b2", "tool_c"));
        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("B");
    }

    // ── Output key ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Emitted record is keyed by session_id")
    void outputKey_isSessionId() {
        thinkInput.pipeInput("key-sess", thinkResp("key-sess", "u",
                List.of(toolUseItem("t1", "tool_x", "key-sess"))));
        resultInput.pipeInput("key-sess", toolResult("key-sess", "t1", "tool_x"));

        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("key-sess");
    }

    // ── ToolResultAccumulator model unit tests ────────────────────────────────

    @Test
    @DisplayName("isComplete: true when results.size() == expectedCount and not yet emitted")
    void isComplete_true() {
        ToolUseResult r = toolResult("s", "t1", "tool");
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), false, TS);
        assertThat(acc.isComplete()).isTrue();
    }

    @Test
    @DisplayName("isComplete: false when results.size() < expectedCount")
    void isComplete_false_notEnoughResults() {
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 2, List.of(toolResult("s","t1","x")), false, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    @Test
    @DisplayName("isComplete: false when already emitted")
    void isComplete_false_alreadyEmitted() {
        ToolUseResult r = toolResult("s", "t1", "tool");
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), true, TS);  // emitted=true
        assertThat(acc.isComplete()).isFalse();
    }

    @Test
    @DisplayName("isComplete: false when expectedCount is 0 (not yet registered)")
    void isComplete_false_zeroExpected() {
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 0, List.of(toolResult("s","t1","x")), false, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ThinkResponse thinkResp(String sessionId, String userId,
                                            List<ToolUseItem> tools) {
        return new ThinkResponse(sessionId, userId, 0.005, 150, 60,
                List.of(), tools, false, TS);
    }

    private static ToolUseItem toolUseItem(String toolUseId, String name, String sessionId) {
        return new ToolUseItem(toolUseId, "tool_" + toolUseId, name,
                Map.of(), sessionId, 1, TS);
    }

    private static ToolUseResult toolResult(String sessionId, String toolUseId, String name) {
        return new ToolUseResult(sessionId, toolUseId, name,
                Map.of("status", "ok"), 320L, "success", 1, TS);
    }
}