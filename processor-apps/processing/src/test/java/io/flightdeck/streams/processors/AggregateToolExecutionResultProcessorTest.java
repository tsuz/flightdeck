package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.*;
import io.flightdeck.streams.serdes.JsonSerde;
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
        resultInput.pipeInput("sess-1", toolResult("sess-1", "tuid-1", "get_balance", 1));

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
        // First two results — should NOT emit yet
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t1", "tool_a", 3));
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t2", "tool_b", 3));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        // Third result — NOW it emits
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t3", "tool_c", 3));
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
        resultInput.pipeInput("sess-3", toolResult("sess-3", "ta", "get_invoice", 2));
        resultInput.pipeInput("sess-3", toolResult("sess-3", "tb", "send_email", 2));

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).extracting(ToolUseResult::name)
                .containsExactlyInAnyOrder("get_invoice", "send_email");
    }

    // ── One-shot emission guard ───────────────────────────────────────────────

    @Test
    @DisplayName("Late duplicate result after completion starts a new cycle (store was reset)")
    void lateDuplicate_startsNewCycle() {
        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x", 1));
        allCompleteOutput.readRecord(); // consume the legitimate emission

        // After store reset, a duplicate is treated as a brand new cycle
        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x", 1));

        // New cycle completes immediately since totalTools=1
        assertThat(allCompleteOutput.isEmpty()).isFalse();
    }

    @Test
    @DisplayName("Two identical results each trigger separate cycles after store reset")
    void duplicateResults_eachTriggerCycle() {
        resultInput.pipeInput("sess-5", toolResult("sess-5", "t1", "tool_a", 1));
        resultInput.pipeInput("sess-5", toolResult("sess-5", "t1", "tool_a", 1));

        // Each result starts a fresh cycle (store is deleted after emission)
        assertThat(allCompleteOutput.readRecordsToList()).hasSize(2);
    }

    // ── Store reset after emission ─────────────────────────────────────────────

    @Test
    @DisplayName("After emission, stores are reset — a new think cycle starts fresh")
    void storeReset_newCycleBeginsClean() {
        // First think cycle: 1 tool
        resultInput.pipeInput("sess-6", toolResult("sess-6", "t1", "tool_a", 1));
        allCompleteOutput.readRecord(); // drain first emission

        // Second think cycle on the SAME session: 2 tools
        resultInput.pipeInput("sess-6", toolResult("sess-6", "t2", "tool_b", 2));
        assertThat(allCompleteOutput.isEmpty()).isTrue();  // not yet

        resultInput.pipeInput("sess-6", toolResult("sess-6", "t3", "tool_c", 2));
        assertThat(allCompleteOutput.isEmpty()).isFalse(); // now complete

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).hasSize(2);
        assertThat(acc.expectedCount()).isEqualTo(2);
    }

    // ── Session isolation ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Two sessions accumulate independently with no cross-contamination")
    void sessionIsolation() {
        // Session A completes
        resultInput.pipeInput("A", toolResult("A", "a1", "tool_a", 1));
        // Session B gets only its first result
        resultInput.pipeInput("B", toolResult("B", "b1", "tool_b", 2));

        // Only A should have emitted
        List<TestRecord<String, ToolResultAccumulator>> records =
                allCompleteOutput.readRecordsToList();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("A");

        // Now complete B
        resultInput.pipeInput("B", toolResult("B", "b2", "tool_c", 2));
        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("B");
    }

    // ── Output key ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Emitted record is keyed by session_id")
    void outputKey_isSessionId() {
        resultInput.pipeInput("key-sess", toolResult("key-sess", "t1", "tool_x", 1));

        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("key-sess");
    }

    // ── ToolResultAccumulator model unit tests ────────────────────────────────

    @Test
    @DisplayName("isComplete: true when results.size() == expectedCount and not yet emitted")
    void isComplete_true() {
        ToolUseResult r = toolResult("s", "t1", "tool", 1);
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), false, TS);
        assertThat(acc.isComplete()).isTrue();
    }

    @Test
    @DisplayName("isComplete: false when results.size() < expectedCount")
    void isComplete_false_notEnoughResults() {
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 2, List.of(toolResult("s","t1","x", 2)), false, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    @Test
    @DisplayName("isComplete: false when already emitted")
    void isComplete_false_alreadyEmitted() {
        ToolUseResult r = toolResult("s", "t1", "tool", 1);
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), true, TS);  // emitted=true
        assertThat(acc.isComplete()).isFalse();
    }

    @Test
    @DisplayName("isComplete: false when expectedCount is 0 (not yet registered)")
    void isComplete_false_zeroExpected() {
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 0, List.of(toolResult("s","t1","x", 0)), false, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ToolUseResult toolResult(String sessionId, String toolUseId,
                                            String name, int totalTools) {
        return new ToolUseResult(sessionId, toolUseId, name,
                Map.of("status", "ok"), 320L, "success", totalTools, TS);
    }
}
