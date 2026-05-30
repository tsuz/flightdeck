package io.flightdeck.streams.processors;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.*;
import io.flightdeck.streams.model.ToolResultAccumulator.ExpectedTool;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link AggregateToolExecutionResultProcessor}.
 *
 * Invariants under test:
 *   - Count-based completion still works when no seed is present (sync-only turns).
 *   - Seeding from think-request-response drives expected-set completion.
 *   - Async tools that never call back are timed out into synthesised error results.
 *   - A completed turn leaves a tombstone: late/duplicate results are ignored,
 *     but a new turn (new tool_use_ids) on the same session proceeds.
 */
class AggregateToolExecutionResultProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ToolUseResult>          resultInput;
    private TestInputTopic<String, ThinkResponse>          seedInput;
    private TestOutputTopic<String, ToolResultAccumulator> allCompleteOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));

        AggregateToolExecutionResultProcessor.register(builder, thinkStream);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-aggregate-tool-results");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        // Anchor the mock wall clock to now so deadlines (computed from
        // System.currentTimeMillis()) line up with advanceWallClockTime().
        driver = new TopologyTestDriver(builder.build(), props, Instant.now());

        resultInput = driver.createInputTopic(
                Topics.TOOL_USE_RESULT,
                Serdes.String().serializer(),
                JsonSerde.of(ToolUseResult.class).serializer());

        seedInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        allCompleteOutput = driver.createOutputTopic(
                Topics.TOOL_USE_ALL_COMPLETE,
                Serdes.String().deserializer(),
                JsonSerde.of(ToolResultAccumulator.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Count-based completion (sync-only turns, no seed) ─────────────────────

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
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t1", "tool_a", 3));
        resultInput.pipeInput("sess-2", toolResult("sess-2", "t2", "tool_b", 3));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        resultInput.pipeInput("sess-2", toolResult("sess-2", "t3", "tool_c", 3));
        assertThat(allCompleteOutput.isEmpty()).isFalse();

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).hasSize(3);
        assertThat(acc.results()).extracting(ToolUseResult::toolUseId)
                .containsExactlyInAnyOrder("t1", "t2", "t3");
    }

    @Test
    @DisplayName("Emitted accumulator carries the correct tool names")
    void emittedAccumulator_containsAllToolNames() {
        resultInput.pipeInput("sess-3", toolResult("sess-3", "ta", "get_invoice", 2));
        resultInput.pipeInput("sess-3", toolResult("sess-3", "tb", "send_email", 2));

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).extracting(ToolUseResult::name)
                .containsExactlyInAnyOrder("get_invoice", "send_email");
    }

    // ── Seeded (expected-set) completion ──────────────────────────────────────

    @Test
    @DisplayName("Seed + all results: emits with the seeded expected set")
    void seeded_emitsWhenAllExpectedArrive() {
        seedInput.pipeInput("sess-s", thinkSeed("sess-s", "u",
                toolItem("sess-s", "x1", "tool_a"), toolItem("sess-s", "x2", "tool_b")));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        resultInput.pipeInput("sess-s", toolResult("sess-s", "x1", "tool_a", 2));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        resultInput.pipeInput("sess-s", toolResult("sess-s", "x2", "tool_b", 2));
        assertThat(allCompleteOutput.isEmpty()).isFalse();

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).extracting(ToolUseResult::toolUseId)
                .containsExactlyInAnyOrder("x1", "x2");
        assertThat(acc.expected()).extracting(ExpectedTool::toolUseId)
                .containsExactlyInAnyOrder("x1", "x2");
    }

    // ── Async timeout ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("Async tool that never calls back is timed out into a synthesised error result")
    void asyncTimeout_synthesisesErrorForMissing() {
        // Two tools requested; only one (sync) returns. The other is async and never calls back.
        seedInput.pipeInput("sess-t", thinkSeed("sess-t", "u",
                toolItem("sess-t", "sync1", "lookup"),
                toolItem("sess-t", "async1", "generate_report")));
        resultInput.pipeInput("sess-t", toolResult("sess-t", "sync1", "lookup", 2));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        // Advance past the async timeout (default 5 min).
        driver.advanceWallClockTime(Duration.ofMinutes(6));

        assertThat(allCompleteOutput.isEmpty()).isFalse();
        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).hasSize(2);

        ToolUseResult synthesised = acc.results().stream()
                .filter(r -> r.toolUseId().equals("async1")).findFirst().orElseThrow();
        assertThat(synthesised.status()).isEqualTo("error");
        assertThat(synthesised.result()).containsEntry("reason", "timeout");
    }

    // ── Tombstone: late/duplicate vs next turn ────────────────────────────────

    @Test
    @DisplayName("Late duplicate result for a completed turn is ignored (no second emission)")
    void lateDuplicate_isIgnored() {
        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x", 1));
        allCompleteOutput.readRecord(); // legitimate emission

        // Same tool_use_id again — a duplicate callback for the completed turn.
        resultInput.pipeInput("sess-4", toolResult("sess-4", "t1", "tool_x", 1));
        assertThat(allCompleteOutput.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("A new turn (new tool_use_ids) on a completed session proceeds normally")
    void nextTurn_afterCompletion_proceeds() {
        // Turn 1: one tool.
        resultInput.pipeInput("sess-6", toolResult("sess-6", "t1", "tool_a", 1));
        allCompleteOutput.readRecord();

        // Turn 2 on the SAME session: two new tools.
        resultInput.pipeInput("sess-6", toolResult("sess-6", "t2", "tool_b", 2));
        assertThat(allCompleteOutput.isEmpty()).isTrue();

        resultInput.pipeInput("sess-6", toolResult("sess-6", "t3", "tool_c", 2));
        assertThat(allCompleteOutput.isEmpty()).isFalse();

        ToolResultAccumulator acc = allCompleteOutput.readRecord().value();
        assertThat(acc.results()).extracting(ToolUseResult::toolUseId)
                .containsExactlyInAnyOrder("t2", "t3");
    }

    // ── Session isolation ─────────────────────────────────────────────────────

    @Test
    @DisplayName("Two sessions accumulate independently")
    void sessionIsolation() {
        resultInput.pipeInput("A", toolResult("A", "a1", "tool_a", 1));
        resultInput.pipeInput("B", toolResult("B", "b1", "tool_b", 2));

        List<TestRecord<String, ToolResultAccumulator>> records = allCompleteOutput.readRecordsToList();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).key()).isEqualTo("A");

        resultInput.pipeInput("B", toolResult("B", "b2", "tool_c", 2));
        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("B");
    }

    @Test
    @DisplayName("Emitted record is keyed by session_id")
    void outputKey_isSessionId() {
        resultInput.pipeInput("key-sess", toolResult("key-sess", "t1", "tool_x", 1));
        assertThat(allCompleteOutput.readRecord().key()).isEqualTo("key-sess");
    }

    // ── ToolResultAccumulator model unit tests ────────────────────────────────

    @Test
    @DisplayName("isComplete (count fallback): true when results.size() == expectedCount")
    void isComplete_countFallback_true() {
        ToolUseResult r = toolResult("s", "t1", "tool", 1);
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), List.of(), 0L, false, TS);
        assertThat(acc.isComplete()).isTrue();
    }

    @Test
    @DisplayName("isComplete (count fallback): false when not enough results")
    void isComplete_countFallback_false() {
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 2, List.of(toolResult("s", "t1", "x", 2)), List.of(), 0L, false, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    @Test
    @DisplayName("isComplete (expected set): true only when every expected id has a result")
    void isComplete_expectedSet() {
        List<ExpectedTool> expected = List.of(new ExpectedTool("a", "ta"), new ExpectedTool("b", "tb"));

        ToolResultAccumulator partial = new ToolResultAccumulator(
                "s", "u", 2, List.of(toolResult("s", "a", "ta", 2)), expected, 0L, false, TS);
        assertThat(partial.isComplete()).isFalse();
        assertThat(partial.missing()).extracting(ExpectedTool::toolUseId).containsExactly("b");

        ToolResultAccumulator full = new ToolResultAccumulator(
                "s", "u", 2,
                List.of(toolResult("s", "a", "ta", 2), toolResult("s", "b", "tb", 2)),
                expected, 0L, false, TS);
        assertThat(full.isComplete()).isTrue();
        assertThat(full.missing()).isEmpty();
    }

    @Test
    @DisplayName("isComplete: false when already emitted")
    void isComplete_false_alreadyEmitted() {
        ToolUseResult r = toolResult("s", "t1", "tool", 1);
        ToolResultAccumulator acc = new ToolResultAccumulator(
                "s", "u", 1, List.of(r), List.of(), 0L, true, TS);
        assertThat(acc.isComplete()).isFalse();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ToolUseResult toolResult(String sessionId, String toolUseId,
                                            String name, int totalTools) {
        return new ToolUseResult(sessionId, toolUseId, null, name,
                Map.of("status", "ok"), 320L, "success", totalTools, TS);
    }

    private static ToolUseItem toolItem(String sessionId, String toolUseId, String name) {
        return new ToolUseItem(toolUseId, name, name, Map.of(), sessionId, 0, TS);
    }

    private static ThinkResponse thinkSeed(String sessionId, String userId, ToolUseItem... items) {
        return new ThinkResponse(
                sessionId, userId, 0.0, 0.0, 0.0, 0, 0,
                List.of(), null, List.of(), List.of(items),
                false, false, 0, 0, 0.0, TS);
    }
}
