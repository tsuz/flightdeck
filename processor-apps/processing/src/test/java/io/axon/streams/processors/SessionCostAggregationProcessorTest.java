package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.MessageInput;
import io.axon.streams.model.SessionCost;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.model.ToolUseItem;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Properties;

import static io.axon.streams.processors.SessionCostAggregationProcessor.*;
import static org.assertj.core.api.Assertions.*;

class SessionCostAggregationProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ThinkResponse> thinkInput;
    private TestOutputTopic<String, SessionCost>  costOutput;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));
        SessionCostAggregationProcessor.register(builder, thinkStream);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-session-cost");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);

        thinkInput = driver.createInputTopic(
                Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(),
                JsonSerde.of(ThinkResponse.class).serializer());

        costOutput = driver.createOutputTopic(
                Topics.SESSION_COST,
                Serdes.String().deserializer(),
                JsonSerde.of(SessionCost.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Aggregation ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("First ThinkResponse creates a SessionCost with llmCalls=1")
    void firstResponse_createsSessionCost() {
        thinkInput.pipeInput("sess-1", response("sess-1", "user-A", 0.005, 200, 80));

        SessionCost cost = costOutput.readRecord().value();
        assertThat(cost.sessionId()).isEqualTo("sess-1");
        assertThat(cost.userId()).isEqualTo("user-A");
        assertThat(cost.llmCalls()).isEqualTo(1);
        assertThat(cost.totalInputTokens()).isEqualTo(200);
        assertThat(cost.totalOutputTokens()).isEqualTo(80);
        assertThat(cost.estimatedCostUsd()).isCloseTo(0.005, within(0.000001));
    }

    @Test
    @DisplayName("Subsequent responses accumulate tokens and cost")
    void multipleResponses_accumulate() {
        thinkInput.pipeInput("sess-2", response("sess-2", "u", 0.003, 100, 50));
        thinkInput.pipeInput("sess-2", response("sess-2", "u", 0.007, 300, 120));
        thinkInput.pipeInput("sess-2", response("sess-2", "u", 0.002,  80,  30));

        List<TestRecord<String, SessionCost>> records = costOutput.readRecordsToList();
        assertThat(records).hasSize(3);

        SessionCost latest = records.get(2).value();
        assertThat(latest.llmCalls()).isEqualTo(3);
        assertThat(latest.totalInputTokens()).isEqualTo(480);
        assertThat(latest.totalOutputTokens()).isEqualTo(200);
        assertThat(latest.estimatedCostUsd()).isCloseTo(0.012, within(0.000001));
    }

    @Test
    @DisplayName("Different sessions maintain independent cost accumulators")
    void independentSessions() {
        thinkInput.pipeInput("sess-A", response("sess-A", "u1", 0.01, 100, 50));
        thinkInput.pipeInput("sess-B", response("sess-B", "u2", 0.02, 200, 80));
        thinkInput.pipeInput("sess-A", response("sess-A", "u1", 0.03, 150, 60));

        List<TestRecord<String, SessionCost>> all = costOutput.readRecordsToList();

        SessionCost latestA = all.stream().filter(r -> "sess-A".equals(r.key()))
                .reduce((a, b) -> b).orElseThrow().value();
        SessionCost latestB = all.stream().filter(r -> "sess-B".equals(r.key()))
                .reduce((a, b) -> b).orElseThrow().value();

        assertThat(latestA.llmCalls()).isEqualTo(2);
        assertThat(latestA.estimatedCostUsd()).isCloseTo(0.04, within(0.000001));
        assertThat(latestB.llmCalls()).isEqualTo(1);
        assertThat(latestB.estimatedCostUsd()).isCloseTo(0.02, within(0.000001));
    }

    @Test
    @DisplayName("Output record key is the session_id")
    void outputKey_isSessionId() {
        thinkInput.pipeInput("sess-key", response("sess-key", "u", 0.001, 50, 20));
        assertThat(costOutput.readRecord().key()).isEqualTo("sess-key");
    }

    // ── Fallback cost estimation ───────────────────────────────────────────────

    @Test
    @DisplayName("When response.cost is 0, estimateCost fallback is applied")
    void zeroCostField_usesFallbackEstimate() {
        // 1M input tokens + 1M output tokens at list price = $3 + $15 = $18
        thinkInput.pipeInput("sess-est", response("sess-est", "u", 0.0, 1_000_000, 1_000_000));

        SessionCost cost = costOutput.readRecord().value();
        assertThat(cost.estimatedCostUsd())
                .isCloseTo(INPUT_COST_PER_M_TOKENS + OUTPUT_COST_PER_M_TOKENS, within(0.001));
    }

    @Test
    @DisplayName("estimateCost unit test — 500k input + 200k output tokens")
    void estimateCost_unit() {
        double cost = estimateCost(500_000, 200_000);
        double expected = (500_000 / 1_000_000.0) * INPUT_COST_PER_M_TOKENS
                        + (200_000 / 1_000_000.0) * OUTPUT_COST_PER_M_TOKENS;
        assertThat(cost).isCloseTo(expected, within(0.000001));
    }

    @Test
    @DisplayName("estimateCost with zero tokens returns 0.0")
    void estimateCost_zeroTokens() {
        assertThat(estimateCost(0, 0)).isEqualTo(0.0);
    }

    // ── Tombstone / session close ─────────────────────────────────────────────

    @Test
    @DisplayName("Close-sentinel response emits a tombstone (null value) on session-cost")
    void closeSignal_emitsTombstone() {
        // Seed a real cost first
        thinkInput.pipeInput("sess-close", response("sess-close", "u", 0.01, 100, 50));
        costOutput.readRecord(); // drain the normal update

        // Send close sentinel
        thinkInput.pipeInput("sess-close", closeSignal("sess-close"));

        TestRecord<String, SessionCost> tombstone = costOutput.readRecord();
        assertThat(tombstone.key()).isEqualTo("sess-close");
        assertThat(tombstone.value()).isNull();
    }

    @Test
    @DisplayName("Close-sentinel for unknown session still emits tombstone")
    void closeSignalWithNoHistory_stillEmitsTombstone() {
        thinkInput.pipeInput("sess-unknown", closeSignal("sess-unknown"));

        TestRecord<String, SessionCost> tombstone = costOutput.readRecord();
        assertThat(tombstone.key()).isEqualTo("sess-unknown");
        assertThat(tombstone.value()).isNull();
    }

    // ── userId resolution ─────────────────────────────────────────────────────

    @Test
    @DisplayName("resolveUserId preserves existing when incoming is null")
    void resolveUserId_null() {
        assertThat(resolveUserId("existing", null)).isEqualTo("existing");
    }

    @Test
    @DisplayName("resolveUserId preserves existing when incoming is blank")
    void resolveUserId_blank() {
        assertThat(resolveUserId("existing", "  ")).isEqualTo("existing");
    }

    @Test
    @DisplayName("resolveUserId uses incoming when provided")
    void resolveUserId_incoming() {
        assertThat(resolveUserId("old", "new")).isEqualTo("new");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ThinkResponse response(String sessionId, String userId,
                                          double cost, int inputTokens, int outputTokens) {
        return new ThinkResponse(sessionId, userId, cost, inputTokens, outputTokens,
                List.of(), List.of(), false, TS);
    }

    /** Builds the sentinel close signal recognised by the processor. */
    private static ThinkResponse closeSignal(String sessionId) {
        return new ThinkResponse(sessionId, null, SESSION_CLOSE_SENTINEL,
                0, 0, List.of(), List.of(), true, TS);
    }
}