package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.*;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

class AccumulateSessionContextProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ThinkResponse>      thinkInput;
    private TestOutputTopic<String, SessionContext>    contextOutput;
    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));
        AccumulateSessionContextProcessor.register(builder, thinkStream);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-accumulate");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        thinkInput       = driver.createInputTopic(Topics.THINK_REQUEST_RESPONSE,
                Serdes.String().serializer(), JsonSerde.of(ThinkResponse.class).serializer());
        contextOutput    = driver.createOutputTopic(Topics.SESSION_CONTEXT,
                Serdes.String().deserializer(), JsonSerde.of(SessionContext.class).deserializer());
    }

    @AfterEach
    void tearDown() { driver.close(); }

    // ── Aggregation ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("First ThinkResponse creates a SessionContext with one message in history")
    void firstResponse_createsContext() {
        thinkInput.pipeInput("sess-1", thinkResp("sess-1", "user-A", 0.02, List.of(
                assistantMsg("sess-1", "user-A", "Sure, I can help."))));

        SessionContext ctx = contextOutput.readRecord().value();
        assertThat(ctx.sessionId()).isEqualTo("sess-1");
        assertThat(ctx.userId()).isEqualTo("user-A");
        assertThat(ctx.llmCalls()).isEqualTo(1);
        assertThat(ctx.cost()).isCloseTo(0.02, within(0.0001));
        assertThat(ctx.history()).hasSize(1);
        assertThat(ctx.history().get(0).content()).isEqualTo("Sure, I can help.");
    }

    @Test
    @DisplayName("Second ThinkResponse appends to history and accumulates cost + call count")
    void secondResponse_appendsHistory() {
        thinkInput.pipeInput("sess-2", thinkResp("sess-2", "u", 0.03, List.of(
                assistantMsg("sess-2", "u", "First."))));
        thinkInput.pipeInput("sess-2", thinkResp("sess-2", "u", 0.05, List.of(
                assistantMsg("sess-2", "u", "Second."))));

        List<TestRecord<String, SessionContext>> records = contextOutput.readRecordsToList();
        assertThat(records).hasSize(2);
        SessionContext latest = records.get(1).value();
        assertThat(latest.llmCalls()).isEqualTo(2);
        assertThat(latest.cost()).isCloseTo(0.08, within(0.0001));
        assertThat(latest.history()).extracting(MessageInput::content)
                .containsExactly("First.", "Second.");
    }

    @Test
    @DisplayName("Multiple messages in a single ThinkResponse all land in history")
    void multipleMessagesInOneResponse() {
        thinkInput.pipeInput("sess-3", thinkResp("sess-3", "u", 0.04, List.of(
                userMsg("sess-3", "u", "What's my balance?"),
                assistantMsg("sess-3", "u", "Your balance is $142.50."))));

        SessionContext ctx = contextOutput.readRecord().value();
        assertThat(ctx.history()).hasSize(2);
        assertThat(ctx.messages()).hasSize(2);
    }

    @Test
    @DisplayName("Sessions maintain independent history — no cross-contamination")
    void independentSessions() {
        thinkInput.pipeInput("A", thinkResp("A", "u1", 0.01, List.of(assistantMsg("A","u1","a1"))));
        thinkInput.pipeInput("B", thinkResp("B", "u2", 0.02, List.of(assistantMsg("B","u2","b1"))));
        thinkInput.pipeInput("A", thinkResp("A", "u1", 0.01, List.of(assistantMsg("A","u1","a2"))));

        List<TestRecord<String, SessionContext>> all = contextOutput.readRecordsToList();

        SessionContext ctxA = all.stream().filter(r -> "A".equals(r.key()))
                .reduce((a,b)->b).orElseThrow().value();
        SessionContext ctxB = all.stream().filter(r -> "B".equals(r.key()))
                .reduce((a,b)->b).orElseThrow().value();

        assertThat(ctxA.history()).hasSize(2);
        assertThat(ctxB.history()).hasSize(1);
    }

    // ── Helper unit tests ─────────────────────────────────────────────────────

    @Test
    @DisplayName("appendMessages: null history is treated as empty")
    void appendMessages_nullHistory() {
        List<MessageInput> r = AccumulateSessionContextProcessor
                .appendMessages(null, List.of(userMsg("s","u","x")));
        assertThat(r).hasSize(1);
    }

    @Test
    @DisplayName("appendMessages: null newMessages is treated as empty")
    void appendMessages_nullNew() {
        List<MessageInput> r = AccumulateSessionContextProcessor
                .appendMessages(List.of(userMsg("s","u","x")), null);
        assertThat(r).hasSize(1);
    }

    @Test @DisplayName("resolveUserId prefers incoming non-blank value")
    void resolveUserId_incoming() {
        assertThat(AccumulateSessionContextProcessor.resolveUserId("old","new")).isEqualTo("new");
    }

    @Test @DisplayName("resolveUserId falls back to existing when incoming is null")
    void resolveUserId_null() {
        assertThat(AccumulateSessionContextProcessor.resolveUserId("old", null)).isEqualTo("old");
    }

    @Test @DisplayName("resolveUserId falls back to existing when incoming is blank")
    void resolveUserId_blank() {
        assertThat(AccumulateSessionContextProcessor.resolveUserId("old", "  ")).isEqualTo("old");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static final String TS = "2026-03-10T12:00:00Z";

    private static ThinkResponse thinkResp(String sid, String uid, double cost,
                                           List<MessageInput> msgs) {
        return new ThinkResponse(sid, uid, cost, 100, 50, msgs, List.of(), true, TS);
    }

    private static MessageInput userMsg(String sid, String uid, String content) {
        return new MessageInput(sid, uid, "user", content, TS, Map.of());
    }

    private static MessageInput assistantMsg(String sid, String uid, String content) {
        return new MessageInput(sid, uid, "assistant", content, TS, Map.of());
    }
}