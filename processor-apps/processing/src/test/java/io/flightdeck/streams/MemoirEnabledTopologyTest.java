package io.flightdeck.streams;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.FullSessionContext;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.ThinkResponse;
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
 * Tests that the MEMOIR_ENABLED flag correctly includes or excludes
 * memoir-related processors from the topology.
 */
class MemoirEnabledTopologyTest {

    private static final String TS = "2026-03-10T12:00:00Z";

    // ── Memoir ENABLED ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("When MEMOIR_ENABLED=true")
    class MemoirEnabled {

        private TopologyTestDriver driver;
        private TestInputTopic<String, MessageInput> messageInput;
        private TestInputTopic<String, ThinkResponse> thinkInput;
        private TestInputTopic<String, String> memoirInput;
        private TestOutputTopic<String, FullSessionContext> enrichedOutput;
        private TestOutputTopic<String, String> memoirSessionEndOutput;

        @BeforeEach
        void setUp() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true);
            driver = new TopologyTestDriver(topology, testProps());

            messageInput = driver.createInputTopic(
                    Topics.MESSAGE_INPUT,
                    Serdes.String().serializer(),
                    JsonSerde.of(MessageInput.class).serializer());

            thinkInput = driver.createInputTopic(
                    Topics.THINK_REQUEST_RESPONSE,
                    Serdes.String().serializer(),
                    JsonSerde.of(ThinkResponse.class).serializer());

            memoirInput = driver.createInputTopic(
                    Topics.MEMOIR_CONTEXT,
                    Serdes.String().serializer(),
                    Serdes.String().serializer());

            enrichedOutput = driver.createOutputTopic(
                    Topics.ENRICHED_MESSAGE_INPUT,
                    Serdes.String().deserializer(),
                    JsonSerde.of(FullSessionContext.class).deserializer());

            memoirSessionEndOutput = driver.createOutputTopic(
                    Topics.MEMOIR_CONTEXT_SESSION_END,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Memoir context is joined into enriched message")
        void memoirContextIncludedInEnrichedMessage() {
            // Seed memoir for user-1
            memoirInput.pipeInput("user-1", "User prefers concise answers.");

            // Send a message
            messageInput.pipeInput("sess-1", userMsg("sess-1", "user-1", "Hello"));

            FullSessionContext result = enrichedOutput.readRecord().value();
            assertThat(result.memoirContext()).isEqualTo("User prefers concise answers.");
        }

        @Test
        @DisplayName("memoir-context-session-end sink is registered in topology")
        void memoirSessionEndProcessorRegistered() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true);
            String description = topology.describe().toString();
            assertThat(description).contains(Topics.MEMOIR_CONTEXT_SESSION_END);
        }

        @Test
        @DisplayName("memoir-context-store is registered in topology")
        void memoirContextStoreRegistered() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true);
            String description = topology.describe().toString();
            assertThat(description).contains("memoir-context-store");
        }

        @Test
        @DisplayName("session-end sink and store are registered in topology")
        void sessionEndProcessorRegistered() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true);
            String description = topology.describe().toString();
            assertThat(description).contains(Topics.SESSION_END);
            assertThat(description).contains("session-last-seen-store");
        }
    }

    // ── Memoir DISABLED ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("When MEMOIR_ENABLED=false")
    class MemoirDisabled {

        private TopologyTestDriver driver;
        private TestInputTopic<String, MessageInput> messageInput;
        private TestInputTopic<String, ThinkResponse> thinkInput;
        private TestOutputTopic<String, FullSessionContext> enrichedOutput;

        @BeforeEach
        void setUp() {
            Topology topology = FlightDeckStreamsApp.buildTopology(false);
            driver = new TopologyTestDriver(topology, testProps());

            messageInput = driver.createInputTopic(
                    Topics.MESSAGE_INPUT,
                    Serdes.String().serializer(),
                    JsonSerde.of(MessageInput.class).serializer());

            thinkInput = driver.createInputTopic(
                    Topics.THINK_REQUEST_RESPONSE,
                    Serdes.String().serializer(),
                    JsonSerde.of(ThinkResponse.class).serializer());

            enrichedOutput = driver.createOutputTopic(
                    Topics.ENRICHED_MESSAGE_INPUT,
                    Serdes.String().deserializer(),
                    JsonSerde.of(FullSessionContext.class).deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Enriched message has null memoir context")
        void memoirContextIsNull() {
            messageInput.pipeInput("sess-1", userMsg("sess-1", "user-1", "Hello"));

            FullSessionContext result = enrichedOutput.readRecord().value();
            assertThat(result.memoirContext()).isNull();
        }

        @Test
        @DisplayName("Enriched message still includes session history from ThinkResponse")
        void sessionHistoryStillWorks() {
            ThinkResponse prevResponse = new ThinkResponse("sess-2", "user-2", 0.01, null, 0.01, 100, 50,
                    null, null,
                    List.of(assistantMsg("sess-2", "user-2", "Prior reply.")),
                    List.of(), true, false, 0, 0, 0.0, TS);
            thinkInput.pipeInput("sess-2", prevResponse);

            messageInput.pipeInput("sess-2", userMsg("sess-2", "user-2", "Follow-up"));

            FullSessionContext result = enrichedOutput.readRecord().value();
            assertThat(result.history()).hasSize(1);
            assertThat(result.latestInput().content()).isEqualTo("Follow-up");
        }

        @Test
        @DisplayName("memoir-context topic is not registered in topology")
        void memoirTopicNotInTopology() {
            Topology topology = FlightDeckStreamsApp.buildTopology(false);
            String description = topology.describe().toString();
            assertThat(description).doesNotContain("memoir-context-store");
        }

        @Test
        @DisplayName("memoir-context-session-end sink is not registered in topology")
        void memoirSessionEndProcessorNotRegistered() {
            Topology topology = FlightDeckStreamsApp.buildTopology(false);
            String description = topology.describe().toString();
            assertThat(description).doesNotContain(Topics.MEMOIR_CONTEXT_SESSION_END);
        }

        @Test
        @DisplayName("session-end sink is not registered in topology")
        void sessionEndProcessorNotRegistered() {
            Topology topology = FlightDeckStreamsApp.buildTopology(false);
            String description = topology.describe().toString();
            assertThat(description).doesNotContain(Topics.SESSION_END);
            assertThat(description).doesNotContain("session-last-seen-store");
        }

        @Test
        @DisplayName("Output key is preserved as session_id")
        void outputKeyPreserved() {
            messageInput.pipeInput("sess-key", userMsg("sess-key", "user-1", "hi"));

            TestRecord<String, FullSessionContext> record = enrichedOutput.readRecord();
            assertThat(record.key()).isEqualTo("sess-key");
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static Properties testProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-memoir-enabled");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return props;
    }

    private static MessageInput userMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "user", content, TS, Map.of());
    }

    private static MessageInput assistantMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "assistant", content, TS, Map.of());
    }
}
