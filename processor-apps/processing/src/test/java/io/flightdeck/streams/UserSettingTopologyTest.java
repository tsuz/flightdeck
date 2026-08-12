package io.flightdeck.streams;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.model.FullSessionContext;
import io.flightdeck.streams.model.MessageInput;
import io.flightdeck.streams.model.UserSettings;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests that the ENABLE_USER_SETTING flag correctly includes or excludes
 * the user-settings KTable and its join from the topology.
 */
class UserSettingTopologyTest {

    private static final String TS = "2026-03-10T12:00:00Z";

    // ── User settings ENABLED ───────────────────────────────────────────────

    @Nested
    @DisplayName("When ENABLE_USER_SETTING=true")
    class UserSettingEnabled {

        private TopologyTestDriver driver;
        private TestInputTopic<String, MessageInput> messageInput;
        private TestInputTopic<String, UserSettings> userSettingsInput;
        private TestOutputTopic<String, FullSessionContext> enrichedOutput;

        @BeforeEach
        void setUp() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true, true);
            driver = new TopologyTestDriver(topology, testProps());

            messageInput = driver.createInputTopic(
                    Topics.MESSAGE_INPUT,
                    Serdes.String().serializer(),
                    JsonSerde.of(MessageInput.class).serializer());

            userSettingsInput = driver.createInputTopic(
                    Topics.USER_SETTINGS,
                    Serdes.String().serializer(),
                    JsonSerde.of(UserSettings.class).serializer());

            enrichedOutput = driver.createOutputTopic(
                    Topics.ENRICHED_MESSAGE_INPUT,
                    Serdes.String().deserializer(),
                    JsonSerde.of(FullSessionContext.class).deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Per-user system prompt is joined into the enriched message")
        void systemPromptJoinedIntoEnrichedMessage() {
            userSettingsInput.pipeInput("user-1", new UserSettings("You are a billing agent.", TS));

            messageInput.pipeInput("sess-1", userMsg("sess-1", "user-1", "Hello"));

            FullSessionContext result = enrichedOutput.readRecord().value();
            assertThat(result.systemPrompt()).isEqualTo("You are a billing agent.");
        }

        @Test
        @DisplayName("system_prompt stays null for users without a settings record")
        void systemPromptNullWithoutSettings() {
            messageInput.pipeInput("sess-2", userMsg("sess-2", "user-none", "Hello"));

            assertThat(enrichedOutput.readRecord().value().systemPrompt()).isNull();
        }

        @Test
        @DisplayName("user-settings-store is registered in topology")
        void userSettingsStoreRegistered() {
            String description = FlightDeckStreamsApp.buildTopology(true, true)
                    .describe().toString();
            assertThat(description).contains(FlightDeckStreamsApp.USER_SETTINGS_STORE);
        }
    }

    // ── User settings ENABLED while memoir is DISABLED ──────────────────────

    @Nested
    @DisplayName("When ENABLE_USER_SETTING=true and MEMOIR_ENABLED=false")
    class UserSettingEnabledMemoirDisabled {

        private TopologyTestDriver driver;
        private TestInputTopic<String, MessageInput> messageInput;
        private TestInputTopic<String, UserSettings> userSettingsInput;
        private TestOutputTopic<String, FullSessionContext> enrichedOutput;

        @BeforeEach
        void setUp() {
            Topology topology = FlightDeckStreamsApp.buildTopology(false, true);
            driver = new TopologyTestDriver(topology, testProps());

            messageInput = driver.createInputTopic(
                    Topics.MESSAGE_INPUT,
                    Serdes.String().serializer(),
                    JsonSerde.of(MessageInput.class).serializer());

            userSettingsInput = driver.createInputTopic(
                    Topics.USER_SETTINGS,
                    Serdes.String().serializer(),
                    JsonSerde.of(UserSettings.class).serializer());

            enrichedOutput = driver.createOutputTopic(
                    Topics.ENRICHED_MESSAGE_INPUT,
                    Serdes.String().deserializer(),
                    JsonSerde.of(FullSessionContext.class).deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("System prompt join works without the memoir join")
        void systemPromptJoinWorksWithoutMemoir() {
            userSettingsInput.pipeInput("user-3", new UserSettings("Custom prompt.", TS));

            messageInput.pipeInput("sess-3", userMsg("sess-3", "user-3", "Hello"));

            FullSessionContext result = enrichedOutput.readRecord().value();
            assertThat(result.systemPrompt()).isEqualTo("Custom prompt.");
            assertThat(result.memoirContext()).isNull();
        }

        @Test
        @DisplayName("Output key is preserved as session_id after the user-keyed join")
        void outputKeyPreserved() {
            userSettingsInput.pipeInput("user-4", new UserSettings("p", TS));
            messageInput.pipeInput("sess-4", userMsg("sess-4", "user-4", "hi"));

            assertThat(enrichedOutput.readRecord().key()).isEqualTo("sess-4");
        }
    }

    // ── User settings DISABLED ──────────────────────────────────────────────

    @Nested
    @DisplayName("When ENABLE_USER_SETTING=false")
    class UserSettingDisabled {

        private TopologyTestDriver driver;
        private TestInputTopic<String, MessageInput> messageInput;
        private TestOutputTopic<String, FullSessionContext> enrichedOutput;

        @BeforeEach
        void setUp() {
            Topology topology = FlightDeckStreamsApp.buildTopology(true, false);
            driver = new TopologyTestDriver(topology, testProps());

            messageInput = driver.createInputTopic(
                    Topics.MESSAGE_INPUT,
                    Serdes.String().serializer(),
                    JsonSerde.of(MessageInput.class).serializer());

            enrichedOutput = driver.createOutputTopic(
                    Topics.ENRICHED_MESSAGE_INPUT,
                    Serdes.String().deserializer(),
                    JsonSerde.of(FullSessionContext.class).deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Enriched message has null system_prompt")
        void systemPromptIsNull() {
            messageInput.pipeInput("sess-5", userMsg("sess-5", "user-5", "Hello"));

            assertThat(enrichedOutput.readRecord().value().systemPrompt()).isNull();
        }

        @Test
        @DisplayName("user-settings topic and store are not registered in topology")
        void userSettingsNotInTopology() {
            String description = FlightDeckStreamsApp.buildTopology(true, false)
                    .describe().toString();
            assertThat(description).doesNotContain(FlightDeckStreamsApp.USER_SETTINGS_STORE);
            assertThat(description).doesNotContain(Topics.USER_SETTINGS);
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static Properties testProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-user-setting");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return props;
    }

    private static MessageInput userMsg(String sessionId, String userId, String content) {
        return new MessageInput(sessionId, userId, "user", content, TS, Map.of());
    }
}
