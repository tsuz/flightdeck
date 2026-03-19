package io.axon.streams.processors;

import io.axon.streams.config.Topics;
import io.axon.streams.model.ThinkResponse;
import io.axon.streams.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link SessionEndProcessor} — inactivity-based session end detection.
 */
class SessionEndProcessorTest {

    private static final String TS = "2026-03-10T12:00:00Z";

    // ── Default threshold ────────────────────────────────────────────────────

    @Test
    @DisplayName("Default threshold is read from env or falls back to 20s")
    void defaultThreshold() {
        String envValue = System.getenv("MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS");
        long expected = envValue != null ? Long.parseLong(envValue) : 20;
        assertThat(SessionEndProcessor.INACTIVITY_THRESHOLD.getSeconds()).isEqualTo(expected);
    }

    // ── Custom threshold via register overload ───────────────────────────────

    @Nested
    @DisplayName("With 1-second inactivity threshold")
    class ShortThreshold {

        private TopologyTestDriver driver;
        private TestInputTopic<String, ThinkResponse> thinkInput;
        private TestOutputTopic<String, String> sessionEndOutput;

        @BeforeEach
        void setUp() {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, ThinkResponse> thinkStream = builder.stream(
                    Topics.THINK_REQUEST_RESPONSE,
                    Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));

            SessionEndProcessor.register(builder, thinkStream, Duration.ofSeconds(1));

            driver = new TopologyTestDriver(builder.build(), testProps());

            thinkInput = driver.createInputTopic(
                    Topics.THINK_REQUEST_RESPONSE,
                    Serdes.String().serializer(),
                    JsonSerde.of(ThinkResponse.class).serializer());

            sessionEndOutput = driver.createOutputTopic(
                    Topics.SESSION_END,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("No session-end emitted before threshold elapses")
        void noSessionEndBeforeThreshold() {
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            // Advance wall clock by less than the threshold
            driver.advanceWallClockTime(Duration.ofMillis(500));

            assertThat(sessionEndOutput.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Session-end emitted after threshold elapses")
        void sessionEndAfterThreshold() throws InterruptedException {
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            // Sleep so System.currentTimeMillis() in the processor sees real elapsed time
            Thread.sleep(1100);
            driver.advanceWallClockTime(Duration.ofSeconds(6));

            assertThat(sessionEndOutput.isEmpty()).isFalse();
            assertThat(sessionEndOutput.readRecord().key()).isEqualTo("sess-1");
        }

        @Test
        @DisplayName("Activity resets the inactivity timer")
        void activityResetsTimer() throws InterruptedException {
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            Thread.sleep(600);
            // Send another message before the 1s threshold — this resets the timer
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            Thread.sleep(600);
            driver.advanceWallClockTime(Duration.ofSeconds(6));

            // Should NOT have fired — total inactivity since last message is only ~600ms
            assertThat(sessionEndOutput.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Multiple sessions end independently")
        void multipleSessions() throws InterruptedException {
            thinkInput.pipeInput("sess-A", thinkResponse("sess-A"));
            thinkInput.pipeInput("sess-B", thinkResponse("sess-B"));

            Thread.sleep(1100);
            driver.advanceWallClockTime(Duration.ofSeconds(6));

            List<String> endedSessions = sessionEndOutput.readRecordsToList()
                    .stream().map(r -> r.key()).toList();
            assertThat(endedSessions).containsExactlyInAnyOrder("sess-A", "sess-B");
        }

        @Test
        @DisplayName("Session is not emitted twice after already ended")
        void noDoubleEmit() throws InterruptedException {
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            Thread.sleep(1100);
            driver.advanceWallClockTime(Duration.ofSeconds(6));
            assertThat(sessionEndOutput.readRecordsToList()).hasSize(1);

            // Advance again — should not emit again
            driver.advanceWallClockTime(Duration.ofSeconds(6));
            assertThat(sessionEndOutput.isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("With 10-second inactivity threshold")
    class LongerThreshold {

        private TopologyTestDriver driver;
        private TestInputTopic<String, ThinkResponse> thinkInput;
        private TestOutputTopic<String, String> sessionEndOutput;

        @BeforeEach
        void setUp() {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, ThinkResponse> thinkStream = builder.stream(
                    Topics.THINK_REQUEST_RESPONSE,
                    Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class)));

            SessionEndProcessor.register(builder, thinkStream, Duration.ofSeconds(10));

            driver = new TopologyTestDriver(builder.build(), testProps());

            thinkInput = driver.createInputTopic(
                    Topics.THINK_REQUEST_RESPONSE,
                    Serdes.String().serializer(),
                    JsonSerde.of(ThinkResponse.class).serializer());

            sessionEndOutput = driver.createOutputTopic(
                    Topics.SESSION_END,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("No session-end after 1s with 10s threshold")
        void noSessionEndWithLongerThreshold() throws InterruptedException {
            thinkInput.pipeInput("sess-1", thinkResponse("sess-1"));

            Thread.sleep(1100);
            driver.advanceWallClockTime(Duration.ofSeconds(6));

            // 1s elapsed is well under the 10s threshold
            assertThat(sessionEndOutput.isEmpty()).isTrue();
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static Properties testProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-session-end");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        return props;
    }

    private static ThinkResponse thinkResponse(String sessionId) {
        return new ThinkResponse(sessionId, "user-1", 0.01, 100, 50,
                List.of(), null, true, TS);
    }
}
