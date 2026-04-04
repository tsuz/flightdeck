package io.flightdeck.streams.config;

import io.flightdeck.streams.FlightDeckStreamsApp;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests that AGENT_NAME is required and correctly prefixes all topic names.
 * The surefire plugin sets AGENT_NAME=test for the test environment.
 */
class TopicsTest {

    @Test
    @DisplayName("AGENT_NAME is read from environment and is non-empty")
    void agentNameIsSet() {
        assertThat(Topics.AGENT_NAME).isNotNull().isNotBlank();
    }

    @Test
    @DisplayName("PREFIX is AGENT_NAME followed by a dash")
    void prefixFormat() {
        assertThat(Topics.PREFIX).isEqualTo(Topics.AGENT_NAME + "-");
    }

    @Test
    @DisplayName("All topic names start with the AGENT_NAME prefix")
    void allTopicsArePrefixed() {
        String prefix = Topics.AGENT_NAME + "-";

        assertThat(Topics.MESSAGE_INPUT).startsWith(prefix);
        assertThat(Topics.SESSION_CONTEXT).startsWith(prefix);
        assertThat(Topics.ENRICHED_MESSAGE_INPUT).startsWith(prefix);
        assertThat(Topics.THINK_REQUEST_RESPONSE).startsWith(prefix);
        assertThat(Topics.TOOL_USE).startsWith(prefix);
        assertThat(Topics.TOOL_USE_DLQ).startsWith(prefix);
        assertThat(Topics.TOOL_USE_RESULT).startsWith(prefix);
        assertThat(Topics.TOOL_USE_ALL_COMPLETE).startsWith(prefix);
        assertThat(Topics.TOOL_USE_LATENCY).startsWith(prefix);
        assertThat(Topics.SESSION_END).startsWith(prefix);
        assertThat(Topics.MEMOIR_CONTEXT).startsWith(prefix);
        assertThat(Topics.MEMOIR_CONTEXT_SESSION_END).startsWith(prefix);
        assertThat(Topics.MESSAGE_OUTPUT).startsWith(prefix);
    }

    @Test
    @DisplayName("Topic names have the correct base name after the prefix")
    void topicBaseNames() {
        String p = Topics.PREFIX;

        assertThat(Topics.MESSAGE_INPUT).isEqualTo(p + "message-input");
        assertThat(Topics.SESSION_CONTEXT).isEqualTo(p + "session-context");
        assertThat(Topics.ENRICHED_MESSAGE_INPUT).isEqualTo(p + "enriched-message-input");
        assertThat(Topics.THINK_REQUEST_RESPONSE).isEqualTo(p + "think-request-response");
        assertThat(Topics.TOOL_USE).isEqualTo(p + "tool-use");
        assertThat(Topics.TOOL_USE_DLQ).isEqualTo(p + "tool-use-dlq");
        assertThat(Topics.TOOL_USE_RESULT).isEqualTo(p + "tool-use-result");
        assertThat(Topics.TOOL_USE_ALL_COMPLETE).isEqualTo(p + "tool-use-all-complete");
        assertThat(Topics.TOOL_USE_LATENCY).isEqualTo(p + "tool-use-latency");
        assertThat(Topics.SESSION_END).isEqualTo(p + "session-end");
        assertThat(Topics.MEMOIR_CONTEXT).isEqualTo(p + "memoir-context");
        assertThat(Topics.MEMOIR_CONTEXT_SESSION_END).isEqualTo(p + "memoir-context-session-end");
        assertThat(Topics.MESSAGE_OUTPUT).isEqualTo(p + "message-output");
    }

    @Test
    @DisplayName("No topic name contains a double dash (AGENT_NAME is not empty)")
    void noDoubleDash() {
        assertThat(Topics.MESSAGE_INPUT).doesNotContain("--");
        assertThat(Topics.THINK_REQUEST_RESPONSE).doesNotContain("--");
        assertThat(Topics.MESSAGE_OUTPUT).doesNotContain("--");
    }

    @Test
    @DisplayName("Topology uses AGENT_NAME-prefixed topics as sources and sinks")
    void topologyUsesAgentNameTopics() {
        Topology topology = FlightDeckStreamsApp.buildTopology();
        String description = topology.describe().toString();

        assertThat(description).contains(Topics.MESSAGE_INPUT);
        assertThat(description).contains(Topics.ENRICHED_MESSAGE_INPUT);
        assertThat(description).contains(Topics.THINK_REQUEST_RESPONSE);
        assertThat(description).contains(Topics.MESSAGE_OUTPUT);
        assertThat(description).contains(Topics.TOOL_USE);
        assertThat(description).contains(Topics.SESSION_END);
        assertThat(description).contains(Topics.MEMOIR_CONTEXT_SESSION_END);
    }

    @Test
    @DisplayName("Kafka Streams application ID is AGENT_NAME-streams")
    void streamsApplicationIdUsesAgentName() {
        java.util.Properties config = FlightDeckStreamsApp.buildConfig();
        String appId = config.getProperty("application.id");

        assertThat(appId).isEqualTo(Topics.AGENT_NAME + "-streams");
    }
}
