package io.axon.streams;

import io.axon.streams.processors.AccumulateSessionContextProcessor;
import io.axon.streams.processors.EnrichInputMessageProcessor;
import io.axon.streams.processors.ExtractToolUseItemsProcessor;
import io.axon.streams.processors.SessionCostAggregationProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Entry-point: wires all processor fragments into a single Kafka Streams topology
 * and starts the application.
 *
 * As additional beige processors are implemented (AccumulateSessionContext,
 * Think, ToolExecution, ToolResultAggregation, ToolLatencyAggregation) each
 * one is registered here with a single call.
 */
public class AxonStreamsApp {

    private static final Logger log = LoggerFactory.getLogger(AxonStreamsApp.class);

    public static void main(String[] args) {
        Properties props = buildConfig();
        Topology topology = buildTopology();

        log.info("Topology description:\n{}", topology.describe());

        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.setUncaughtExceptionHandler((e) -> {
                log.error("Uncaught stream exception — shutting down", e);
                return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            streams.start();
            log.info("Axon Streams started.");
        }
    }

    /** Build and return the full topology (also used by tests). */
    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // ── Register each beige processor fragment ───────────────────────────
        ExtractToolUseItemsProcessor.register(builder);
        AccumulateSessionContextProcessor.register(builder);
        EnrichInputMessageProcessor.register(builder);
        SessionCostAggregationProcessor.register(builder);
        // ThinkProcessor.register(builder);                       // TODO
        // ToolExecutionProcessor.register(builder);               // TODO
        // ToolResultAggregationProcessor.register(builder);       // TODO
        // ToolLatencyAggregationProcessor.register(builder);      // TODO

        return builder.build();
    }

    private static Properties buildConfig() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG,    "axon-streams");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
              org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
              org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        // At-least-once delivery; change to EXACTLY_ONCE_V2 for production
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        return p;
    }
}