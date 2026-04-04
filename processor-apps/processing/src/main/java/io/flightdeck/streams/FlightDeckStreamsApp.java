package io.flightdeck.streams;

import io.flightdeck.streams.config.Topics;
import io.flightdeck.streams.processors.AggregateToolExecutionResultProcessor;
import io.flightdeck.streams.processors.EndTurnProcessor;
import io.flightdeck.streams.processors.EnrichInputMessageProcessor;
import io.flightdeck.streams.processors.ExtractToolUseItemsProcessor;
import io.flightdeck.streams.processors.SessionCostAggregationProcessor;
import io.flightdeck.streams.processors.MemoirSessionEndProcessor;
import io.flightdeck.streams.processors.SessionEndProcessor;
import io.flightdeck.streams.processors.TransformToolUseDoneProcessor;
import io.flightdeck.streams.model.SessionCost;
import io.flightdeck.streams.model.ThinkResponse;
import io.flightdeck.streams.serdes.JsonSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Entry-point: wires all processor fragments into a single Kafka Streams topology
 * and starts the application.
 */
public class FlightDeckStreamsApp {

    private static final Logger log = LoggerFactory.getLogger(FlightDeckStreamsApp.class);

    static final boolean MEMOIR_ENABLED = Boolean.parseBoolean(
            System.getenv().getOrDefault("MEMOIR_ENABLED", "true"));

    static final String MEMOIR_CONTEXT_STORE = "memoir-context-store";
    static final String THINK_RESPONSE_STORE = "think-response-store";
    static final String SESSION_COST_TABLE_STORE = "session-cost-table-store";

    public static void main(String[] args) {
        Properties props = buildConfig();
        ensureTopicsExist(props);
        Topology topology = buildTopology();

        log.info("Topology description:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        // Latch keeps the main thread alive until SIGTERM / Ctrl-C
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

        streams.setUncaughtExceptionHandler((e) -> {
            log.error("Uncaught stream exception — shutting down", e);
            latch.countDown();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — closing streams.");
            streams.close();
            latch.countDown();
        }));

        streams.start();
        log.info("FlightDeck Streams started. Waiting for messages...");

        try {
            latch.await(); // blocks here until shutdown hook fires or uncaught exception
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Build and return the full topology (also used by tests). */
    public static Topology buildTopology() {
        return buildTopology(MEMOIR_ENABLED);
    }

    static Topology buildTopology(boolean memoirEnabled) {
        StreamsBuilder builder = new StreamsBuilder();

        // ── Shared KTable: memoir-context (only when memoir is enabled) ───────
        KTable<String, String> memoirTable = null;
        if (memoirEnabled) {
            memoirTable = builder.table(
                    Topics.MEMOIR_CONTEXT,
                    Consumed.with(Serdes.String(), Serdes.String()),
                    Materialized.<String, String>as(
                                    Stores.persistentKeyValueStore(MEMOIR_CONTEXT_STORE))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String())
            );
        }

        // ── Shared KStream + KTable: think-request-response ─────────────────
        //    ONE source registration — shared across all processors that read
        //    this topic, avoiding TopologyException conflicts.
        KStream<String, ThinkResponse> thinkStream = builder.stream(
                Topics.THINK_REQUEST_RESPONSE,
                Consumed.with(Serdes.String(), JsonSerde.of(ThinkResponse.class))
        );

        KTable<String, ThinkResponse> thinkTable = thinkStream.toTable(
                Materialized.<String, ThinkResponse>as(
                                Stores.persistentKeyValueStore(THINK_RESPONSE_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(ThinkResponse.class))
        );

        // ── Shared KTable: session-cost (aggregated cost per session) ────────
        KTable<String, SessionCost> sessionCostTable = builder.table(
                Topics.SESSION_COST,
                Consumed.with(Serdes.String(), JsonSerde.of(SessionCost.class)),
                Materialized.<String, SessionCost>as(
                                Stores.persistentKeyValueStore(SESSION_COST_TABLE_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerde.of(SessionCost.class))
        );

        // ── Register each processor fragment ──────────────────────────────────
        EnrichInputMessageProcessor.register(builder, memoirTable, thinkTable, sessionCostTable);
        ExtractToolUseItemsProcessor.register(builder, thinkStream);
        SessionCostAggregationProcessor.register(builder, thinkStream);
        EndTurnProcessor.register(builder, thinkStream);
        AggregateToolExecutionResultProcessor.register(builder);
        TransformToolUseDoneProcessor.register(builder);

        if (memoirEnabled) {
            SessionEndProcessor.register(builder, thinkStream);
            MemoirSessionEndProcessor.register(builder, memoirTable, thinkTable);
            log.info("Memoir is ENABLED (inactivity_threshold={}s)",
                    SessionEndProcessor.INACTIVITY_THRESHOLD.getSeconds());
        } else {
            log.info("Memoir is DISABLED");
        }

        return builder.build();
    }

    /**
     * Pre-creates all source topics required by the topology.
     * Kafka Streams crashes with MissingSourceTopicException if source topics
     * don't exist at rebalance time, so we create them before starting.
     */
    private static void ensureTopicsExist(Properties streamsProps) {
        String bootstrapServers = streamsProps.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        List<String> requiredTopics = new java.util.ArrayList<>(List.of(
                Topics.MESSAGE_INPUT,
                Topics.ENRICHED_MESSAGE_INPUT,
                Topics.THINK_REQUEST_RESPONSE,
                Topics.TOOL_USE,
                Topics.TOOL_USE_DLQ,
                Topics.TOOL_USE_RESULT,
                Topics.TOOL_USE_ALL_COMPLETE,
                Topics.SESSION_COST,
                Topics.TOOL_USE_LATENCY,
                Topics.MESSAGE_OUTPUT
        ));

        if (MEMOIR_ENABLED) {
            requiredTopics.addAll(List.of(
                    Topics.SESSION_END,
                    Topics.MEMOIR_CONTEXT,
                    Topics.MEMOIR_CONTEXT_SESSION_END
            ));
        }

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(30, TimeUnit.SECONDS);

            List<NewTopic> toCreate = requiredTopics.stream()
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, 1, (short) 1))
                    .collect(Collectors.toList());

            if (!toCreate.isEmpty()) {
                log.info("Creating {} missing topics: {}",
                        toCreate.size(),
                        toCreate.stream().map(NewTopic::name).toList());
                var results = admin.createTopics(toCreate).values();
                for (var entry : results.entrySet()) {
                    try {
                        entry.getValue().get(30, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.ExecutionException e) {
                        if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                            log.debug("Topic {} already exists — skipping", entry.getKey());
                        } else {
                            throw e;
                        }
                    }
                }
            }

            log.info("All {} required topics verified", requiredTopics.size());
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure required topics exist", e);
        }
    }

    public static Properties buildConfig() {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            bootstrapServers = "localhost:9092";
        }

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, Topics.AGENT_NAME + "-streams");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
              org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
              org.apache.kafka.common.serialization.Serdes.StringSerde.class);
        // At-least-once delivery; change to EXACTLY_ONCE_V2 for production
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        // Reduce latency: flush cache and commit more frequently
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
        return p;
    }
}
