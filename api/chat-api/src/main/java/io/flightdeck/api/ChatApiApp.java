package io.flightdeck.api;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Chat API server:
 *   - HTTP  on port 8000 — POST /api/chat → produces to Kafka message-input
 *   - WebSocket on port 8001 — streams responses from Kafka message-output to clients
 *
 * Ports are configurable via PORT and WS_PORT env vars.
 */
public class ChatApiApp {

    private static final Logger log = LoggerFactory.getLogger(ChatApiApp.class);

    private static final int PORT    = Integer.parseInt(env("PORT", "8000"));
    private static final int WS_PORT = Integer.parseInt(env("WS_PORT", "8001"));

    public static void main(String[] args) throws Exception {
        // 1. One shared Kafka producer for all outbound topics. A KafkaProducer is
        //    thread-safe and routes each record to the topic on the ProducerRecord,
        //    so the wrappers (chat → message-input, async callbacks → tool-use-result,
        //    multi-agent reply routes → reply-to) all share a single set of broker
        //    connections instead of one set per topic.
        KafkaProducer<String, String> sharedProducer = KafkaProducerFactory.create();
        KafkaMessageProducer producer = new KafkaMessageProducer(sharedProducer);
        ToolResultProducer toolResultProducer = new ToolResultProducer(sharedProducer);
        ReplyToProducer replyToProducer = new ReplyToProducer(sharedProducer);

        // Shared secret for verifying async tool callback tokens. Optional —
        // if unset, /api/tools/response rejects every callback.
        String callbackSecret = env("TOOL_CALLBACK_SECRET", "");
        if (callbackSecret.isBlank()) {
            log.warn("TOOL_CALLBACK_SECRET is not set — /api/tools/response will reject all callbacks");
        }

        // 2. HTTP server for REST API
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(PORT), 0);
        httpServer.createContext("/api/chat", new ChatHandler(producer, replyToProducer));
        httpServer.createContext("/api/tools/response",
                new ToolResponseHandler(toolResultProducer, callbackSecret));
        httpServer.setExecutor(null);
        httpServer.start();
        log.info("HTTP server started on port {}", PORT);

        // 3. WebSocket server (message-output → browser)
        ChatWebSocketServer wsServer = new ChatWebSocketServer(WS_PORT);
        wsServer.start();

        // 4. Kafka consumer (message-output → WebSocket chat response, or → HTTP
        //    callback for sessions that carry a reply-to descriptor)
        OutputConsumer outputConsumer = new OutputConsumer(wsServer, replyToProducer);
        Thread outputThread = new Thread(outputConsumer, "output-consumer");
        outputThread.setDaemon(true);
        outputThread.start();

        // 5. Kafka consumer (all pipeline topics → WebSocket pipeline events)
        PipelineConsumer pipelineConsumer = new PipelineConsumer(wsServer);
        Thread pipelineThread = new Thread(pipelineConsumer, "pipeline-consumer");
        pipelineThread.setDaemon(true);
        pipelineThread.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            outputConsumer.stop();
            pipelineConsumer.stop();
            try { wsServer.stop(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            httpServer.stop(2);
            sharedProducer.close();
            log.info("Shared Kafka producer closed");
        }));

        log.info("Chat API ready — HTTP={} WS={}", PORT, WS_PORT);
    }

    static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    static String requireEnv(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Required environment variable " + key + " is not set");
        }
        return v;
    }
}
