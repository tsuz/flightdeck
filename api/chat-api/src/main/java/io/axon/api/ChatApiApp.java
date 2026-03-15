package io.axon.api;

import com.sun.net.httpserver.HttpServer;
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
        // 1. Kafka producer (chat → message-input)
        KafkaMessageProducer producer = new KafkaMessageProducer();

        // 2. HTTP server for REST API
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(PORT), 0);
        httpServer.createContext("/api/chat", new ChatHandler(producer));
        httpServer.setExecutor(null);
        httpServer.start();
        log.info("HTTP server started on port {}", PORT);

        // 3. WebSocket server (message-output → browser)
        ChatWebSocketServer wsServer = new ChatWebSocketServer(WS_PORT);
        wsServer.start();

        // 4. Kafka consumer (message-output → WebSocket broadcast)
        OutputConsumer outputConsumer = new OutputConsumer(wsServer);
        Thread consumerThread = new Thread(outputConsumer, "output-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            outputConsumer.stop();
            try { wsServer.stop(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            httpServer.stop(2);
            producer.close();
        }));

        log.info("Chat API ready — HTTP={} WS={}", PORT, WS_PORT);
    }

    static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
