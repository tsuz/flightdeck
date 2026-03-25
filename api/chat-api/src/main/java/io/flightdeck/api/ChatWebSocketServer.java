package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket server that bridges Kafka message-output to frontend clients.
 *
 * Clients connect to ws://host:port/ws.
 * When a UserResponse is consumed from the message-output topic, it is
 * wrapped into a {@code { type: "chat_response", data: ChatMessage }} envelope
 * and sent to all clients that are subscribed to that session.
 */
public class ChatWebSocketServer extends WebSocketServer {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    // sessionId → set of connected WebSocket clients watching that session
    private final Map<String, Set<WebSocket>> sessionClients = new ConcurrentHashMap<>();

    // WebSocket → sessionId (reverse lookup for cleanup on disconnect)
    private final Map<WebSocket, String> clientSessions = new ConcurrentHashMap<>();

    public ChatWebSocketServer(int port) {
        super(new InetSocketAddress(port));
        setReuseAddr(true);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        log.info("WebSocket client connected: {}", conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JsonNode msg = mapper.readTree(message);
            String type = msg.path("type").asText("");

            // Client subscribes to a session by sending: { "type": "subscribe", "session_id": "..." }
            if ("subscribe".equals(type)) {
                String sessionId = msg.path("session_id").asText("");
                if (!sessionId.isBlank()) {
                    // Remove from previous session if any
                    unsubscribe(conn);

                    sessionClients.computeIfAbsent(sessionId, k -> ConcurrentHashMap.newKeySet()).add(conn);
                    clientSessions.put(conn, sessionId);
                    log.info("Client subscribed to session: {}", sessionId);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse WebSocket message: {}", message);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        unsubscribe(conn);
        log.info("WebSocket client disconnected: {}", conn.getRemoteSocketAddress());
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        log.error("WebSocket error", ex);
    }

    @Override
    public void onStart() {
        log.info("WebSocket server started on port {}", getPort());
    }

    /**
     * Called by the Kafka consumer when a UserResponse arrives on message-output.
     * Converts it to a ChatMessage envelope and sends to all clients subscribed to that session.
     */
    public void broadcastResponse(String sessionId, String userResponseJson) {
        Set<WebSocket> clients = sessionClients.get(sessionId);
        if (clients == null || clients.isEmpty()) {
            // No subscribed clients — also try broadcasting to all connected clients
            // (fallback for clients that haven't subscribed yet)
            broadcastToAll(sessionId, userResponseJson);
            return;
        }

        String envelope = buildChatResponseEnvelope(sessionId, userResponseJson);
        if (envelope == null) return;

        for (WebSocket ws : clients) {
            if (ws.isOpen()) {
                ws.send(envelope);
            }
        }
        log.info("[{}] Sent response to {} client(s)", sessionId, clients.size());
    }

    private void broadcastToAll(String sessionId, String userResponseJson) {
        String envelope = buildChatResponseEnvelope(sessionId, userResponseJson);
        if (envelope == null) return;

        int sent = 0;
        for (WebSocket ws : getConnections()) {
            if (ws.isOpen()) {
                ws.send(envelope);
                sent++;
            }
        }
        if (sent > 0) {
            log.info("[{}] Broadcast response to {} client(s) (no subscription match)", sessionId, sent);
        }
    }

    private String buildChatResponseEnvelope(String sessionId, String userResponseJson) {
        try {
            JsonNode response = mapper.readTree(userResponseJson);

            // Build ChatMessage matching frontend's expected format
            ObjectNode chatMessage = mapper.createObjectNode();
            chatMessage.put("id", UUID.randomUUID().toString());
            chatMessage.put("role", "assistant");
            chatMessage.put("content", response.path("content").asText(""));
            chatMessage.put("timestamp", response.path("timestamp").asText(""));
            if (response.has("cost") && !response.get("cost").isNull()) {
                chatMessage.put("cost", response.get("cost").asDouble());
            }

            // Wrap in WebSocket envelope
            ObjectNode envelope = mapper.createObjectNode();
            envelope.put("type", "chat_response");
            envelope.set("data", chatMessage);

            return mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            log.error("[{}] Failed to build chat response envelope", sessionId, e);
            return null;
        }
    }

    /**
     * Broadcasts a pre-built pipeline_event envelope to all connected clients.
     * Pipeline events are sent to all clients (not session-filtered) so the
     * Execution tab can show the full pipeline activity.
     */
    public void broadcastPipelineEvent(String sessionId, String envelope) {
        for (WebSocket ws : getConnections()) {
            if (ws.isOpen()) {
                ws.send(envelope);
            }
        }
    }

    private void unsubscribe(WebSocket conn) {
        String prevSession = clientSessions.remove(conn);
        if (prevSession != null) {
            Set<WebSocket> clients = sessionClients.get(prevSession);
            if (clients != null) {
                clients.remove(conn);
                if (clients.isEmpty()) {
                    sessionClients.remove(prevSession);
                }
            }
        }
    }
}
