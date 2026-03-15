import { useEffect, useRef, useState, useCallback } from "react";
import type { WsMessage, ChatMessage, Conversation, LogEntry, PipelineEvent } from "./types";

const WS_URL = `ws://${window.location.host}/ws`;
const RECONNECT_INTERVAL = 3000;

function generateId(): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

/** Generates a session ID in the format session-YYYYMMDD-HHmmss */
function generateSessionId(): string {
  const now = new Date();
  const y = now.getFullYear();
  const mo = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const h = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");
  const sec = String(now.getSeconds()).padStart(2, "0");
  return `session-${y}${mo}${d}-${h}${mi}${sec}`;
}

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [connected, setConnected] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [pipelineEvents, setPipelineEvents] = useState<PipelineEvent[]>([]);
  const [thinking, setThinking] = useState(false);
  const [sessionId, setSessionId] = useState<string>(generateSessionId);
  const sessionIdRef = useRef(sessionId);

  // Keep ref in sync so the onopen callback always has the latest sessionId
  useEffect(() => {
    sessionIdRef.current = sessionId;

    // If already connected, re-subscribe to the new session
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: "subscribe", session_id: sessionId }));
    }
  }, [sessionId]);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      ws.send(JSON.stringify({ type: "subscribe", session_id: sessionIdRef.current }));
      console.log("WebSocket connected, subscribed to", sessionIdRef.current);
    };

    ws.onmessage = (event) => {
      try {
        const msg: WsMessage = JSON.parse(event.data);
        switch (msg.type) {
          case "chat_response":
            setMessages((prev) => [...prev, msg.data]);
            setThinking(false);
            break;
          case "conversation_update":
            setConversations((prev) => {
              const idx = prev.findIndex((c) => c.id === msg.data.id);
              if (idx >= 0) {
                const updated = [...prev];
                updated[idx] = msg.data;
                return updated;
              }
              return [...prev, msg.data];
            });
            break;
          case "pipeline_event":
            setPipelineEvents((prev) => [...prev, msg.data]);
            break;
          case "log":
            setLogs((prev) => [...prev, msg.data]);
            break;
        }
      } catch {
        console.error("Failed to parse WebSocket message");
      }
    };

    ws.onclose = () => {
      setConnected(false);
      reconnectTimer.current = setTimeout(connect, RECONNECT_INTERVAL);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  const sendMessage = useCallback(async (content: string) => {
    const now = new Date().toISOString();

    const localMsg: ChatMessage = {
      id: generateId(),
      role: "user",
      content,
      timestamp: now,
    };
    setMessages((prev) => [...prev, localMsg]);
    setThinking(true);

    const payload = {
      session_id: sessionId,
      content,
    };

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        console.error("Failed to send message:", res.status, await res.text());
        setThinking(false);
      }
    } catch (err) {
      console.error("Failed to send message:", err);
      setThinking(false);
    }
  }, [sessionId]);

  const newChat = useCallback(() => {
    setSessionId(generateSessionId());
    setMessages([]);
    setThinking(false);
  }, []);

  return {
    connected, messages, conversations, logs, pipelineEvents,
    thinking, sessionId, sendMessage, newChat,
  };
}
