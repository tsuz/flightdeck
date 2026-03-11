import { useEffect, useRef, useState, useCallback } from "react";
import type { WsMessage, ChatMessage, Conversation, LogEntry, PipelineStep, ToolUseBlock, StepCost } from "./types";

const WS_URL = `ws://${window.location.host}/ws`;
const RECONNECT_INTERVAL = 3000;

function generateId(): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function shortId(): string {
  return generateId().split("-")[0];
}

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [connected, setConnected] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [thinking, setThinking] = useState(false);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      console.log("WebSocket connected");
    };

    ws.onmessage = (event) => {
      try {
        const msg: WsMessage = JSON.parse(event.data);
        switch (msg.type) {
          case "chat_response":
            setMessages((prev) => [...prev, msg.data]);
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

  const sendMessage = useCallback((content: string) => {
    const now = new Date().toISOString();
    const conversationId = shortId();

    const msg: ChatMessage = {
      id: generateId(),
      role: "user",
      content,
      timestamp: now,
    };
    setMessages((prev) => [...prev, msg]);

    // Pricing: Sonnet input $3/MTok, output $15/MTok
    const INPUT_PRICE = 3 / 1_000_000;
    const OUTPUT_PRICE = 15 / 1_000_000;

    function makeCost(inp: number, out: number): StepCost {
      return { inputTokens: inp, outputTokens: out, dollars: inp * INPUT_PRICE + out * OUTPUT_PRICE };
    }

    const tools = [
      { name: "lookup_order", description: "Look up an order by ID or email" },
      { name: "process_refund", description: "Process a refund for an order" },
      { name: "check_policy", description: "Check refund/return policy" },
    ];

    const steps: PipelineStep[] = [];
    let t = Date.now();

    // ── Step 1: User Request ──
    steps.push({
      id: generateId(),
      type: "user_request",
      timestamp: now,
      status: "completed",
      latencyMs: 12,
      payload: { topic: "user-request", content },
    });
    t += 12;

    // ── Step 2: LLM Query (round 1) ──
    const round1Messages = [{ role: "user", content }];
    steps.push({
      id: generateId(),
      type: "llm_query",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 45,
      payload: {
        model: "claude-sonnet-4-20250514",
        system: "You are a helpful customer support agent.",
        messages: round1Messages,
        tools,
      },
    });
    t += 45;

    // ── Step 3: LLM Response (round 1) → tool_use ──
    const round1ToolBlocks: ToolUseBlock[] = [
      { id: `toolu_${shortId()}`, name: "check_policy", input: { policy_type: "refund", query: content } },
      { id: `toolu_${shortId()}`, name: "lookup_order", input: { search_query: content, lookup_type: "recent" } },
    ];
    const round1InTok = 342;
    const round1OutTok = 128;
    steps.push({
      id: generateId(),
      type: "llm_response",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 1380,
      cost: makeCost(round1InTok, round1OutTok),
      payload: {
        id: `msg_${shortId()}`,
        model: "claude-sonnet-4-20250514",
        role: "assistant",
        stop_reason: "tool_use",
        content: [
          { type: "text", text: "I'll help you with your refund request. Let me look into this." },
          ...round1ToolBlocks.map((tb) => ({ type: "tool_use", id: tb.id, name: tb.name, input: tb.input })),
        ],
        usage: { input_tokens: round1InTok, output_tokens: round1OutTok },
      },
    });
    t += 1380;

    // ── Step 4–5: Tool Executions (round 1) with request & response ──
    const round1ToolResults: { toolUseId: string; name: string; result: unknown }[] = [];

    const toolResponses: Record<string, unknown> = {
      check_policy: {
        eligible: true,
        policy: "Full refund within 30 days of purchase. Partial refund within 60 days.",
        conditions: ["Item must be unused", "Original receipt required"],
      },
      lookup_order: {
        order_id: "ORD-20260215-7842",
        status: "delivered",
        items: [{ name: "Wireless Headphones", price: 89.99, qty: 1 }],
        delivered_at: "2026-02-20T14:30:00Z",
        total: 89.99,
      },
    };

    for (const tb of round1ToolBlocks) {
      const response = toolResponses[tb.name] ?? { status: "ok" };

      if (tb.name === "lookup_order") {
        // First attempt fails
        const failedId = generateId();
        steps.push({
          id: failedId,
          type: "tool_execution",
          timestamp: new Date(t).toISOString(),
          status: "failed",
          latencyMs: 540,
          payload: {
            topic: "tool-use",
            toolUseId: tb.id,
            name: tb.name,
            request: tb.input,
            response: {
              error: "ECONNRESET",
              message: "Connection to order service timed out after 500ms",
            },
          },
        });
        t += 540;

        // Retry succeeds
        steps.push({
          id: generateId(),
          type: "tool_execution",
          timestamp: new Date(t).toISOString(),
          status: "completed",
          latencyMs: 380,
          retryOf: failedId,
          payload: {
            topic: "tool-use",
            toolUseId: tb.id,
            name: tb.name,
            request: tb.input,
            response,
          },
        });
        t += 380;
      } else {
        const latency = 320;
        steps.push({
          id: generateId(),
          type: "tool_execution",
          timestamp: new Date(t).toISOString(),
          status: "completed",
          latencyMs: latency,
          payload: {
            topic: "tool-use",
            toolUseId: tb.id,
            name: tb.name,
            request: tb.input,
            response,
          },
        });
        t += latency;
      }

      round1ToolResults.push({ toolUseId: tb.id, name: tb.name, result: response });
    }

    // ── Step 6: LLM Query (round 2) — includes tool results ──
    const round2Messages = [
      ...round1Messages,
      {
        role: "assistant",
        content: [
          { type: "text", text: "I'll help you with your refund request. Let me look into this." },
          ...round1ToolBlocks.map((tb) => ({ type: "tool_use", id: tb.id, name: tb.name, input: tb.input })),
        ],
      },
      ...round1ToolResults.map((tr) => ({
        role: "user",
        content: [{ type: "tool_result", tool_use_id: tr.toolUseId, content: JSON.stringify(tr.result) }],
      })),
    ];
    steps.push({
      id: generateId(),
      type: "llm_query",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 38,
      payload: {
        model: "claude-sonnet-4-20250514",
        system: "You are a helpful customer support agent.",
        messages: round2Messages,
        tools,
      },
    });
    t += 38;

    // ── Step 7: LLM Response (round 2) → another tool_use (process_refund) ──
    const round2ToolBlocks: ToolUseBlock[] = [
      { id: `toolu_${shortId()}`, name: "process_refund", input: { order_id: "ORD-20260215-7842", amount: 89.99, reason: "customer_request" } },
    ];
    const round2InTok = 891;
    const round2OutTok = 96;
    steps.push({
      id: generateId(),
      type: "llm_response",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 1120,
      cost: makeCost(round2InTok, round2OutTok),
      payload: {
        id: `msg_${shortId()}`,
        model: "claude-sonnet-4-20250514",
        role: "assistant",
        stop_reason: "tool_use",
        content: [
          { type: "text", text: "Your order ORD-20260215-7842 is eligible for a full refund. Let me process that now." },
          ...round2ToolBlocks.map((tb) => ({ type: "tool_use", id: tb.id, name: tb.name, input: tb.input })),
        ],
        usage: { input_tokens: round2InTok, output_tokens: round2OutTok },
      },
    });
    t += 1120;

    // ── Step 8: Tool Execution (round 2) ──
    const round2ToolResults: { toolUseId: string; name: string; result: unknown }[] = [];
    for (const tb of round2ToolBlocks) {
      const response = {
        refund_id: "REF-20260307-1234",
        status: "processed",
        amount: 89.99,
        estimated_arrival: "3-5 business days",
      };
      steps.push({
        id: generateId(),
        type: "tool_execution",
        timestamp: new Date(t).toISOString(),
        status: "completed",
        latencyMs: 890,
        payload: {
          topic: "tool-use",
          toolUseId: tb.id,
          name: tb.name,
          request: tb.input,
          response,
        },
      });
      round2ToolResults.push({ toolUseId: tb.id, name: tb.name, result: response });
      t += 890;
    }

    // ── Step 9: LLM Query (round 3) — includes all tool results ──
    const round3Messages = [
      ...round2Messages,
      {
        role: "assistant",
        content: [
          { type: "text", text: "Your order ORD-20260215-7842 is eligible for a full refund. Let me process that now." },
          ...round2ToolBlocks.map((tb) => ({ type: "tool_use", id: tb.id, name: tb.name, input: tb.input })),
        ],
      },
      ...round2ToolResults.map((tr) => ({
        role: "user",
        content: [{ type: "tool_result", tool_use_id: tr.toolUseId, content: JSON.stringify(tr.result) }],
      })),
    ];
    steps.push({
      id: generateId(),
      type: "llm_query",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 41,
      payload: {
        model: "claude-sonnet-4-20250514",
        system: "You are a helpful customer support agent.",
        messages: round3Messages,
        tools,
      },
    });
    t += 41;

    // ── Step 10: LLM Response (round 3) → end_turn (final answer) ──
    const round3InTok = 1247;
    const round3OutTok = 156;
    const finalText = "Your refund of $89.99 for order ORD-20260215-7842 (Wireless Headphones) has been processed successfully. The refund ID is REF-20260307-1234 and you should see the funds back in your account within 3-5 business days. Is there anything else I can help you with?";
    steps.push({
      id: generateId(),
      type: "llm_response",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 1650,
      cost: makeCost(round3InTok, round3OutTok),
      payload: {
        id: `msg_${shortId()}`,
        model: "claude-sonnet-4-20250514",
        role: "assistant",
        stop_reason: "end_turn",
        content: [{ type: "text", text: finalText }],
        usage: { input_tokens: round3InTok, output_tokens: round3OutTok },
      },
    });
    t += 1650;

    // ── Step 11: User Response ──
    steps.push({
      id: generateId(),
      type: "user_response",
      timestamp: new Date(t).toISOString(),
      status: "completed",
      latencyMs: 8,
      payload: {
        topic: "user-response",
        content: finalText,
      },
    });

    const conversation: Conversation = {
      id: conversationId,
      userMessage: content,
      createdAt: now,
      maxCostDollars: 1.0,
      steps,
    };

    setConversations((prev) => [...prev, conversation]);
    setThinking(true);

    // Simulate processing delay, then add assistant response
    setTimeout(() => {
      const assistantMsg: ChatMessage = {
        id: generateId(),
        role: "assistant",
        content: finalText,
        timestamp: new Date().toISOString(),
      };
      setMessages((prev) => [...prev, assistantMsg]);
      setThinking(false);
    }, 3000);

    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: "chat_message", data: msg }));
    }
  }, []);

  return { connected, messages, conversations, logs, thinking, sendMessage };
}
