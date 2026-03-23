import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import type { PipelineEvent } from "../types";
import "./PipelineEventsTab.css";

interface Props {
  pipelineEvents: PipelineEvent[];
}

// ── Topic display config ──

const TOPIC_LABELS: Record<string, string> = {
  "message-input": "Message Input",
  "session-context": "Session Context",
  "enriched-message-input": "Enriched Input",
  "think-request-response": "LLM Response",
  "tool-use": "Tool Dispatch",
  "tool-use-dlq": "Tool DLQ",
  "tool-use-result": "Tool Result",
  "tool-use-all-complete": "Tools Complete",
  "tool-use-latency": "Tool Latency",
  "session-cost": "Session Cost",
  "message-output": "Final Output",
};

const TOPIC_ICONS: Record<string, string> = {
  "message-input": "IN",
  "session-context": "CTX",
  "enriched-message-input": "ENR",
  "think-request-response": "LLM",
  "tool-use": "USE",
  "tool-use-dlq": "DLQ",
  "tool-use-result": "RES",
  "tool-use-all-complete": "ALL",
  "tool-use-latency": "LAT",
  "session-cost": "CST",
  "message-output": "OUT",
};

const TOPIC_COLORS: Record<string, string> = {
  "message-input": "#3b82f6",
  "session-context": "#8b5cf6",
  "enriched-message-input": "#6366f1",
  "think-request-response": "#f59e0b",
  "tool-use": "#ec4899",
  "tool-use-dlq": "#ef4444",
  "tool-use-result": "#10b981",
  "tool-use-all-complete": "#14b8a6",
  "tool-use-latency": "#06b6d4",
  "session-cost": "#f97316",
  "message-output": "#22c55e",
};

// Sonnet pricing
const INPUT_PRICE = 3 / 1_000_000;
const OUTPUT_PRICE = 15 / 1_000_000;

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatDollars(amount: number): string {
  if (amount < 0.01) return `$${amount.toFixed(6)}`;
  return `$${amount.toFixed(4)}`;
}

function eventSummary(event: PipelineEvent): string {
  const v = event.value;
  if (!v) return event.rawValue ?? "";

  switch (event.topic) {
    case "message-input":
      return truncate(String(v.content ?? ""), 100);
    case "think-request-response": {
      const endTurn = v.end_turn;
      const toolUses = v.tool_uses;
      const toolCount = Array.isArray(toolUses) ? toolUses.length : 0;
      if (endTurn) return "end_turn";
      return `tool_use (${toolCount} tool${toolCount !== 1 ? "s" : ""})`;
    }
    case "tool-use":
      return `${v.name ?? "unknown"}`;
    case "tool-use-result":
      return `${v.name ?? "unknown"} → ${v.status ?? ""}`;
    case "tool-use-all-complete":
      return `${v.complete ? "complete" : "pending"} (${Array.isArray(v.results) ? v.results.length : 0} results)`;
    case "message-output":
      return truncate(String(v.content ?? ""), 100);
    case "enriched-message-input":
      return `history: ${Array.isArray(v.history) ? v.history.length : 0} items`;
    case "session-context":
      return `turns: ${v.llm_calls ?? 0}, cost: ${v.cost != null ? formatDollars(Number(v.cost)) : "-"}`;
    case "session-cost":
      return `cost: ${v.total_cost != null ? formatDollars(Number(v.total_cost)) : JSON.stringify(v).slice(0, 60)}`;
    default:
      return truncate(JSON.stringify(v), 80);
  }
}

function truncate(s: string, max: number) {
  return s.length <= max ? s : s.slice(0, max) + "...";
}

// ── Components ──

function CostBadge({ cost }: { cost: NonNullable<PipelineEvent["cost"]> }) {
  const inputTok = cost.inputTokens ?? 0;
  const outputTok = cost.outputTokens ?? 0;
  const dollars = cost.dollars ?? (inputTok * INPUT_PRICE + outputTok * OUTPUT_PRICE);

  return (
    <span className="pe-cost-badge">
      <span className="pe-cost-tokens">{inputTok} in / {outputTok} out</span>
      <span className="pe-cost-dollars">{formatDollars(dollars)}</span>
    </span>
  );
}

function EventDetail({ event }: { event: PipelineEvent }) {
  const v = event.value;

  return (
    <div className="pe-event-detail">
      <div className="pe-event-meta-row">
        <span className="pe-meta-item">partition: {event.partition}</span>
        <span className="pe-meta-item">offset: {event.offset}</span>
        {event.latencyMs != null && (
          <span className="pe-meta-item">latency: {event.latencyMs}ms</span>
        )}
      </div>
      {v && (
        <pre className="pe-event-json">{JSON.stringify(v, null, 2)}</pre>
      )}
      {!v && event.rawValue && (
        <pre className="pe-event-json">{event.rawValue}</pre>
      )}
    </div>
  );
}

// ── Main ──

export function PipelineEventsTab({ pipelineEvents }: Props) {
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [expandedIdx, setExpandedIdx] = useState<Set<number>>(new Set());
  const bottomRef = useRef<HTMLDivElement>(null);

  const sessions = useMemo(() => {
    const map = new Map<string, PipelineEvent[]>();
    for (const evt of pipelineEvents) {
      const sid = evt.sessionId;
      if (!map.has(sid)) map.set(sid, []);
      map.get(sid)!.push(evt);
    }
    return map;
  }, [pipelineEvents]);

  const sessionIds = useMemo(() => [...sessions.keys()].reverse(), [sessions]);

  const currentEvents = selectedSession ? (sessions.get(selectedSession) ?? []) : [];

  const totals = useMemo(() => {
    let dollars = 0;
    let inputTokens = 0;
    let outputTokens = 0;
    for (const evt of currentEvents) {
      if (evt.cost) {
        inputTokens += evt.cost.inputTokens ?? 0;
        outputTokens += evt.cost.outputTokens ?? 0;
        dollars += evt.cost.dollars ?? 0;
      }
    }
    return { dollars, inputTokens, outputTokens };
  }, [currentEvents]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [currentEvents.length]);

  const toggleExpand = useCallback((idx: number) => {
    setExpandedIdx((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx); else next.add(idx);
      return next;
    });
  }, []);

  const handleSelectSession = useCallback((sid: string) => {
    setSelectedSession(sid);
    setExpandedIdx(new Set());
  }, []);

  return (
    <div className="pe-tab">
      {/* Sidebar: sessions */}
      <div className="pe-sidebar">
        <div className="pe-sidebar-header">Sessions</div>
        {sessionIds.length === 0 && (
          <div className="pe-sidebar-empty">No pipeline events yet.</div>
        )}
        {sessionIds.map((sid) => {
          const events = sessions.get(sid)!;
          const firstInput = events.find((e) => e.topic === "message-input");
          const preview = firstInput?.value
            ? truncate(String((firstInput.value as Record<string, unknown>).content ?? ""), 40)
            : `${events.length} events`;
          return (
            <button
              key={sid}
              className={`pe-sidebar-item ${selectedSession === sid ? "selected" : ""}`}
              onClick={() => handleSelectSession(sid)}
            >
              <div className="pe-sidebar-id">{sid}</div>
              <div className="pe-sidebar-msg">{preview}</div>
              <div className="pe-sidebar-time">{events.length} events</div>
            </button>
          );
        })}
      </div>

      {/* Main: event timeline */}
      <div className="pe-main">
        {!selectedSession ? (
          <div className="pe-main-empty">Select a session to view pipeline events.</div>
        ) : (
          <div className="pe-pipeline">
            <div className="pe-pipeline-header">
              <div className="pe-pipeline-title">
                <span className="pe-pipeline-id">{selectedSession}</span>
                <span className="pe-pipeline-count">{currentEvents.length} events</span>
              </div>
              {(totals.inputTokens > 0 || totals.outputTokens > 0) && (
                <div className="pe-pipeline-totals">
                  <div className="pe-total-item">
                    <span className="pe-total-label">Tokens</span>
                    <span className="pe-total-value">
                      {totals.inputTokens} in / {totals.outputTokens} out
                    </span>
                  </div>
                  <div className="pe-total-item">
                    <span className="pe-total-label">Total Cost</span>
                    <span className="pe-total-value cost">{formatDollars(totals.dollars)}</span>
                  </div>
                </div>
              )}
            </div>

            <div className="pe-timeline">
              {currentEvents.map((evt, idx) => {
                const isExpanded = expandedIdx.has(idx);
                const color = TOPIC_COLORS[evt.topic] ?? "#888";
                const icon = TOPIC_ICONS[evt.topic] ?? "???";
                const label = TOPIC_LABELS[evt.topic] ?? evt.topic;

                return (
                  <div key={`${evt.topic}-${evt.offset}-${idx}`} className="pe-step">
                    {/* Rail */}
                    <div className="pe-step-rail">
                      <div className="pe-step-dot" style={{ borderColor: color }}>
                        <span className="pe-step-icon" style={{ color }}>{icon}</span>
                      </div>
                      {idx < currentEvents.length - 1 && <div className="pe-step-line" />}
                    </div>

                    {/* Card */}
                    <div className={`pe-step-card ${isExpanded ? "expanded" : "collapsed"}`}>
                      <button className="pe-step-card-header" onClick={() => toggleExpand(idx)}>
                        <span className={`pe-step-chevron ${isExpanded ? "open" : ""}`}>&#9654;</span>
                        <span className="pe-topic-badge" style={{ background: color + "22", color }}>
                          {evt.topic}
                        </span>
                        <span className="pe-step-label">{label}</span>
                        {!isExpanded && (
                          <span className="pe-step-summary">{eventSummary(evt)}</span>
                        )}
                        {evt.cost && <CostBadge cost={evt.cost} />}
                        {evt.latencyMs != null && (
                          <span className="pe-latency-badge">{evt.latencyMs}ms</span>
                        )}
                        <span className="pe-step-time">
                          {formatTime(typeof evt.value?.timestamp === "string" ? evt.value.timestamp : evt.timestamp)}
                        </span>
                      </button>
                      {isExpanded && <EventDetail event={evt} />}
                    </div>
                  </div>
                );
              })}
              <div ref={bottomRef} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
