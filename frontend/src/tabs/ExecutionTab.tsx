import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import type { PipelineEvent } from "../types";
import { JsonView, collapseAllNested, darkStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";
import "./ExecutionTab.css";

interface Props {
  pipelineEvents: PipelineEvent[];
}

// ── Row types ──

type RowCategory = "Client Input" | "Message Input" | "Think" | "Tools Execution" | "Client Output";

interface SubRow {
  label: string;
  comment: string;
  timestamp: string;
  event: PipelineEvent;
}

interface TableRow {
  category: RowCategory;
  comment: string;
  timestamp: string;
  event: PipelineEvent;
  subRows?: SubRow[];
  dividerAfter?: boolean;
}

// ── Topic matching ──

// Order matters: longer/more-specific suffixes first so that e.g.
// "agent-enriched-message-input" matches "enriched-message-input"
// before "message-input".
const BASE_TOPICS = [
  "enriched-message-input",
  "message-input",
  "session-context",
  "think-request-response",
  "tool-use-all-complete",
  "tool-use-result",
  "tool-use",
  "message-output",
] as const;

function baseTopic(topic: string): string {
  for (const base of BASE_TOPICS) {
    if (topic === base || topic.endsWith("-" + base)) return base;
  }
  return topic;
}

// ── Helpers for message-input comment ──

function messageInputComment(v: Record<string, unknown> | undefined): string {
  if (!v) return "Input received";
  const role = String(v.role ?? "");
  if (role === "tool") {
    const metadata = v.metadata as Record<string, unknown> | undefined;
    const toolCount = metadata?.tool_count != null ? Number(metadata.tool_count) : null;
    const content = v.content;
    if (toolCount != null) {
      return `Tool use results (${toolCount} tool${toolCount !== 1 ? "s" : ""})`;
    }
    if (Array.isArray(content)) {
      return `Tool use results (${content.length} tool${content.length !== 1 ? "s" : ""})`;
    }
    return "Tool use results";
  }
  return "User input";
}

function enrichedInputComment(v: Record<string, unknown> | undefined): string {
  if (!v) return "Enriched context";
  const historyCount = Array.isArray(v.history) ? v.history.length : 0;
  const hasMemoir = Boolean(v.memoir_context);
  const parts: string[] = [];
  parts.push(`${historyCount} history item${historyCount !== 1 ? "s" : ""}`);
  if (hasMemoir) parts.push("memoir attached");
  return parts.join(", ");
}

// ── Build table rows from pipeline events ──

function buildRows(events: PipelineEvent[]): TableRow[] {
  const rows: TableRow[] = [];

  // We accumulate sub-rows for the current "Message Input" group
  let currentMsgRow: TableRow | null = null;

  // We accumulate tool sub-rows for the current "Tools Execution" group
  let currentToolRow: TableRow | null = null;

  // Track the last enriched-message-input event to use as Think "request"
  let lastEnrichedEvent: PipelineEvent | null = null;

  for (const evt of events) {
    const v = evt.value;
    const topic = baseTopic(evt.topic);

    switch (topic) {
      case "message-input": {
        // If we already have a pending Message Input row (e.g. duplicate
        // event from Kafka), skip it entirely.
        if (currentMsgRow) break;

        // Flush any pending tools
        flushToolRow();

        const role = v ? String(v.role ?? "") : "";
        const isUser = role !== "tool";
        const comment = messageInputComment(v);
        currentMsgRow = {
          category: isUser ? "Client Input" : "Message Input",
          comment,
          timestamp: evt.timestamp,
          event: evt,
          subRows: [
            {
              label: "message-input",
              comment,
              timestamp: evt.timestamp,
              event: evt,
            },
          ],
        };
        break;
      }

      case "enriched-message-input": {
        lastEnrichedEvent = evt;
        const comment = enrichedInputComment(v);
        if (currentMsgRow) {
          currentMsgRow.subRows!.push({
            label: "enriched-message-input",
            comment,
            timestamp: evt.timestamp,
            event: evt,
          });
        }
        break;
      }

      case "session-context": {
        // Attach cost info to the enriched sub-row if available
        if (currentMsgRow && v) {
          const cost = v.cost != null ? Number(v.cost) : null;
          const llmCalls = v.llm_calls != null ? Number(v.llm_calls) : null;
          if (cost != null) {
            // Update the enriched sub-row comment to include cost
            const enrichedSub = currentMsgRow.subRows!.find(
              (s) => s.label === "enriched-message-input"
            );
            if (enrichedSub) {
              const costStr = cost < 0.01 ? `$${cost.toFixed(6)}` : `$${cost.toFixed(4)}`;
              const parts = [enrichedSub.comment];
              parts.push(`cost: ${costStr}`);
              if (llmCalls != null) parts.push(`${llmCalls} LLM call${llmCalls !== 1 ? "s" : ""}`);
              enrichedSub.comment = parts.join(", ");
            }
          }
        }
        break;
      }

      case "think-request-response": {
        flushToolRow();
        flushMsgRow();

        const endTurn = Boolean(v?.end_turn);
        const toolUses = Array.isArray(v?.tool_uses) ? v.tool_uses : [];

        let comment: string;
        if (endTurn) {
          comment = "Respond to user";
        } else {
          const toolNames = toolUses.map(
            (tu) => String((tu as Record<string, unknown>).name ?? "unknown")
          );
          const count = toolNames.length;
          const names = toolNames.join(", ");
          comment = `Looking up ${count} source${count !== 1 ? "s" : ""} from ${names}`;
        }

        const inputTokens = v?.input_tokens != null ? Number(v.input_tokens) : null;
        const outputTokens = v?.output_tokens != null ? Number(v.output_tokens) : null;
        if (inputTokens != null || outputTokens != null) {
          const parts: string[] = [];
          if (inputTokens != null) parts.push(`in: ${inputTokens}`);
          if (outputTokens != null) parts.push(`out: ${outputTokens}`);
          comment += ` (${parts.join(", ")})`;
        }

        const thinkSubRows: SubRow[] = [];

        // Request sub-row: the enriched input that was sent to the LLM
        if (lastEnrichedEvent) {
          thinkSubRows.push({
            label: "request",
            comment: "Input sent to LLM",
            timestamp: lastEnrichedEvent.timestamp,
            event: lastEnrichedEvent,
          });
        }

        // Response sub-row: the LLM output
        thinkSubRows.push({
          label: "response",
          comment,
          timestamp: evt.timestamp,
          event: evt,
        });

        rows.push({
          category: "Think",
          comment,
          timestamp: evt.timestamp,
          event: evt,
          subRows: thinkSubRows,
        });

        lastEnrichedEvent = null;
        break;
      }

      case "tool-use": {
        flushMsgRow();
        const name = String(v?.name ?? "unknown");

        if (!currentToolRow) {
          currentToolRow = {
            category: "Tools Execution",
            comment: "",
            timestamp: evt.timestamp,
            event: evt,
            subRows: [],
          };
        }

        currentToolRow.subRows!.push({
          label: name,
          comment: `Executing ${name}`,
          timestamp: evt.timestamp,
          event: evt,
        });
        break;
      }

      case "tool-use-result": {
        if (currentToolRow) {
          const name = String(v?.name ?? "unknown");
          const status = String(v?.status ?? "");
          const latency = evt.latencyMs ?? (v?.latency_ms != null ? Number(v.latency_ms) : null);
          const sub = currentToolRow.subRows!.find(
            (s) => s.label === name && !s.comment.includes("→")
          );
          if (sub) {
            const parts = [name, "→", status];
            if (latency != null) parts.push(`(${latency}ms)`);
            sub.comment = parts.join(" ");
          }
        }
        break;
      }

      case "tool-use-all-complete": {
        flushMsgRow();

        const resultCount = Array.isArray(v?.results) ? v.results.length : 0;

        if (currentToolRow) {
          // Add collect results as the last sub-row
          currentToolRow.subRows!.push({
            label: "collect-results",
            comment: `${resultCount} result${resultCount !== 1 ? "s" : ""} collected`,
            timestamp: evt.timestamp,
            event: evt,
          });
          // Update the parent row comment
          const toolCount = currentToolRow.subRows!.length - 1; // exclude collect-results
          const toolNames = currentToolRow.subRows!
            .filter((s) => s.label !== "collect-results")
            .map((s) => s.label)
            .join(", ");
          currentToolRow.comment = `${toolCount} tool${toolCount !== 1 ? "s" : ""} executing (${toolNames})`;
          currentToolRow.timestamp = evt.timestamp;
          flushToolRow();
        } else {
          // tool-use-all-complete without prior tool-use events
          rows.push({
            category: "Tools Execution",
            comment: `${resultCount} result${resultCount !== 1 ? "s" : ""} collected`,
            timestamp: evt.timestamp,
            event: evt,
          });
        }
        break;
      }

      case "message-output": {
        flushToolRow();
        flushMsgRow();

        const content = v ? String(v.content ?? "") : "";
        const outputComment = truncate(content, 140);
        rows.push({
          category: "Client Output",
          comment: outputComment,
          timestamp: evt.timestamp,
          event: evt,
          subRows: [
            {
              label: "message-output",
              comment: outputComment,
              timestamp: evt.timestamp,
              event: evt,
            },
          ],
          dividerAfter: true,
        });
        break;
      }
    }
  }

  flushToolRow();
  flushMsgRow();

  return rows;

  function flushToolRow() {
    if (currentToolRow) {
      // If flushing without a collect-results (e.g. still in progress),
      // set comment from accumulated tools
      if (!currentToolRow.comment) {
        const toolCount = currentToolRow.subRows!.length;
        const toolNames = currentToolRow.subRows!.map((s) => s.label).join(", ");
        currentToolRow.comment = `${toolCount} tool${toolCount !== 1 ? "s" : ""} executing (${toolNames})`;
      }
      rows.push(currentToolRow);
      currentToolRow = null;
    }
  }

  function flushMsgRow() {
    if (currentMsgRow) {
      rows.push(currentMsgRow);
      currentMsgRow = null;
    }
  }
}

function truncate(s: string, max: number) {
  return s.length <= max ? s : s.slice(0, max) + "...";
}

function formatTimestamp(iso: string) {
  const d = new Date(iso);
  return d.toLocaleTimeString([], {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  } as Intl.DateTimeFormatOptions);
}

/** Check if a tool sub-row has a failed status */
function isFailedSubRow(comment: string): boolean {
  return comment.includes("→ error");
}

// ── Main ──

export function ExecutionTab({ pipelineEvents }: Props) {
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [expandedSubKey, setExpandedSubKey] = useState<string | null>(null);
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

  const currentEvents = selectedSession
    ? (sessions.get(selectedSession) ?? [])
    : [];

  const rows = useMemo(() => buildRows(currentEvents), [currentEvents]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [rows.length]);

  const handleSelectSession = useCallback((sid: string) => {
    setSelectedSession(sid);
    setExpandedIdx(null);
    setExpandedSubKey(null);
  }, []);

  const toggleExpand = useCallback((idx: number) => {
    setExpandedIdx((prev) => (prev === idx ? null : idx));
    setExpandedSubKey(null);
  }, []);

  const toggleSubRow = useCallback((key: string) => {
    setExpandedSubKey((prev) => (prev === key ? null : key));
  }, []);

  return (
    <div className="exec-tab">
      {/* Sidebar */}
      <div className="exec-sidebar">
        <div className="exec-sidebar-header">Sessions</div>
        {sessionIds.length === 0 && (
          <div className="exec-sidebar-empty">No pipeline events yet.</div>
        )}
        {sessionIds.map((sid) => {
          const events = sessions.get(sid)!;
          const firstInput = events.find(
            (e) => baseTopic(e.topic) === "message-input"
          );
          const preview = firstInput?.value
            ? truncate(
                String(
                  (firstInput.value as Record<string, unknown>).content ?? ""
                ),
                40
              )
            : `${events.length} events`;
          return (
            <button
              key={sid}
              className={`exec-sidebar-item ${selectedSession === sid ? "selected" : ""}`}
              onClick={() => handleSelectSession(sid)}
            >
              <div className="exec-sidebar-id">{sid}</div>
              <div className="exec-sidebar-msg">{preview}</div>
              <div className="exec-sidebar-time">{events.length} events</div>
            </button>
          );
        })}
      </div>

      {/* Main: table */}
      <div className="exec-main">
        {!selectedSession ? (
          <div className="exec-main-empty">
            Select a session to view execution flow.
          </div>
        ) : rows.length === 0 ? (
          <div className="exec-main-empty">
            No execution steps yet for this session.
          </div>
        ) : (
          <div className="et-table-wrap">
            <table className="et-table">
              <thead>
                <tr>
                  <th className="et-th et-th-category">Step</th>
                  <th className="et-th et-th-comment">Comment</th>
                  <th className="et-th et-th-time">Timestamp</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, idx) => {
                  const isExpanded = expandedIdx === idx;
                  const hasSubRows =
                    row.subRows && row.subRows.length > 0;

                  return (
                    <>
                      <tr
                        key={idx}
                        className={`et-row ${isExpanded ? "expanded" : ""} ${hasSubRows ? "expandable" : ""}`}
                        onClick={() => toggleExpand(idx)}
                      >
                        <td className="et-td et-td-category">
                          <span className="et-category-label">
                            {hasSubRows && (
                              <span
                                className={`et-chevron ${isExpanded ? "open" : ""}`}
                              >
                                &#9654;
                              </span>
                            )}
                            {row.category}
                          </span>
                        </td>
                        <td className="et-td et-td-comment">
                          <div className="et-comment-text">{row.comment}</div>
                        </td>
                        <td className="et-td et-td-time">
                          {formatTimestamp(row.timestamp)}
                        </td>
                      </tr>
                      {isExpanded &&
                        hasSubRows &&
                        row.subRows!.map((sub, si) => {
                          const failed = isFailedSubRow(sub.comment);
                          const subKey = `${idx}-sub-${si}`;
                          const isSubExpanded = expandedSubKey === subKey;
                          return (
                            <>
                              <tr
                                key={subKey}
                                className={`et-row et-sub-row et-sub-clickable ${failed ? "et-failed" : ""}`}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  toggleSubRow(subKey);
                                }}
                              >
                                <td className="et-td et-td-category">
                                  <span className="et-sub-label">
                                    {sub.label}
                                  </span>
                                </td>
                                <td className="et-td et-td-comment">
                                  <div className="et-comment-text">
                                    {sub.comment}
                                  </div>
                                </td>
                                <td className="et-td et-td-time">
                                  {formatTimestamp(sub.timestamp)}
                                </td>
                              </tr>
                              {isSubExpanded && sub.event.value && (
                                <tr
                                  key={`${subKey}-detail`}
                                  className="et-row et-sub-row et-sub-detail-row"
                                >
                                  <td
                                    className="et-td"
                                    colSpan={3}
                                    style={{ paddingLeft: 40 }}
                                  >
                                    <div className="et-json-tree">
                                      <JsonView
                                        data={sub.event.value}
                                        shouldExpandNode={collapseAllNested}
                                        style={darkStyles}
                                      />
                                    </div>
                                  </td>
                                </tr>
                              )}
                            </>
                          );
                        })}
                      {row.dividerAfter && (
                        <tr className="et-spacer-row">
                          <td colSpan={3} />
                        </tr>
                      )}
                    </>
                  );
                })}
              </tbody>
            </table>
            <div ref={bottomRef} />
          </div>
        )}
      </div>
    </div>
  );
}
