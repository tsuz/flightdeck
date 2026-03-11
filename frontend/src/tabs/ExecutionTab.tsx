import { useState, useCallback } from "react";
import type { Conversation, PipelineStep } from "../types";
import "./ExecutionTab.css";

interface Props {
  conversations: Conversation[];
}

const STATUS_CLASS: Record<string, string> = {
  pending: "status-pending",
  processing: "status-processing",
  completed: "status-completed",
  failed: "status-failed",
  retrying: "status-retrying",
};

const STEP_LABELS: Record<string, string> = {
  user_request: "User Request",
  llm_query: "LLM Query",
  llm_response: "LLM Response",
  tool_execution: "Tool Execution",
  user_response: "User Response",
};

const STEP_ICONS: Record<string, string> = {
  user_request: "USR",
  llm_query: "LLM",
  llm_response: "RES",
  tool_execution: "EXE",
  user_response: "OUT",
};

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString();
}

function formatDollars(amount: number): string {
  if (amount < 0.01) return `$${amount.toFixed(6)}`;
  return `$${amount.toFixed(4)}`;
}

function formatLatency(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  return `${ms}ms`;
}

function computeTotals(steps: PipelineStep[]) {
  let totalLatencyMs = 0;
  let totalDollars = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;

  for (const step of steps) {
    if (step.latencyMs != null) totalLatencyMs += step.latencyMs;
    if (step.cost) {
      totalDollars += step.cost.dollars;
      totalInputTokens += step.cost.inputTokens ?? 0;
      totalOutputTokens += step.cost.outputTokens ?? 0;
    }
  }

  return { totalLatencyMs, totalDollars, totalInputTokens, totalOutputTokens };
}

// ── Display components ──

function StepMetrics({ step }: { step: PipelineStep }) {
  const parts: string[] = [];
  if (step.latencyMs != null) parts.push(formatLatency(step.latencyMs));
  if (step.cost) {
    if (step.cost.inputTokens != null || step.cost.outputTokens != null) {
      parts.push(`${step.cost.inputTokens ?? 0} in / ${step.cost.outputTokens ?? 0} out`);
    }
    parts.push(formatDollars(step.cost.dollars));
  }
  if (parts.length === 0) return null;
  return <span className="exec-step-metrics">{parts.join(" | ")}</span>;
}

function ToolUseContentBlock({ block }: { block: Record<string, unknown> }) {
  const type = block.type as string;
  if (type === "text") {
    return <div className="step-text-block">{block.text as string}</div>;
  }
  if (type === "tool_use") {
    return (
      <div className="step-tool-use-block">
        <div className="step-tool-use-header">
          <span className="step-tool-use-badge">tool_use</span>
          <span className="step-tool-use-name">{block.name as string}</span>
          <span className="step-tool-use-id">{block.id as string}</span>
        </div>
        <pre className="step-pre">{JSON.stringify(block.input, null, 2)}</pre>
      </div>
    );
  }
  return <pre className="step-pre">{JSON.stringify(block, null, 2)}</pre>;
}

function stepSummary(step: PipelineStep): string {
  const payload = step.payload as Record<string, unknown>;
  switch (step.type) {
    case "user_request":
      return payload.content as string;
    case "llm_query":
      return `${payload.model as string}`;
    case "llm_response": {
      const content = (payload.content ?? []) as Record<string, unknown>[];
      const toolCount = content.filter((b) => b.type === "tool_use").length;
      return `stop_reason: ${payload.stop_reason as string}` +
        (toolCount > 0 ? ` | ${toolCount} tool_use block${toolCount !== 1 ? "s" : ""}` : "");
    }
    case "tool_execution":
      return `${payload.name as string}`;
    case "user_response": {
      const text = payload.content as string;
      return text.length > 80 ? text.slice(0, 80) + "..." : text;
    }
    default:
      return "";
  }
}

function StepDetail({ step }: { step: PipelineStep }) {
  const payload = step.payload as Record<string, unknown>;

  if (step.type === "user_request") {
    return (
      <div className="step-detail-body">
        <div className="step-field">
          <span className="step-field-label">Kafka Topic</span>
          <span className="step-field-value mono">{payload.topic as string}</span>
        </div>
        <div className="step-field">
          <span className="step-field-label">Content</span>
          <span className="step-field-value">{payload.content as string}</span>
        </div>
        <div className="step-raw">
          <pre className="step-pre">{JSON.stringify(payload, null, 2)}</pre>
        </div>
      </div>
    );
  }

  if (step.type === "llm_query") {
    return (
      <div className="step-detail-body">
        <div className="step-field">
          <span className="step-field-label">Model</span>
          <span className="step-field-value mono">{payload.model as string}</span>
        </div>
        <div className="step-field">
          <span className="step-field-label">System Prompt</span>
          <span className="step-field-value">{payload.system as string}</span>
        </div>
        <div className="step-subsection">
          <span className="step-field-label">Messages</span>
          <pre className="step-pre">{JSON.stringify(payload.messages, null, 2)}</pre>
        </div>
        <div className="step-subsection">
          <span className="step-field-label">Tools</span>
          <pre className="step-pre">{JSON.stringify(payload.tools, null, 2)}</pre>
        </div>
      </div>
    );
  }

  if (step.type === "llm_response") {
    const content = (payload.content ?? []) as Record<string, unknown>[];
    const usage = payload.usage as Record<string, unknown> | undefined;
    return (
      <div className="step-detail-body">
        <div className="step-field-row">
          <div className="step-field">
            <span className="step-field-label">Model</span>
            <span className="step-field-value mono">{payload.model as string}</span>
          </div>
          <div className="step-field">
            <span className="step-field-label">Stop Reason</span>
            <span className="step-field-value mono">{payload.stop_reason as string}</span>
          </div>
          {usage && (
            <div className="step-field">
              <span className="step-field-label">Tokens</span>
              <span className="step-field-value mono">
                {usage.input_tokens as number} in / {usage.output_tokens as number} out
              </span>
            </div>
          )}
          {step.cost && (
            <div className="step-field">
              <span className="step-field-label">Cost</span>
              <span className="step-field-value mono">{formatDollars(step.cost.dollars)}</span>
            </div>
          )}
          {step.latencyMs != null && (
            <div className="step-field">
              <span className="step-field-label">Latency</span>
              <span className="step-field-value mono">{formatLatency(step.latencyMs)}</span>
            </div>
          )}
        </div>
        <div className="step-subsection">
          <span className="step-field-label">Content Blocks ({content.length})</span>
          <div className="step-content-blocks">
            {content.map((block, i) => (
              <ToolUseContentBlock key={i} block={block} />
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (step.type === "tool_execution") {
    return (
      <div className="step-detail-body">
        <div className="step-field-row">
          <div className="step-field">
            <span className="step-field-label">Kafka Topic</span>
            <span className="step-field-value mono">{payload.topic as string}</span>
          </div>
          <div className="step-field">
            <span className="step-field-label">Tool</span>
            <span className="step-field-value mono">{payload.name as string}</span>
          </div>
          <div className="step-field">
            <span className="step-field-label">tool_use_id</span>
            <span className="step-field-value mono">{payload.toolUseId as string}</span>
          </div>
          {step.latencyMs != null && (
            <div className="step-field">
              <span className="step-field-label">Latency</span>
              <span className="step-field-value mono">{formatLatency(step.latencyMs)}</span>
            </div>
          )}
        </div>
        <div className="step-subsection">
          <span className="step-field-label">Request</span>
          <pre className="step-pre">{JSON.stringify(payload.request, null, 2)}</pre>
        </div>
        {payload.response != null && (
          <div className="step-subsection">
            <span className="step-field-label">Response</span>
            <pre className="step-pre">{JSON.stringify(payload.response, null, 2)}</pre>
          </div>
        )}
        <div className={`step-exec-status ${STATUS_CLASS[step.status]}`}>
          {step.status === "pending" && "Waiting to execute..."}
          {step.status === "processing" && "Executing..."}
          {step.status === "completed" && "Execution complete"}
          {step.status === "failed" && "Execution failed"}
          {step.status === "retrying" && "Retrying..."}
        </div>
      </div>
    );
  }

  if (step.type === "user_response") {
    return (
      <div className="step-detail-body">
        <div className="step-field">
          <span className="step-field-label">Kafka Topic</span>
          <span className="step-field-value mono">{payload.topic as string}</span>
        </div>
        <div className="step-field">
          <span className="step-field-label">Final Response</span>
          <span className="step-field-value">{payload.content as string}</span>
        </div>
        {step.latencyMs != null && (
          <div className="step-field">
            <span className="step-field-label">Latency</span>
            <span className="step-field-value mono">{formatLatency(step.latencyMs)}</span>
          </div>
        )}
      </div>
    );
  }

  return <pre className="step-pre">{JSON.stringify(payload, null, 2)}</pre>;
}

// ── Main component ──

export function ExecutionTab({ conversations }: Props) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set());
  const selected = conversations.find((c) => c.id === selectedId);

  const toggleStep = useCallback((stepId: string) => {
    setExpandedSteps((prev) => {
      const next = new Set(prev);
      if (next.has(stepId)) {
        next.delete(stepId);
      } else {
        next.add(stepId);
      }
      return next;
    });
  }, []);

  const handleSelectConversation = useCallback((id: string) => {
    setSelectedId(id);
    setExpandedSteps(new Set());
  }, []);

  const totals = selected ? computeTotals(selected.steps) : null;

  return (
    <div className="exec-tab">
      {/* Sidebar */}
      <div className="exec-sidebar">
        <div className="exec-sidebar-header">Conversations</div>
        {conversations.length === 0 && (
          <div className="exec-sidebar-empty">No conversations yet.</div>
        )}
        {[...conversations].reverse().map((conv) => (
          <button
            key={conv.id}
            className={`exec-sidebar-item ${selectedId === conv.id ? "selected" : ""}`}
            onClick={() => handleSelectConversation(conv.id)}
          >
            <div className="exec-sidebar-id">{conv.id}</div>
            <div className="exec-sidebar-msg">{conv.userMessage}</div>
            <div className="exec-sidebar-time">{formatTime(conv.createdAt)}</div>
          </button>
        ))}
      </div>

      {/* Main content: pipeline timeline */}
      <div className="exec-main">
        {!selected ? (
          <div className="exec-main-empty">Select a conversation to view execution pipeline.</div>
        ) : (
          <div className="exec-pipeline">
            <div className="exec-pipeline-header">
              <div className="exec-pipeline-title">
                <span className="exec-pipeline-id">Conversation {selected.id}</span>
                <span className="exec-pipeline-time">{formatTime(selected.createdAt)}</span>
              </div>
              {totals && (
                <div className="exec-pipeline-totals">
                  <div className="exec-total-item">
                    <span className="exec-total-label">Total Latency</span>
                    <span className="exec-total-value">{formatLatency(totals.totalLatencyMs)}</span>
                  </div>
                  {(totals.totalInputTokens > 0 || totals.totalOutputTokens > 0) && (
                    <div className="exec-total-item">
                      <span className="exec-total-label">Tokens</span>
                      <span className="exec-total-value">
                        {totals.totalInputTokens} in / {totals.totalOutputTokens} out
                      </span>
                    </div>
                  )}
                  <div className="exec-total-item">
                    <span className="exec-total-label">Total Cost</span>
                    <span className="exec-total-value cost">{formatDollars(totals.totalDollars)}</span>
                    <div className="exec-cost-bar-wrap">
                      <div
                        className="exec-cost-bar"
                        style={{ width: `${Math.min((totals.totalDollars / selected.maxCostDollars) * 100, 100)}%` }}
                      />
                    </div>
                    <span className="exec-cost-limit">
                      of {formatDollars(selected.maxCostDollars)} max
                    </span>
                  </div>
                </div>
              )}
            </div>

            <div className="exec-timeline">
              {selected.steps.map((step, idx) => {
                const isExpanded = expandedSteps.has(step.id);
                const isRetry = !!step.retryOf;
                const isDone = step.status === "completed" || step.status === "failed";

                return (
                  <div key={step.id} className="exec-step">
                    {/* Timeline connector */}
                    <div className="exec-step-rail">
                      <div className={`exec-step-dot ${STATUS_CLASS[step.status]}`}>
                        <span className="exec-step-icon">{STEP_ICONS[step.type]}</span>
                      </div>
                      {idx < selected.steps.length - 1 && (
                        <div className="exec-step-line" />
                      )}
                    </div>

                    {/* Step card — collapsible */}
                    <div className={`exec-step-card ${isExpanded ? "expanded" : "collapsed"} ${step.status === "failed" ? "card-failed" : ""}`}>
                      <button
                        className="exec-step-card-header"
                        onClick={() => toggleStep(step.id)}
                      >
                        <span className={`exec-step-chevron ${isExpanded ? "open" : ""}`}>
                          &#9654;
                        </span>
                        <span className="exec-step-label">
                          {STEP_LABELS[step.type]}
                          {isRetry && <span className="exec-retry-badge">RETRY</span>}
                        </span>
                        <span className={`exec-step-status ${STATUS_CLASS[step.status]}`}>
                          {step.status}
                        </span>
                        {!isExpanded && (
                          <span className="exec-step-summary">{stepSummary(step)}</span>
                        )}
                        {isDone && <StepMetrics step={step} />}
                        <span className="exec-step-time">{formatTime(step.timestamp)}</span>
                      </button>
                      {isExpanded && <StepDetail step={step} />}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
