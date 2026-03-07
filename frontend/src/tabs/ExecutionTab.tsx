import { useState } from "react";
import type { ExecutionRequest } from "../types";
import "./ExecutionTab.css";

interface Props {
  executions: ExecutionRequest[];
}

const STATUS_CLASS: Record<string, string> = {
  pending: "status-pending",
  processing: "status-processing",
  completed: "status-completed",
  failed: "status-failed",
  running: "status-processing",
};

export function ExecutionTab({ executions }: Props) {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected = executions.find((e) => e.id === selectedId);

  return (
    <div className="exec-tab">
      <div className="exec-list">
        <div className="exec-list-header">Requests ({executions.length})</div>
        {executions.length === 0 && (
          <div className="exec-empty">No requests yet. Send a message in the Chat tab.</div>
        )}
        {[...executions].reverse().map((exec) => (
          <button
            key={exec.id}
            className={`exec-item ${selectedId === exec.id ? "selected" : ""}`}
            onClick={() => setSelectedId(exec.id)}
          >
            <div className="exec-item-top">
              <span className={`exec-status ${STATUS_CLASS[exec.status]}`}>
                {exec.status}
              </span>
              <span className="exec-item-time">
                {new Date(exec.createdAt).toLocaleTimeString()}
              </span>
            </div>
            <div className="exec-item-msg">{exec.userMessage}</div>
            <div className="exec-item-tools">
              {exec.toolCalls.length} tool call{exec.toolCalls.length !== 1 && "s"}
            </div>
          </button>
        ))}
      </div>

      <div className="exec-detail">
        {!selected ? (
          <div className="exec-empty">Select a request to view details.</div>
        ) : (
          <>
            <div className="exec-detail-header">
              <h3>Request Detail</h3>
              <span className={`exec-status ${STATUS_CLASS[selected.status]}`}>
                {selected.status}
              </span>
            </div>

            <div className="exec-section">
              <div className="exec-section-label">User Message</div>
              <div className="exec-section-content">{selected.userMessage}</div>
            </div>

            {selected.response && (
              <div className="exec-section">
                <div className="exec-section-label">Agent Response</div>
                <div className="exec-section-content">{selected.response}</div>
              </div>
            )}

            <div className="exec-section">
              <div className="exec-section-label">
                Tool Calls ({selected.toolCalls.length})
              </div>
              {selected.toolCalls.length === 0 && (
                <div className="exec-section-content muted">No tool calls</div>
              )}
              {selected.toolCalls.map((tc) => (
                <div key={tc.id} className="tool-call-card">
                  <div className="tool-call-header">
                    <span className="tool-call-name">{tc.name}</span>
                    <span className={`exec-status small ${STATUS_CLASS[tc.status]}`}>
                      {tc.status}
                    </span>
                  </div>
                  <div className="tool-call-body">
                    <div className="tool-call-label">Input</div>
                    <pre className="tool-call-pre">
                      {JSON.stringify(tc.input, null, 2)}
                    </pre>
                    {tc.result && (
                      <>
                        <div className="tool-call-label">Result</div>
                        <pre className="tool-call-pre">{tc.result}</pre>
                      </>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
