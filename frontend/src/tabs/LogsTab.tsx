import { useRef, useEffect, useState } from "react";
import type { LogEntry } from "../types";
import "./LogsTab.css";

interface Props {
  logs: LogEntry[];
}

const LEVEL_OPTIONS = ["all", "debug", "info", "warn", "error"] as const;

export function LogsTab({ logs }: Props) {
  const [levelFilter, setLevelFilter] = useState<string>("all");
  const [sourceFilter, setSourceFilter] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  const filtered = logs.filter((log) => {
    if (levelFilter !== "all" && log.level !== levelFilter) return false;
    if (sourceFilter && !log.source.toLowerCase().includes(sourceFilter.toLowerCase()))
      return false;
    return true;
  });

  useEffect(() => {
    if (autoScroll) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [filtered.length, autoScroll]);

  return (
    <div className="logs-tab">
      <div className="logs-toolbar">
        <div className="logs-filters">
          <select
            className="logs-select"
            value={levelFilter}
            onChange={(e) => setLevelFilter(e.target.value)}
          >
            {LEVEL_OPTIONS.map((l) => (
              <option key={l} value={l}>
                {l === "all" ? "All levels" : l.toUpperCase()}
              </option>
            ))}
          </select>
          <input
            className="logs-search"
            type="text"
            placeholder="Filter by source..."
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
          />
        </div>
        <label className="logs-autoscroll">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
          />
          Auto-scroll
        </label>
      </div>

      <div className="logs-list">
        {filtered.length === 0 && (
          <div className="logs-empty">No logs to display.</div>
        )}
        {filtered.map((log) => (
          <div key={log.id} className={`log-row level-${log.level}`}>
            <span className="log-time">
              {new Date(log.timestamp).toLocaleTimeString()}
            </span>
            <span className={`log-level level-${log.level}`}>{log.level.toUpperCase()}</span>
            <span className="log-source">{log.source}</span>
            <span className="log-msg">{log.message}</span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
