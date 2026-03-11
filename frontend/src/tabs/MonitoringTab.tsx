import { useState, useMemo, useRef, useEffect, useCallback } from "react";
import type { Conversation } from "../types";
import "./MonitoringTab.css";

interface Props {
  conversations: Conversation[];
}

type MonitoringView = "latency" | "cost";

// ── Data extraction ──

interface DataPoint {
  timestamp: number;
  value: number;
  conversationId: string;
  label: string;
}

interface CategorySeries {
  key: string;
  label: string;
  color: string;
  points: DataPoint[];
}

const CATEGORY_COLORS: Record<string, string> = {
  "end_to_end": "#6366f1",
  "llm_query": "#3b82f6",
  "llm_response": "#8b5cf6",
  "user_request": "#22c55e",
  "user_response": "#14b8a6",
};

const TOOL_COLORS = [
  "#f59e0b", "#ef4444", "#ec4899", "#f97316", "#84cc16", "#06b6d4",
];

function getToolColor(idx: number): string {
  return TOOL_COLORS[idx % TOOL_COLORS.length];
}

// Seeded pseudo-random for stable historical data
function seededRandom(seed: number): () => number {
  let s = seed;
  return () => {
    s = (s * 16807 + 0) % 2147483647;
    return (s - 1) / 2147483646;
  };
}

// Generate synthetic historical data points so the chart always shows lines
function generateHistoricalData(): {
  latency: Record<string, DataPoint[]>;
  cost: { per: DataPoint[]; cum: DataPoint[] };
} {
  const rand = seededRandom(42);
  const now = Date.now();
  const HISTORY_COUNT = 30;
  const INTERVAL = 60_000; // 1 minute apart

  const toolNames = ["check_policy", "lookup_order", "process_refund"];
  const latency: Record<string, DataPoint[]> = {
    end_to_end: [],
    llm_query: [],
    llm_response: [],
  };
  for (const name of toolNames) {
    latency[`tool:${name}`] = [];
  }

  const costPer: DataPoint[] = [];
  const costCum: DataPoint[] = [];
  let cumTotal = 0;

  for (let i = 0; i < HISTORY_COUNT; i++) {
    const ts = now - (HISTORY_COUNT - i) * INTERVAL;
    const convId = `hist-${i.toString().padStart(3, "0")}`;

    const llmQ = 30 + Math.round(rand() * 40);
    const llmR = 900 + Math.round(rand() * 800);
    const toolLatencies: Record<string, number> = {};
    for (const name of toolNames) {
      toolLatencies[name] = 200 + Math.round(rand() * 600);
    }
    // Only include 2 random tools per conversation
    const activeTools = toolNames.filter(() => rand() > 0.3);
    const totalMs = llmQ + llmR + activeTools.reduce((s, n) => s + toolLatencies[n], 0)
      + llmQ + Math.round(rand() * 400) + 20; // second round

    latency.end_to_end.push({ timestamp: ts, value: totalMs, conversationId: convId, label: "End to End" });
    latency.llm_query.push({ timestamp: ts, value: llmQ, conversationId: convId, label: "LLM Query" });
    latency.llm_response.push({ timestamp: ts, value: llmR, conversationId: convId, label: "LLM Response" });
    for (const name of activeTools) {
      latency[`tool:${name}`].push({ timestamp: ts, value: toolLatencies[name], conversationId: convId, label: name });
    }

    const cost = 0.002 + rand() * 0.004;
    cumTotal += cost;
    costPer.push({ timestamp: ts, value: cost, conversationId: convId, label: "Per Conversation" });
    costCum.push({ timestamp: ts, value: cumTotal, conversationId: convId, label: "Cumulative" });
  }

  return { latency, cost: { per: costPer, cum: costCum } };
}

const HISTORICAL = generateHistoricalData();

function extractLatencySeries(conversations: Conversation[]): CategorySeries[] {
  const toolNames = new Set<string>(["check_policy", "lookup_order", "process_refund"]);
  for (const conv of conversations) {
    for (const step of conv.steps) {
      if (step.type === "tool_execution" && !step.retryOf) {
        const p = step.payload as Record<string, unknown>;
        toolNames.add(p.name as string);
      }
    }
  }

  // Start with historical data
  const seriesMap: Record<string, DataPoint[]> = {
    end_to_end: [...HISTORICAL.latency.end_to_end],
    llm_query: [...HISTORICAL.latency.llm_query],
    llm_response: [...HISTORICAL.latency.llm_response],
  };
  for (const name of toolNames) {
    seriesMap[`tool:${name}`] = [...(HISTORICAL.latency[`tool:${name}`] ?? [])];
  }

  // Append real conversation data
  for (const conv of conversations) {
    const ts = new Date(conv.createdAt).getTime();

    let totalMs = 0;
    for (const step of conv.steps) {
      if (step.latencyMs != null) totalMs += step.latencyMs;
    }
    seriesMap.end_to_end.push({
      timestamp: ts, value: totalMs,
      conversationId: conv.id, label: "End to End",
    });

    const byType: Record<string, number[]> = {};
    for (const step of conv.steps) {
      if (step.latencyMs == null) continue;
      if (step.type === "llm_query") {
        (byType["llm_query"] ??= []).push(step.latencyMs);
      } else if (step.type === "llm_response") {
        (byType["llm_response"] ??= []).push(step.latencyMs);
      } else if (step.type === "tool_execution" && !step.retryOf) {
        const p = step.payload as Record<string, unknown>;
        const name = p.name as string;
        (byType[`tool:${name}`] ??= []).push(step.latencyMs);
      }
    }

    for (const [key, values] of Object.entries(byType)) {
      const avg = values.reduce((a, b) => a + b, 0) / values.length;
      seriesMap[key]?.push({
        timestamp: ts, value: Math.round(avg),
        conversationId: conv.id, label: key,
      });
    }
  }

  const result: CategorySeries[] = [];
  result.push({
    key: "end_to_end", label: "User Request to Response",
    color: CATEGORY_COLORS.end_to_end, points: seriesMap.end_to_end,
  });
  result.push({
    key: "llm_query", label: "LLM Query",
    color: CATEGORY_COLORS.llm_query, points: seriesMap.llm_query ?? [],
  });
  result.push({
    key: "llm_response", label: "LLM Response",
    color: CATEGORY_COLORS.llm_response, points: seriesMap.llm_response ?? [],
  });

  let toolIdx = 0;
  for (const name of [...toolNames].sort()) {
    result.push({
      key: `tool:${name}`, label: name,
      color: getToolColor(toolIdx++),
      points: seriesMap[`tool:${name}`] ?? [],
    });
  }

  return result;
}

function extractCostSeries(conversations: Conversation[]): CategorySeries[] {
  // Start with historical data
  const points: DataPoint[] = [...HISTORICAL.cost.per];
  const cumulative: DataPoint[] = [...HISTORICAL.cost.cum];
  let runningTotal = HISTORICAL.cost.cum.length > 0
    ? HISTORICAL.cost.cum[HISTORICAL.cost.cum.length - 1].value
    : 0;

  for (const conv of conversations) {
    const ts = new Date(conv.createdAt).getTime();
    let convCost = 0;
    for (const step of conv.steps) {
      if (step.cost) convCost += step.cost.dollars;
    }
    runningTotal += convCost;
    points.push({
      timestamp: ts, value: convCost,
      conversationId: conv.id, label: "Per Conversation",
    });
    cumulative.push({
      timestamp: ts, value: runningTotal,
      conversationId: conv.id, label: "Cumulative",
    });
  }

  return [
    { key: "per_conversation", label: "Per Conversation ($)", color: "#f59e0b", points },
    { key: "cumulative", label: "Cumulative ($)", color: "#ef4444", points: cumulative },
  ];
}

// ── Canvas chart ──

interface ChartProps {
  series: CategorySeries[];
  enabledKeys: Set<string>;
  yLabel: string;
  formatValue: (v: number) => string;
}

function Chart({ series, enabledKeys, yLabel, formatValue }: ChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [tooltip, setTooltip] = useState<{
    x: number; y: number; text: string;
  } | null>(null);

  // Collect all visible points
  const visibleSeries = useMemo(
    () => series.filter((s) => enabledKeys.has(s.key)),
    [series, enabledKeys]
  );

  const allPoints = useMemo(() => {
    const pts: { x: number; y: number; color: string; label: string; value: number; convId: string }[] = [];
    for (const s of visibleSeries) {
      for (const p of s.points) {
        pts.push({ x: p.timestamp, y: p.value, color: s.color, label: s.label, value: p.value, convId: p.conversationId });
      }
    }
    return pts;
  }, [visibleSeries]);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;

    const dpr = window.devicePixelRatio || 1;
    const rect = container.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width = `${rect.width}px`;
    canvas.style.height = `${rect.height}px`;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.scale(dpr, dpr);

    const w = rect.width;
    const h = rect.height;
    const pad = { top: 20, right: 20, bottom: 50, left: 70 };
    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top - pad.bottom;

    // Clear
    ctx.clearRect(0, 0, w, h);

    if (allPoints.length === 0) {
      ctx.fillStyle = "#a1a1aa";
      ctx.font = "14px -apple-system, sans-serif";
      ctx.textAlign = "center";
      ctx.fillText("No data yet. Send messages in the Chat tab.", w / 2, h / 2);
      return;
    }

    // Compute bounds
    let minX = Infinity, maxX = -Infinity, maxY = 0;
    for (const p of allPoints) {
      if (p.x < minX) minX = p.x;
      if (p.x > maxX) maxX = p.x;
      if (p.y > maxY) maxY = p.y;
    }
    if (minX === maxX) { minX -= 1000; maxX += 1000; }
    if (maxY === 0) maxY = 10;
    maxY *= 1.15; // headroom

    const scaleX = (v: number) => pad.left + ((v - minX) / (maxX - minX)) * plotW;
    const scaleY = (v: number) => pad.top + plotH - (v / maxY) * plotH;

    // Grid lines
    ctx.strokeStyle = "#2e2f3e";
    ctx.lineWidth = 1;
    const yTicks = 5;
    for (let i = 0; i <= yTicks; i++) {
      const val = (maxY / yTicks) * i;
      const y = scaleY(val);
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(w - pad.right, y);
      ctx.stroke();

      ctx.fillStyle = "#a1a1aa";
      ctx.font = "11px monospace";
      ctx.textAlign = "right";
      ctx.fillText(formatValue(val), pad.left - 8, y + 4);
    }

    // Y axis label
    ctx.save();
    ctx.translate(14, pad.top + plotH / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillStyle = "#a1a1aa";
    ctx.font = "11px -apple-system, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(yLabel, 0, 0);
    ctx.restore();

    // X axis labels
    const xTicks = Math.min(allPoints.length, 8);
    for (let i = 0; i < xTicks; i++) {
      const val = minX + ((maxX - minX) / Math.max(xTicks - 1, 1)) * i;
      const x = scaleX(val);
      const d = new Date(val);
      ctx.fillStyle = "#a1a1aa";
      ctx.font = "10px monospace";
      ctx.textAlign = "center";
      ctx.fillText(d.toLocaleTimeString(), x, h - pad.bottom + 20);
    }

    // Draw lines and dots per series
    for (const s of visibleSeries) {
      if (s.points.length === 0) continue;
      const sorted = [...s.points].sort((a, b) => a.timestamp - b.timestamp);

      // Line
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;
      ctx.lineJoin = "round";
      ctx.beginPath();
      for (let p = 0; p < sorted.length; p++) {
        const x = scaleX(sorted[p].timestamp);
        const y = scaleY(sorted[p].value);
        if (p === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      // Dots
      for (const pt of sorted) {
        const x = scaleX(pt.timestamp);
        const y = scaleY(pt.value);
        ctx.beginPath();
        ctx.arc(x, y, 4, 0, Math.PI * 2);
        ctx.fillStyle = s.color;
        ctx.fill();
        ctx.strokeStyle = "#0f1117";
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }
    }
  }, [allPoints, visibleSeries, yLabel, formatValue]);

  useEffect(() => {
    draw();
    window.addEventListener("resize", draw);
    return () => window.removeEventListener("resize", draw);
  }, [draw]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    const canvas = canvasRef.current;
    if (!canvas || allPoints.length === 0) { setTooltip(null); return; }
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;

    const w = rect.width;
    const h = rect.height;
    const pad = { top: 20, right: 20, bottom: 50, left: 70 };
    const plotW = w - pad.left - pad.right;
    const plotH = h - pad.top - pad.bottom;

    let minX = Infinity, maxX = -Infinity, maxY = 0;
    for (const p of allPoints) {
      if (p.x < minX) minX = p.x;
      if (p.x > maxX) maxX = p.x;
      if (p.y > maxY) maxY = p.y;
    }
    if (minX === maxX) { minX -= 1000; maxX += 1000; }
    if (maxY === 0) maxY = 10;
    maxY *= 1.15;

    const scaleX = (v: number) => pad.left + ((v - minX) / (maxX - minX)) * plotW;
    const scaleY = (v: number) => pad.top + plotH - (v / maxY) * plotH;

    let closest: typeof allPoints[0] | null = null;
    let closestDist = Infinity;
    for (const p of allPoints) {
      const px = scaleX(p.x);
      const py = scaleY(p.y);
      const dist = Math.hypot(mx - px, my - py);
      if (dist < closestDist && dist < 30) {
        closestDist = dist;
        closest = p;
      }
    }

    if (closest) {
      setTooltip({
        x: scaleX(closest.x),
        y: scaleY(closest.y),
        text: `${closest.label}: ${formatValue(closest.value)} (${closest.convId})`,
      });
    } else {
      setTooltip(null);
    }
  }, [allPoints, formatValue]);

  return (
    <div className="mon-chart-container" ref={containerRef}>
      <canvas
        ref={canvasRef}
        className="mon-chart-canvas"
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setTooltip(null)}
      />
      {tooltip && (
        <div
          className="mon-chart-tooltip"
          style={{ left: tooltip.x, top: tooltip.y - 36 }}
        >
          {tooltip.text}
        </div>
      )}
    </div>
  );
}

// ── Main component ──

export function MonitoringTab({ conversations }: Props) {
  const [view, setView] = useState<MonitoringView>("latency");

  const latencySeries = useMemo(() => extractLatencySeries(conversations), [conversations]);
  const costSeries = useMemo(() => extractCostSeries(conversations), [conversations]);

  const activeSeries = view === "latency" ? latencySeries : costSeries;

  const [enabledKeys, setEnabledKeys] = useState<Set<string>>(new Set());

  // Auto-enable all keys when series change
  useEffect(() => {
    setEnabledKeys(new Set(activeSeries.map((s) => s.key)));
  }, [activeSeries]);

  const toggleKey = useCallback((key: string) => {
    setEnabledKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const formatValue = useCallback(
    (v: number) => view === "latency" ? `${Math.round(v)}ms` : `$${v.toFixed(4)}`,
    [view]
  );

  return (
    <div className="mon-tab">
      {/* Left sidebar */}
      <div className="mon-sidebar">
        <div className="mon-sidebar-header">Monitoring</div>
        <button
          className={`mon-sidebar-item ${view === "latency" ? "active" : ""}`}
          onClick={() => setView("latency")}
        >
          Latency
        </button>
        <button
          className={`mon-sidebar-item ${view === "cost" ? "active" : ""}`}
          onClick={() => setView("cost")}
        >
          Cost
        </button>

        <div className="mon-sidebar-divider" />
        <div className="mon-sidebar-header">Categories</div>
        {activeSeries.map((s) => (
          <label key={s.key} className="mon-category-item">
            <input
              type="checkbox"
              checked={enabledKeys.has(s.key)}
              onChange={() => toggleKey(s.key)}
            />
            <span
              className="mon-category-dot"
              style={{ background: s.color }}
            />
            <span className="mon-category-label">{s.label}</span>
            <span className="mon-category-count">{s.points.length}</span>
          </label>
        ))}
      </div>

      {/* Main chart area */}
      <div className="mon-main">
        <div className="mon-main-header">
          <h3>{view === "latency" ? "Latency over Time" : "Cost over Time"}</h3>
          <span className="mon-main-subtitle">
            {conversations.length} conversation{conversations.length !== 1 && "s"}
          </span>
        </div>
        <Chart
          series={activeSeries}
          enabledKeys={enabledKeys}
          yLabel={view === "latency" ? "Latency (ms)" : "Cost ($)"}
          formatValue={formatValue}
        />
      </div>
    </div>
  );
}
