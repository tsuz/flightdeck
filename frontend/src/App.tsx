import { useState } from "react";
import type { Tab } from "./types";
import { useWebSocket } from "./useWebSocket";
import { ChatTab } from "./tabs/ChatTab";
import { ExecutionTab } from "./tabs/ExecutionTab";
import { LogsTab } from "./tabs/LogsTab";
import "./App.css";

const TABS: { key: Tab; label: string }[] = [
  { key: "chat", label: "Chat" },
  { key: "execution", label: "Execution" },
  { key: "logs", label: "Logs" },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("chat");
  const { connected, messages, executions, logs, sendMessage } = useWebSocket();

  return (
    <div className="app">
      <header className="header">
        <h1 className="header-title">FlightDeck Dashboard</h1>
        <span className={`status-badge ${connected ? "connected" : "disconnected"}`}>
          {connected ? "Connected" : "Disconnected"}
        </span>
      </header>

      <nav className="tabs">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            className={`tab-btn ${activeTab === tab.key ? "active" : ""}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
            {tab.key === "execution" && executions.length > 0 && (
              <span className="tab-badge">{executions.length}</span>
            )}
            {tab.key === "logs" && logs.length > 0 && (
              <span className="tab-badge">{logs.length}</span>
            )}
          </button>
        ))}
      </nav>

      <main className="content">
        {activeTab === "chat" && (
          <ChatTab messages={messages} onSend={sendMessage} />
        )}
        {activeTab === "execution" && <ExecutionTab executions={executions} />}
        {activeTab === "logs" && <LogsTab logs={logs} />}
      </main>
    </div>
  );
}
