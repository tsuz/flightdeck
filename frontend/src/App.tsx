import { useState } from "react";
import type { Tab } from "./types";
import { useWebSocket } from "./useWebSocket";
import { ChatTab } from "./tabs/ChatTab";
import { ExecutionTab } from "./tabs/ExecutionTab";
import { MonitoringTab } from "./tabs/MonitoringTab";
import { LogsTab } from "./tabs/LogsTab";
import "./App.css";

const TABS: { key: Tab; label: string }[] = [
  { key: "chat", label: "Chat" },
  { key: "execution", label: "Execution" },
  { key: "monitoring", label: "Monitoring" },
  { key: "logs", label: "Logs" },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("chat");
  const { connected, messages, conversations, logs, thinking, sessionId, sendMessage, newChat } = useWebSocket();

  return (
    <div className="app">
      <header className="header">
        <h1 className="header-title">Dashboard</h1>
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
            {tab.key === "execution" && conversations.length > 0 && (
              <span className="tab-badge">{conversations.length}</span>
            )}
            {tab.key === "logs" && logs.length > 0 && (
              <span className="tab-badge">{logs.length}</span>
            )}
          </button>
        ))}
      </nav>

      <main className="content">
        {activeTab === "chat" && (
          <ChatTab messages={messages} thinking={thinking} sessionId={sessionId} onSend={sendMessage} onNewChat={newChat} />
        )}
        {activeTab === "execution" && <ExecutionTab conversations={conversations} />}
        {activeTab === "monitoring" && <MonitoringTab conversations={conversations} />}
        {activeTab === "logs" && <LogsTab logs={logs} />}
      </main>
    </div>
  );
}
