import { useState, useRef, useEffect } from "react";
import type { ChatMessage } from "../types";
import "./ChatTab.css";

interface Props {
  messages: ChatMessage[];
  thinking: boolean;
  sessionId: string;
  onSend: (content: string) => void;
  onNewChat: () => void;
}

export function ChatTab({ messages, thinking, sessionId, onSend, onNewChat }: Props) {
  const [input, setInput] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, thinking]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed) return;
    onSend(trimmed);
    setInput("");
  };

  return (
    <div className="chat-tab">
      <div className="chat-header">
        <span className="chat-session-id">{sessionId}</span>
        <button className="chat-new-btn" onClick={onNewChat}>New Chat</button>
      </div>

      <div className="chat-messages">
        {messages.length === 0 && !thinking && (
          <div className="chat-empty">
            <p>Send a message to start a conversation with the AI agent.</p>
          </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`chat-bubble ${msg.role}`}>
            <div className="chat-role">{msg.role === "user" ? "You" : "Agent"}</div>
            <div className="chat-content">{msg.content}</div>
            <div className="chat-meta">
              <span className="chat-time">
                {new Date(msg.timestamp).toLocaleTimeString()}
              </span>
              {msg.cost != null && (
                <span className="chat-cost">
                  {msg.cost < 0.01 ? `$${msg.cost.toFixed(6)}` : `$${msg.cost.toFixed(4)}`}
                </span>
              )}
            </div>
          </div>
        ))}
        {thinking && (
          <div className="chat-bubble assistant">
            <div className="chat-role">Agent</div>
            <div className="chat-thinking">
              <span className="chat-thinking-dot" />
              <span className="chat-thinking-dot" />
              <span className="chat-thinking-dot" />
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <form className="chat-input-bar" onSubmit={handleSubmit}>
        <textarea
          className="chat-input"
          placeholder="Type a message... (Shift+Enter for new line)"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              handleSubmit(e);
            }
          }}
          rows={1}
          autoFocus
        />
        <button className="chat-send-btn" type="submit" disabled={!input.trim()}>
          Send
        </button>
      </form>
    </div>
  );
}
