import { useState, useRef, useEffect } from "react";
import type { ChatMessage } from "../types";
import "./ChatTab.css";

interface Props {
  messages: ChatMessage[];
  onSend: (content: string) => void;
}

export function ChatTab({ messages, onSend }: Props) {
  const [input, setInput] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed) return;
    onSend(trimmed);
    setInput("");
  };

  return (
    <div className="chat-tab">
      <div className="chat-messages">
        {messages.length === 0 && (
          <div className="chat-empty">
            <p>Send a message to start a conversation with the AI agent.</p>
          </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`chat-bubble ${msg.role}`}>
            <div className="chat-role">{msg.role === "user" ? "You" : "Agent"}</div>
            <div className="chat-content">{msg.content}</div>
            <div className="chat-time">
              {new Date(msg.timestamp).toLocaleTimeString()}
            </div>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      <form className="chat-input-bar" onSubmit={handleSubmit}>
        <input
          className="chat-input"
          type="text"
          placeholder="Type a message..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          autoFocus
        />
        <button className="chat-send-btn" type="submit" disabled={!input.trim()}>
          Send
        </button>
      </form>
    </div>
  );
}
