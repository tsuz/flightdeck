export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
}

export interface ExecutionRequest {
  id: string;
  userMessage: string;
  status: "pending" | "processing" | "completed" | "failed";
  toolCalls: ToolCall[];
  response?: string;
  createdAt: string;
  completedAt?: string;
}

export interface ToolCall {
  id: string;
  name: string;
  input: Record<string, unknown>;
  result?: string;
  status: "pending" | "running" | "completed" | "failed";
  startedAt: string;
  completedAt?: string;
}

export interface LogEntry {
  id: string;
  level: "info" | "warn" | "error" | "debug";
  source: string;
  message: string;
  metadata?: Record<string, unknown>;
  timestamp: string;
}

export type Tab = "chat" | "execution" | "logs";

export type WsMessage =
  | { type: "chat_response"; data: ChatMessage }
  | { type: "execution_update"; data: ExecutionRequest }
  | { type: "log"; data: LogEntry };
