export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
}

export interface ToolUseBlock {
  id: string;
  name: string;
  input: Record<string, unknown>;
}

export interface ToolExecution {
  toolUseId: string;
  name: string;
  input: Record<string, unknown>;
  status: "pending" | "running" | "completed" | "failed";
  result?: string;
  startedAt: string;
  completedAt?: string;
}

export interface StepCost {
  inputTokens?: number;
  outputTokens?: number;
  dollars: number;
}

export interface PipelineStep {
  id: string;
  type: "user_request" | "llm_query" | "llm_response" | "tool_execution" | "user_response";
  timestamp: string;
  status: "pending" | "processing" | "completed" | "failed" | "retrying";
  latencyMs?: number;
  cost?: StepCost;
  retryOf?: string;
  payload: unknown;
}

export interface Conversation {
  id: string;
  userMessage: string;
  createdAt: string;
  maxCostDollars: number;
  steps: PipelineStep[];
}

export interface PipelineEvent {
  topic: string;
  sessionId: string;
  timestamp: string;
  partition: number;
  offset: number;
  value?: Record<string, unknown>;
  rawValue?: string;
  cost?: {
    inputTokens?: number;
    outputTokens?: number;
    dollars?: number;
    endTurn?: boolean;
  };
  latencyMs?: number;
}

export interface LogEntry {
  id: string;
  level: "info" | "warn" | "error" | "debug";
  source: string;
  message: string;
  metadata?: Record<string, unknown>;
  timestamp: string;
}

export type Tab = "chat" | "execution" | "monitoring" | "logs";

export type WsMessage =
  | { type: "chat_response"; data: ChatMessage }
  | { type: "conversation_update"; data: Conversation }
  | { type: "pipeline_event"; data: PipelineEvent }
  | { type: "log"; data: LogEntry };
