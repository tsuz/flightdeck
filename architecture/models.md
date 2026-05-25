# Kafka Topic Schemas

All topics are prefixed with the `AGENT_NAME` environment variable: `{AGENT_NAME}-{topic-name}`

## Message Flow

```
message-input
    |
    |  [join with think-request-response KTable,
    |   session-cost KTable, memoir-context KTable]
    |
enriched-message-input
    |
    |  [ThinkConsumer calls Claude/Gemini]
    |  [compaction runs here if triggered]
    |
think-request-response
    |
    |--- [has tool_uses] --> tool-use (fanout: 1 message per tool)
    |                            |
    |                        [tool execution]
    |                            |
    |                        tool-use-result
    |                            |
    |                        tool-use-all-complete (when all results arrive)
    |                            |
    |                        message-input (re-enter pipeline as role="tool")
    |
    |--- [end_turn=true, no tools] --> message-output
    |
    |--- session-cost (aggregate token costs)
    |
    |--- [on inactivity] --> session-end
                                |
                            memoir-context-session-end
                                |
                            memoir-context (updated long-term memory)
```

---

## Environment Variables

### Processing Service

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_NAME` | *(required)* | Unique name for the agent instance. Prefixes all topic names. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `MEMOIR_ENABLED` | `true` | Enable per-user long-term memory across sessions |
| `MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS` | `20` | Seconds of inactivity before a session ends |
| `MEMOIR_SESSION_PUNCTUATE_INTERVAL_SECONDS` | `5` | How often to check for inactive sessions |

### Think Consumer Service

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_NAME` | *(required)* | Unique name for the agent instance |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_CONSUMER_GROUP` | `think-consumer-group` | Kafka consumer group ID |
| `LLM_PROVIDER` | `claude` | LLM provider: `claude` or `gemini` |
| `CLAUDE_API_KEY` | `""` | Anthropic API key |
| `CLAUDE_API_URL` | `https://api.anthropic.com/v1/messages` | Claude API endpoint |
| `CLAUDE_MODEL` | `claude-haiku-4-5-20251001` | Claude model to use |
| `CLAUDE_MAX_TOKENS` | `4096` | Max tokens per Claude response |
| `PROMPT_CACHING` | `false` | Enable Anthropic prompt caching (adds a `cache_control` breakpoint to the system prompt). Only takes effect when the cached prefix exceeds the model's minimum cacheable length (e.g. 4,096 tokens for Haiku 4.5, 1,024 for Sonnet). |
| `GEMINI_API_KEY` | `""` | Google Gemini API key |
| `GEMINI_API_URL` | `https://generativelanguage.googleapis.com/v1beta` | Gemini API endpoint |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Gemini model to use |
| `GEMINI_MAX_TOKENS` | `4096` | Max tokens per Gemini response |
| `SYSTEM_PROMPT_FILE` | `""` | Path to system prompt text file |
| `TOOLS_JSON_FILE` | `""` | Path to tool definitions JSON file |
| `INPUT_TOKEN_PRICE` | *(optional)* | Price per 1M input tokens (e.g. `3` for $3/MTok) |
| `OUTPUT_TOKEN_PRICE` | *(optional)* | Price per 1M output tokens (e.g. `15` for $15/MTok) |
| `BUDGET_PRICE_PER_SESSION` | *(optional)* | Max dollar cost per session. Agent stops on next Think call when exceeded. |
| `COMPACTION_USER_MESSAGE_TRIGGER` | `-1` (disabled) | Number of user messages before compaction runs. Must be >= 2 or -1. |
| `COMPACTION_USER_MESSAGE_UNTIL` | `2` | Number of recent user messages to keep uncompacted |
| `COMPACTION_PROMPT` | `"Summarize the following conversation concisely..."` | Prompt used for the compaction summary LLM call |
| `POLL_TIMEOUT_MS` | `1000` | Kafka consumer poll timeout in milliseconds |

---

## Topics

### message-input

Raw user messages and tool results entering the pipeline.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Unique session identifier |
| `user_id` | String | | User identifier |
| `role` | String | | `"user"`, `"assistant"`, or `"tool"` |
| `content` | Object | | Plain text (String) or structured data (List/Map for tool results) |
| `timestamp` | String | | ISO 8601 timestamp |
| `metadata` | Map\<String, Object\> | `null` | Optional metadata |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "role": "user",
  "content": "What's the invoice balance for customer 456?",
  "timestamp": "2025-04-04T10:30:00Z",
  "metadata": {
    "source": "web_chat"
  }
}
```

When re-entering the pipeline from tool execution (role="tool"):

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "role": "tool",
  "content": [
    {
      "tool_use_id": "tuid-abc123",
      "name": "get_invoice_balance",
      "result": { "balance_due": 1250.50 },
      "status": "success"
    }
  ],
  "timestamp": "2025-04-04T10:30:06Z",
  "metadata": {
    "tool_count": 1,
    "source": "tool-execution"
  }
}
```

---

### enriched-message-input

User message merged with full session history, cost, and memoir context. Consumed by the ThinkConsumer.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Unique session identifier |
| `user_id` | String | | User identifier |
| `cost` | Double | `null` | Aggregated session cost (USD) |
| `history` | List\<MessageInput\> | | Full chronological conversation history |
| `latest_input` | MessageInput | | The most recent user message |
| `memoir_context` | String | `null` | Long-term memoir summary (null if memoir disabled) |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "cost": 0.0005,
  "history": [
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "user",
      "content": "Hello",
      "timestamp": "2025-04-04T10:28:00Z"
    },
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "assistant",
      "content": "Hi! How can I help you today?",
      "timestamp": "2025-04-04T10:28:02Z"
    }
  ],
  "latest_input": {
    "session_id": "sess-12345",
    "user_id": "user-789",
    "role": "user",
    "content": "What's the invoice balance for customer 456?",
    "timestamp": "2025-04-04T10:30:00Z"
  },
  "memoir_context": "User frequently asks about invoice balances. Works in accounts receivable.",
  "timestamp": "2025-04-04T10:30:00Z"
}
```

---

### think-request-response

LLM response including tool-use blocks. Also stored as a KTable for enrichment and memoir joins.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Unique session identifier |
| `user_id` | String | | User identifier |
| `total_session_cost` | Double | `null` | Total session cost: `previous_session_cost + think_cost + compaction_cost` |
| `previous_session_cost` | Double | `null` | Cumulative cost from prior turns |
| `think_cost` | Double | `null` | Cost of this LLM call's input and output tokens (USD) |
| `think_input_tokens` | int | `0` | Tokens consumed by the request |
| `think_output_tokens` | int | `0` | Tokens generated by the response |
| `previous_messages` | List\<MessageInput\> | `[]` | Messages before this turn (may be compacted) |
| `last_input_message` | MessageInput | | The user message that triggered this turn |
| `last_input_response` | List\<MessageInput\> | `[]` | The LLM's response messages |
| `tool_uses` | List\<ToolUseItem\> | `null` | Tool invocation blocks (null/empty if no tools) |
| `end_turn` | boolean | `false` | true if LLM signaled end-of-turn |
| `compaction` | boolean | `false` | true if previous_messages were compacted this turn |
| `compaction_input_tokens` | int | `0` | Tokens used by the compaction summary LLM call |
| `compaction_output_tokens` | int | `0` | Tokens generated by the compaction summary |
| `compaction_cost` | double | `0.0` | Cost of the compaction LLM call |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "total_session_cost": 0.0017,
  "previous_session_cost": 0.0005,
  "think_cost": 0.0012,
  "think_input_tokens": 256,
  "think_output_tokens": 128,
  "previous_messages": [
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "user",
      "content": "Hello",
      "timestamp": "2025-04-04T10:28:00Z"
    },
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "assistant",
      "content": "Hi! How can I help you today?",
      "timestamp": "2025-04-04T10:28:02Z"
    }
  ],
  "last_input_message": {
    "session_id": "sess-12345",
    "user_id": "user-789",
    "role": "user",
    "content": "What's the invoice balance for customer 456?",
    "timestamp": "2025-04-04T10:30:00Z"
  },
  "last_input_response": [
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "assistant",
      "content": "I'll check the invoice balance for you.",
      "timestamp": "2025-04-04T10:30:05Z"
    }
  ],
  "tool_uses": [
    {
      "tool_use_id": "tuid-abc123",
      "tool_id": "billing-api",
      "name": "get_invoice_balance",
      "input": { "customer_id": "456" },
      "session_id": "sess-12345",
      "total_tools": 1,
      "timestamp": "2025-04-04T10:30:05Z"
    }
  ],
  "end_turn": false,
  "compaction": false,
  "compaction_input_tokens": 0,
  "compaction_output_tokens": 0,
  "compaction_cost": 0.0,
  "timestamp": "2025-04-04T10:30:05Z"
}
```

Example with compaction applied:

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "total_session_cost": 0.0071,
  "previous_session_cost": 0.0050,
  "think_cost": 0.0018,
  "think_input_tokens": 180,
  "think_output_tokens": 64,
  "previous_messages": [
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "assistant",
      "content": "Summary: User asked about invoices for customers 100-456. Balances were retrieved and shared.",
      "timestamp": "2025-04-04T10:35:00Z"
    },
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "user",
      "content": "Now check customer 789",
      "timestamp": "2025-04-04T10:36:00Z"
    },
    {
      "session_id": "sess-12345",
      "user_id": "user-789",
      "role": "assistant",
      "content": "The balance for customer 789 is $500.00.",
      "timestamp": "2025-04-04T10:36:02Z"
    }
  ],
  "last_input_message": {
    "session_id": "sess-12345",
    "user_id": "user-789",
    "role": "user",
    "content": "And customer 999?",
    "timestamp": "2025-04-04T10:37:00Z"
  },
  "last_input_response": [
    {
      "role": "assistant",
      "content": "The balance for customer 999 is $320.00."
    }
  ],
  "tool_uses": null,
  "end_turn": true,
  "compaction": true,
  "compaction_input_tokens": 512,
  "compaction_output_tokens": 48,
  "compaction_cost": 0.0003,
  "timestamp": "2025-04-04T10:37:02Z"
}
```

---

### tool-use

One message per individual tool invocation, fanned out from the LLM response by ExtractToolUseItemsProcessor.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tool_use_id` | String | | Unique identifier for this tool call |
| `tool_id` | String | | Identifier for the tool definition |
| `name` | String | | Tool function name |
| `input` | Map\<String, Object\> | | Tool function parameters |
| `session_id` | String | | Session identifier (message key) |
| `total_tools` | int | | Total parallel tool calls in this batch |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "tool_use_id": "tuid-abc123",
  "tool_id": "billing-api",
  "name": "get_invoice_balance",
  "input": {
    "customer_id": "456"
  },
  "session_id": "sess-12345",
  "total_tools": 1,
  "timestamp": "2025-04-04T10:30:05Z"
}
```

---

### tool-use-dlq

Dead letter queue for tool-use items that failed validation (missing `tool_use_id` or `name`).

**Key:** `session_id`

Same schema as **tool-use**, but fields may be null/blank.

```json
{
  "tool_use_id": null,
  "tool_id": "billing-api",
  "name": null,
  "input": { "customer_id": "456" },
  "session_id": "sess-12345",
  "total_tools": 1,
  "timestamp": "2025-04-04T10:30:05Z"
}
```

---

### tool-use-result

Results returned from tool execution.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Session identifier (message key) |
| `tool_use_id` | String | | Correlates with the original tool-use request |
| `name` | String | | Tool function name |
| `result` | Map\<String, Object\> | | The tool's return value |
| `latency_ms` | long | `0` | Execution time in milliseconds |
| `status` | String | | `"success"` or `"error"` |
| `total_tools` | int | | Total parallel tool calls expected |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "tool_use_id": "tuid-abc123",
  "name": "get_invoice_balance",
  "result": {
    "customer_id": "456",
    "balance_due": 1250.50,
    "currency": "USD",
    "due_date": "2025-04-15"
  },
  "latency_ms": 145,
  "status": "success",
  "total_tools": 1,
  "timestamp": "2025-04-04T10:30:06Z"
}
```

---

### tool-use-all-complete

Emitted once all expected tool results for a session have arrived.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Session identifier |
| `user_id` | String | `null` | User identifier |
| `expected_count` | int | `0` | How many tool results were expected |
| `results` | List\<ToolUseResult\> | `[]` | All accumulated tool results |
| `emitted` | boolean | `false` | Guard flag to prevent duplicate emissions |
| `timestamp` | String | `null` | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "expected_count": 1,
  "results": [
    {
      "session_id": "sess-12345",
      "tool_use_id": "tuid-abc123",
      "name": "get_invoice_balance",
      "result": {
        "customer_id": "456",
        "balance_due": 1250.50,
        "currency": "USD",
        "due_date": "2025-04-15"
      },
      "latency_ms": 145,
      "status": "success",
      "total_tools": 1,
      "timestamp": "2025-04-04T10:30:06Z"
    }
  ],
  "emitted": true,
  "timestamp": "2025-04-04T10:30:06Z"
}
```

---

### session-cost

Aggregated cost per session. Compacted topic (KTable). A tombstone (null value) is emitted when the session closes (cost = -1.0 sentinel).

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Session identifier (message key) |
| `user_id` | String | `null` | User identifier |
| `llm_calls` | int | `0` | Total LLM invocations in session |
| `total_input_tokens` | int | `0` | Cumulative input tokens |
| `total_output_tokens` | int | `0` | Cumulative output tokens |
| `estimated_cost_usd` | Double | `null` | Total estimated cost in USD |
| `timestamp` | String | `null` | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "llm_calls": 2,
  "total_input_tokens": 640,
  "total_output_tokens": 224,
  "estimated_cost_usd": 0.0032,
  "timestamp": "2025-04-04T10:30:07Z"
}
```

---

### tool-use-latency

Per-tool execution latency metrics. Keyed by tool name. Topic and model are defined but the processor is not yet implemented.

**Key:** `tool_name`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tool_name` | String | | Tool function name (message key) |
| `latency_ms` | long | `0` | Execution latency in milliseconds |
| `session_id` | String | | Session identifier |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "tool_name": "get_invoice_balance",
  "latency_ms": 145,
  "session_id": "sess-12345",
  "timestamp": "2025-04-04T10:30:06Z"
}
```

---

### session-end

Emitted when a session has been inactive for the configured threshold. Only active when `MEMOIR_ENABLED=true`.

**Key:** `session_id`

| Field | Type | Description |
|-------|------|-------------|
| key | String | session_id |
| value | String | Empty or null |

---

### memoir-context

Long-term user memoir/summary. Compacted topic keyed by user_id. Only active when `MEMOIR_ENABLED=true`.

**Key:** `user_id`

| Field | Type | Description |
|-------|------|-------------|
| key | String | user_id |
| value | String | Serialized memoir/summary text |

```
key: "user-789"
value: "User frequently asks about invoice balances. Prefers concise answers. Works in accounts receivable."
```

---

### memoir-context-session-end

Joined snapshot emitted on session end: previous memoir + last LLM response. Only active when `MEMOIR_ENABLED=true`.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Session identifier |
| `user_id` | String | | User identifier |
| `memoir_context` | String | `null` | Previous memoir/summary |
| `think_response` | ThinkResponse | | Last LLM response (contains full conversation) |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "memoir_context": "User frequently asks about invoice balances.",
  "think_response": {
    "session_id": "sess-12345",
    "user_id": "user-789",
    "total_session_cost": 0.0017,
    "previous_session_cost": 0.0005,
    "think_cost": 0.0012,
    "think_input_tokens": 256,
    "think_output_tokens": 128,
    "previous_messages": [],
    "last_input_message": {
      "role": "user",
      "content": "What's the invoice balance for customer 456?"
    },
    "last_input_response": [
      {
        "role": "assistant",
        "content": "The balance for customer 456 is $1,250.50, due April 15."
      }
    ],
    "tool_uses": null,
    "end_turn": true,
    "compaction": false,
    "compaction_input_tokens": 0,
    "compaction_output_tokens": 0,
    "compaction_cost": 0.0,
    "timestamp": "2025-04-04T10:30:07Z"
  },
  "timestamp": "2025-04-04T10:35:00Z"
}
```

---

### message-output

Final response sent back to the user-facing layer.

**Key:** `session_id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `session_id` | String | | Session identifier |
| `user_id` | String | | User identifier |
| `content` | String | | The response text |
| `llm_calls` | int | `0` | Number of LLM calls in this turn |
| `input_tokens` | int | `0` | Input tokens for this call |
| `output_tokens` | int | `0` | Output tokens for this call |
| `cost` | Double | `null` | Total session cost (USD) |
| `source_agent` | String | | Which agent produced this response |
| `timestamp` | String | | ISO 8601 timestamp |

```json
{
  "session_id": "sess-12345",
  "user_id": "user-789",
  "content": "The current balance due for customer 456 is $1,250.50, due on April 15, 2025.",
  "llm_calls": 2,
  "input_tokens": 384,
  "output_tokens": 96,
  "cost": 0.0032,
  "source_agent": "billing-agent",
  "timestamp": "2025-04-04T10:30:07Z"
}
```

---

## State Stores

| Store | Keyed By | Purpose |
|-------|----------|---------|
| `think-response-store` | session_id | Caches think-request-response KTable |
| `session-cost-table-store` | session_id | Caches session-cost KTable |
| `session-cost-store` | session_id | Aggregates cost per session (processor-local) |
| `tool-result-accumulator-store` | session_id | Accumulates tool results until all arrive |
| `session-last-seen-store` | session_id | Tracks last activity timestamp for inactivity detection |
| `memoir-context-store` | user_id | Caches memoir-context KTable (memoir only) |

## Processors

| Processor | Consumes | Produces |
|-----------|----------|----------|
| EnrichInputMessageProcessor | `message-input` + KTable joins | `enriched-message-input` |
| ExtractToolUseItemsProcessor | `think-request-response` | `tool-use`, `tool-use-dlq` |
| AggregateToolExecutionResultProcessor | `tool-use-result` | `tool-use-all-complete` |
| TransformToolUseDoneProcessor | `tool-use-all-complete` | `message-input` |
| SessionCostAggregationProcessor | `think-request-response` | `session-cost` |
| EndTurnProcessor | `think-request-response` | `message-output` |
| SessionEndProcessor | `think-request-response` | `session-end` |
| MemoirSessionEndProcessor | `session-end` + KTable joins | `memoir-context-session-end` |
