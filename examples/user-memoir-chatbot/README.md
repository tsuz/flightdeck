# User Memoir Chatbot

A simple chatbot that remembers users across sessions using FlightDeck's memoir feature. No tools — just conversation with long-term memory.

## What it does

- Chat with an AI assistant via the web UI
- Session context is maintained throughout a conversation
- After 30 seconds of inactivity, the session is summarized and stored as a memoir (`MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS=30`)
- When the same user starts a new session, the memoir is loaded to personalize responses

## Configuration

| Parameter | Value | Why |
|-----------|-------|-----|
| `MEMOIR_ENABLED` | `true` | Enables per-user long-term memory across sessions |
| `MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS` | `30` | Session ends after 30s of inactivity, triggering memoir save |
| `MEMOIR_SESSION_PUNCTUATE_INTERVAL_SECONDS` | `5` | Checks for inactive sessions every 5s |
| `TOOLS_JSON_FILE` | *(not set)* | No tools — agent runs as a pure chatbot |

## Services

This example runs **without** the tool-execution-consumer and monitoring services since no tools are configured.

| Service | Description |
|---------|-------------|
| `api` | Chat API (REST + WebSocket) |
| `processing` | Kafka Streams pipeline with memoir enabled |
| `think-consumer` | Claude API caller (no tools) |
| `memoir-consumer` | Generates session summaries |
| `frontend` | Web UI |

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Open [http://localhost](http://localhost) in your browser.
