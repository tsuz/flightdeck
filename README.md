# ai-agent-orchestration-kafka-example

A reliable, AI Agent orchestration using Kafka.

![Architecture](assets/img/architecture.png)

## Project Structure

| Folder | Description |
|--------|-------------|
| `api/` | Chat API — REST endpoint and WebSocket server that accepts user messages and produces to Kafka |
| `processor-apps/` | Kafka Streams app — enriches messages with session history, aggregates tool results, and routes the pipeline |
| `think/` | Think consumer — calls the Claude API with tool definitions, produces LLM responses |
| `tools/` | Tool execution consumer — executes tool invocations (e.g. external APIs) dispatched by the LLM |
| `memoir/` | Memoir consumer — generates long-term session summaries using Claude |
| `monitoring/` | Logging consumer — tails all Kafka topics for observability |
| `frontend/` | React + TypeScript dashboard — chat UI, pipeline execution viewer, and logs |

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- An [Anthropic API key](https://console.anthropic.com/)

### Run

```bash
# 1. Clone the repo
git clone https://github.com/tsuz/ai-agent-orchestration-kafka-example.git
cd ai-agent-orchestration-kafka-example

# 2. Create your .env file
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

# 3. Start all services
docker compose up --build
```

Once everything is up, open [http://localhost](http://localhost) in your browser.

### Services

| Service | Port |
|---------|------|
| Frontend (UI) | [localhost:80](http://localhost) |
| Chat API (REST) | [localhost:8000](http://localhost:8000) |
| Chat API (WebSocket) | localhost:8001 |
| Kafka | localhost:9092 |

### Stop

```bash
docker compose down
```