# ai-agent-orchestration-kafka-example

A reliable, AI Agent orchestration using Kafka.

![Architecture](assets/img/architecture.png)

## Project Structure

| Folder | Description |
|--------|-------------|
| `api/` | Chat API — REST endpoint and WebSocket server that accepts user messages and produces to Kafka |
| `processor-apps/` | Kafka Streams app — enriches messages with session history, aggregates tool results, and routes the pipeline |
| `think/` | Think consumer — calls the Claude API with RAG context and tool definitions, produces LLM responses |
| `tools/` | Tool execution consumer — executes tool invocations (e.g. external APIs) dispatched by the LLM |
| `prompts/` | RAG server — Python service with Qdrant vector DB for semantic search and document retrieval |
| `monitoring/` | Logging consumer — tails all Kafka topics for observability |
| `frontend/` | React + TypeScript dashboard — chat UI, pipeline execution viewer, and logs |
| `infra/` | Infrastructure — Docker Compose config for Kafka (KRaft mode) |