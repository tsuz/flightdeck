# System Ops — Kafka Operations Agent

An AI agent that monitors and troubleshoots a Kafka cluster, powered by architecture docs and an ops runbook loaded into a vector database.

## How it works

This example combines three knowledge sources to create a Kafka operations assistant:

### 1. Cluster Architecture (`docs/architecture.md`)

Describes the Kafka cluster design — brokers, topics, consumer groups, data flow, and SLAs. This document is embedded into Qdrant so the agent can reference it when answering questions about the expected state of the system.

### 2. Troubleshooting Runbook (`docs/troubleshooting-runbook.md`)

Step-by-step procedures for diagnosing and resolving common Kafka issues: consumer lag, under-replicated partitions, broker failures, disk pressure, rebalancing storms, and more. Loaded into Qdrant via RAG so the agent retrieves the relevant runbook section based on the user's question.

### 3. Kafka Cluster Tools (`tools.json`)

Tool definitions that give the agent access to the Kafka cluster:

| Tool | Description |
|------|-------------|
| `kafka_list_topics` | List all topics with partition counts and replication factors |
| `kafka_describe_topic` | Get partition details, ISR, and config for a specific topic |
| `kafka_list_consumer_groups` | List all consumer groups with state and member count |
| `kafka_consumer_group_lag` | Check consumer lag for a specific group |
| `kafka_broker_health` | Check broker status and controller info |

## Configuration

| Parameter | Value | Why |
|-----------|-------|-----|
| `SYSTEM_PROMPT_FILE` | `system-prompt.txt` | Custom system prompt that instructs Claude to always use tools and never suggest CLI commands |
| `TOOLS_JSON_FILE` | `tools.json` | Kafka ops tools + `search_knowledge_base` tool defined by the developer |
| `RAG_API_URL` | `http://rag-service:8081/api/rag/query` | On the kafka-tool-service — used by the `search_knowledge_base` tool to query the vector database |
| `MEMOIR_ENABLED` | `false` | Ops agent doesn't need per-user memory across sessions |

## Services

| Service | Description |
|---------|-------------|
| `kafka` | Kafka broker (also the cluster being monitored in this example) |
| `qdrant` | Vector database storing embedded architecture + runbook docs |
| `rag-service` | Semantic search API over Qdrant |
| `rag-seed` | One-shot job that chunks and embeds the docs into Qdrant |
| `kafka-tool-service` | Python Kafka consumer that executes Kafka ops tools and knowledge base searches — replaces the generic tool-execution-consumer |
| `api` | Chat API (REST + WebSocket) |
| `processing` | Kafka Streams pipeline (memoir disabled) |
| `think-consumer` | Claude API caller with RAG context and Kafka ops tools |
| `monitoring` | Logs all pipeline events |
| `frontend` | Web UI |

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Open [http://localhost](http://localhost) in your browser.

## Example prompts

- "What topics exist in the cluster?"
- "Check the consumer lag for the order-processor group"
- "The analytics pipeline seems slow, what should I check?"
- "What's the expected retention for the audit-log topic?"
- "Broker-2 might be having issues, can you check the cluster health?"

## Kafka Tool Service

The `kafka-tool-service/` directory contains a Python Kafka consumer that replaces the generic `tool-execution-consumer` for this example. It reads tool requests from the `{AGENT_NAME}-tool-use` topic, executes them, and produces results to `{AGENT_NAME}-tool-use-result`.

It implements all 6 tools:
- **Kafka ops** (`kafka_list_topics`, `kafka_describe_topic`, `kafka_list_consumer_groups`, `kafka_consumer_group_lag`, `kafka_broker_health`) — uses `confluent-kafka` AdminClient against `TARGET_KAFKA_BOOTSTRAP_SERVERS`
- **`search_knowledge_base`** — queries the RAG service at `RAG_API_URL` to retrieve relevant architecture docs and troubleshooting runbooks from Qdrant

To point at a different Kafka cluster for monitoring, change `TARGET_KAFKA_BOOTSTRAP_SERVERS` on the kafka-tool-service in docker-compose.
