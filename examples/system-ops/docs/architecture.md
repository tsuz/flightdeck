# Kafka Cluster Architecture

## Cluster Overview

- **Brokers**: Single-node Kafka cluster (KRaft mode, no ZooKeeper)
- **Image**: apache/kafka:4.1.1
- **Hostname**: kafka
- **Cluster ID**: MkU3OEVBNTcwNTJENDM2Qk
- **Roles**: Combined broker + controller on a single node (KAFKA_NODE_ID=1)
- **Replication factor**: 1 (single-node cluster)
- **Min ISR**: 1
- **Auto topic creation**: Enabled

## Listeners

| Listener | Address | Purpose |
|----------|---------|---------|
| INTERNAL | kafka:29092 | Communication between services within the Docker network |
| EXTERNAL | localhost:9092 | Access from the host machine (development/debugging) |
| CONTROLLER | kafka:9093 | KRaft controller communication |

- Inter-broker listener: INTERNAL
- All listeners use PLAINTEXT (no TLS)

## Advertised Listeners

- INTERNAL: `kafka:29092` — used by all application services (processing, think-consumer, tool-execution-consumer, etc.)
- EXTERNAL: `localhost:9092` — used by CLI tools running on the host

## Storage

- Log directory: `/var/lib/kafka/data`
- No persistent volume configured — data is ephemeral and lost on container restart

## Configuration Details

| Setting | Value | Notes |
|---------|-------|-------|
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | 1 | Single-node, cannot replicate |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | 1 | Single-node, cannot replicate |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | 1 | Single-node, cannot have ISR > 1 |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | 0 | No delay — consumers join immediately (good for dev) |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | true | Topics are auto-created on first produce/consume |

## Health Check

The broker health is verified by running:
```
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
- Check interval: 10s
- Timeout: 10s
- Retries: 10
- Start period: 30s (waits for broker to initialize before checking)

## Processing Apps

The FlightDeck pipeline runs as a set of services that communicate through Kafka topics. All topic names are prefixed with the agent name (`system-ops-`).

### Services

| Service | Type | Description |
|---------|------|-------------|
| `api` | REST + WebSocket | Accepts user messages, produces to `system-ops-message-input`, streams responses from `system-ops-message-output` via WebSocket |
| `processing` | Kafka Streams | Core orchestration — enriches messages with session history, extracts tool calls, aggregates tool results, routes the pipeline |
| `think-consumer` | Kafka Consumer | Reads from `system-ops-enriched-message-input`, queries RAG for context, calls Claude API, produces to `system-ops-think-request-response` |
| `tool-execution-consumer` | Kafka Consumer | Reads tool calls from `system-ops-tool-use`, executes them, produces results to `system-ops-tool-use-result` |
| `monitoring` | Kafka Consumer | Subscribes to all pipeline topics and logs every message for observability |

### Topics

| Topic | Key | Purpose |
|-------|-----|---------|
| `system-ops-message-input` | session_id | Raw user messages from the Chat API |
| `system-ops-session-context` | session_id | Accumulated conversation history per session (KTable) |
| `system-ops-enriched-message-input` | session_id | User message merged with session history + RAG context, ready for Claude |
| `system-ops-think-request-response` | session_id | Claude API responses (may include tool-use blocks) |
| `system-ops-tool-use` | session_id | Individual tool call requests extracted from Claude's response |
| `system-ops-tool-use-result` | session_id | Results returned from tool execution |
| `system-ops-tool-use-all-complete` | session_id | Signal that all tool results for a turn have been collected |
| `system-ops-tool-use-dlq` | session_id | Dead-letter queue for failed tool executions |
| `system-ops-session-cost` | session_id | Token usage and cost per session |
| `system-ops-tool-use-latency` | tool_name | Per-tool execution latency metrics |
| `system-ops-message-output` | session_id | Final responses sent back to the user |

### Kafka Streams Internal Topics

The processing app (`system-ops-streams`) also creates internal topics for state stores and repartitioning:

- `system-ops-streams-*-changelog` — Changelog topics for KTable state stores (session-context-store, think-response-store)
- `system-ops-streams-KSTREAM-KEY-SELECT-*-repartition` — Repartition topics created when the stream is re-keyed (e.g., re-keying by user_id for memoir joins)

These are managed automatically by Kafka Streams and should not be modified manually.

### Consumer Groups

| Group | Service | Topics Consumed |
|-------|---------|----------------|
| `think-consumer-group` | think-consumer | `system-ops-enriched-message-input` |
| `tool-execution-consumer-group` | tool-execution-consumer | `system-ops-tool-use` |
| `chat-api-output-group` | api | `system-ops-message-output` |
| `chat-api-pipeline-group` | api | All pipeline topics (for WebSocket streaming) |
| `monitoring-consumer-group` | monitoring | All pipeline topics |
| `system-ops-streams` | processing | All source topics (managed by Kafka Streams) |

### Data Flow

```
User (browser)
    |
    v
[Chat API] --produce--> [system-ops-message-input]
                              |
                              v
                     [Processing: Kafka Streams]
                         enrich with session history
                              |
                              v
                     [system-ops-enriched-message-input]
                              |
                              v
                     [Think Consumer]
                         query RAG for ops docs
                         call Claude API with tools
                              |
                              v
                     [system-ops-think-request-response]
                              |
                     +--------+--------+
                     |                 |
                  end_turn?        tool_use?
                     |                 |
                     v                 v
            [system-ops-         [system-ops-tool-use]
             message-output]          |
                     |                v
                     v         [Tool Execution Consumer]
                  User              execute kafka ops
                  (browser)           |
                                     v
                              [system-ops-tool-use-result]
                                     |
                                     v
                              [Processing: aggregate]
                                     |
                                     v
                              [system-ops-message-input]
                                  (tool results fed back
                                   for next Claude turn)
```

## Limitations (Single-Node)

- No fault tolerance — if the broker goes down, the entire cluster is unavailable
- Replication factor is capped at 1 — no data redundancy
- Not suitable for production workloads
- Controller and broker share the same node — no isolation between roles
