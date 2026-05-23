# Lead Follow-Up Agent — Confluent Cloud

Same demo as [`lead-followup-agent`](../lead-followup-agent), but talks to a
**Confluent Cloud** cluster instead of a local Kafka broker. Every service
image is built from local source so you can test code changes against a real
cloud cluster without publishing images.

Use this example to validate that a set of Confluent Cloud API keys works
end-to-end across all FlightDeck processors (Java + Python).

## What's different from `lead-followup-agent`

| | `lead-followup-agent` | this example |
|---|---|---|
| Kafka broker | Local container (`apache/kafka:4.1.1`) | Confluent Cloud (SASL_SSL / PLAIN) |
| Service images | Pulled from `ghcr.io/tsuz/flightdeck/*` | Built from local source |
| Auth env vars | none | `CONFLUENT_*` in `.env` |

## Prerequisites

1. A Confluent Cloud cluster (any type — Basic is fine for testing).
2. A **cluster-scoped API key + secret** (Cloud → Cluster → API Keys → Add Key).
   The cluster's Cloud API key will NOT work — it must be scoped to the
   specific cluster.
3. A Claude API key.
4. Docker + `docker compose`.

## Pre-create topics in Confluent Cloud

> **Why this is required:** the `processing` service tries to auto-create
> topics with replication factor 1, which Confluent Cloud rejects (CC enforces
> RF ≥ 3). So topics must exist *before* the stack starts.

Topics use the agent name as a prefix. Assuming the default `AGENT_NAME=lead-followup`,
create these topics in the cluster (default partition count is fine, RF = 3):

```
lead-followup-message-input
lead-followup-enriched-message-input
lead-followup-think-request-response
lead-followup-tool-use
lead-followup-tool-use-result
lead-followup-tool-use-dlq
lead-followup-tool-use-all-complete
lead-followup-tool-use-latency
lead-followup-message-output
lead-followup-think-dlq
lead-followup-session-context
```

### Option A — Confluent Cloud Console

UI → Cluster → Topics → Add topic — repeat for each name above.

### Option B — `confluent` CLI

```bash
confluent login
confluent kafka cluster use <CLUSTER_ID>

for t in \
  lead-followup-message-input \
  lead-followup-enriched-message-input \
  lead-followup-think-request-response \
  lead-followup-tool-use \
  lead-followup-tool-use-result \
  lead-followup-tool-use-dlq \
  lead-followup-tool-use-all-complete \
  lead-followup-tool-use-latency \
  lead-followup-message-output \
  lead-followup-think-dlq \
  lead-followup-session-context; do
  confluent kafka topic create "$t"
done
```

If you change `AGENT_NAME`, change the topic prefix to match.

## Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | Where to get it |
|---|---|
| `CONFLUENT_BOOTSTRAP_SERVERS` | Cluster → Cluster settings → Bootstrap server (e.g. `pkc-921jm.us-east-2.aws.confluent.cloud:9092`) |
| `CONFLUENT_API_KEY` | Cluster → API Keys → your key |
| `CONFLUENT_API_SECRET` | Shown once when the key was created |
| `CLAUDE_API_KEY` | console.anthropic.com → API Keys |

## Run

```bash
docker compose up --build
```

First build takes a few minutes (Maven downloads + multi-stage Java builds).
Subsequent runs are cached.

Open <http://localhost> and try a prompt like:

> "Find all dormant leads we haven't contacted since August 2025"

## How auth is wired

The processors all use the `KafkaEnvProps` pass-through pattern: any
`KAFKA_*` env var becomes a Kafka client config property (`KAFKA_FOO_BAR` →
`foo.bar`). The compose file sets these per service:

**Java services** (`api`, `processing`, `think-consumer`) — Apache Kafka client uses JAAS:
```
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";
```

**Python service** (`lead-tool-service`) — librdkafka takes credentials directly:
```
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=...
KAFKA_SASL_PASSWORD=...
```

`processing` additionally sets `KAFKA_REPLICATION_FACTOR=3` so Kafka Streams
creates its internal/changelog topics with the RF Confluent Cloud requires.

## Verifying the keys work

A successful end-to-end run means every service authenticated. To narrow down
the source of an auth failure:

```bash
# Show only auth/connection errors
docker compose logs api processing think-consumer lead-tool-service | grep -iE 'sasl|auth|disconnect|topicauthorization'
```

Common failure modes:

| Symptom | Likely cause |
|---|---|
| `SaslAuthenticationException: Authentication failed` | API key/secret wrong, or key not scoped to this cluster |
| `TopicAuthorizationException` on a `lead-followup-*` topic | ACL missing for the key — grant the key write/read on the topic prefix |
| `InvalidReplicationFactorException` from `processing` | A topic still missing — re-check the pre-create list above |
| `UnknownTopicOrPartitionException` | Topic name typo, or `AGENT_NAME` changed but topics not renamed |
| Hangs at startup, no errors | Bootstrap server hostname wrong, or outbound 9092 blocked |

## Clean up

```bash
docker compose down
```

Topics, ACLs, and the API key remain in Confluent Cloud — delete those from
the Cloud console if you don't want to be billed for storage.
