"""
Kafka operations tool consumer — rewritten using the flightdeck SDK.

Tools:
    kafka_list_topics           — list all topics
    kafka_describe_topic        — describe a topic (partitions, ISR, config)
    kafka_list_consumer_groups  — list consumer groups
    kafka_consumer_group_lag    — check consumer group lag
    kafka_broker_health         — check broker health
    search_knowledge_base       — query the RAG vector database for ops docs

Env vars:
    KAFKA_BOOTSTRAP_SERVERS     — FlightDeck Kafka (for consuming/producing tool messages)
    TARGET_KAFKA_BOOTSTRAP_SERVERS — Kafka cluster to monitor (can be the same)
    AGENT_NAME                  — agent name (for topic prefix)
    RAG_API_URL                 — RAG service endpoint (optional)
"""

import json
import os
import urllib.request

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TARGET_BOOTSTRAP = os.environ.get("TARGET_KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP)
AGENT_NAME = os.environ["AGENT_NAME"]
RAG_API_URL = os.environ.get("RAG_API_URL", "")

# ── Admin client for the monitored cluster ───────────────────────────────────

admin = AdminClient({"bootstrap.servers": TARGET_BOOTSTRAP})

# ── Kafka ops tools ──────────────────────────────────────────────────────────


def kafka_list_topics(_input):
    metadata = admin.list_topics(timeout=10)
    topics = []
    for name, topic_meta in sorted(metadata.topics.items()):
        if name.startswith("__"):
            continue
        partitions = topic_meta.partitions
        rep_factor = len(partitions[0].replicas) if partitions else 0
        topics.append({
            "name": name,
            "partitions": len(partitions),
            "replication_factor": rep_factor,
        })
    return {"topics": topics, "count": len(topics)}


def kafka_describe_topic(input_data):
    topic = input_data.get("topic", "")
    if not topic:
        return {"error": "Missing 'topic' parameter"}

    metadata = admin.list_topics(topic=topic, timeout=10)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None or topic_meta.error is not None:
        return {"error": f"Topic '{topic}' not found"}

    partitions = []
    for pid, p_meta in sorted(topic_meta.partitions.items()):
        partitions.append({
            "partition": pid,
            "leader": p_meta.leader,
            "replicas": list(p_meta.replicas),
            "isr": list(p_meta.isrs),
        })

    config = {}
    try:
        from confluent_kafka.admin import ConfigResource, ResourceType
        resource = ConfigResource(ResourceType.TOPIC, topic)
        futures = admin.describe_configs([resource])
        for res, future in futures.items():
            conf = future.result(timeout=10)
            for key, entry in conf.items():
                if not entry.is_default:
                    config[key] = entry.value
    except Exception:
        pass

    return {
        "topic": topic,
        "partitions": partitions,
        "partition_count": len(partitions),
        "replication_factor": len(partitions[0]["replicas"]) if partitions else 0,
        "config": config,
    }


def kafka_list_consumer_groups(_input):
    future = admin.list_consumer_groups()
    result = future.result(timeout=10)

    groups = []
    for g in result.valid:
        groups.append({
            "group_id": g.group_id,
            "state": str(g.state),
            "type": str(g.type) if hasattr(g, "type") else "unknown",
        })

    if groups:
        group_ids = [g["group_id"] for g in groups]
        try:
            desc_futures = admin.describe_consumer_groups(group_ids)
            for group_id, future in desc_futures.items():
                desc = future.result(timeout=10)
                for g in groups:
                    if g["group_id"] == group_id:
                        g["members"] = len(desc.members)
                        g["coordinator"] = desc.coordinator.id if desc.coordinator else None
                        break
        except Exception:
            pass

    return {"groups": groups, "count": len(groups)}


def kafka_consumer_group_lag(input_data):
    group_id = input_data.get("group_id", "")
    if not group_id:
        return {"error": "Missing 'group_id' parameter"}

    try:
        future = admin.list_consumer_group_offsets([group_id])
        offsets_result = future[group_id].result(timeout=10)

        lag_consumer = Consumer({
            "bootstrap.servers": TARGET_BOOTSTRAP,
            "group.id": f"_lag_checker_{group_id}",
            "enable.auto.commit": False,
        })

        partitions_lag = []
        total_lag = 0

        for tp, offset_meta in offsets_result.items():
            committed = offset_meta.offset if offset_meta.offset >= 0 else 0
            low, high = lag_consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=10
            )
            lag = max(0, high - committed)
            total_lag += lag
            partitions_lag.append({
                "topic": tp.topic,
                "partition": tp.partition,
                "committed_offset": committed,
                "end_offset": high,
                "lag": lag,
            })

        lag_consumer.close()
        return {"group_id": group_id, "partitions": partitions_lag, "total_lag": total_lag}

    except KafkaException as e:
        return {"error": f"Failed to get lag for group '{group_id}': {str(e)}"}


def kafka_broker_health(_input):
    metadata = admin.list_topics(timeout=10)
    brokers = []
    for broker_id, broker_meta in sorted(metadata.brokers.items()):
        brokers.append({"id": broker_id, "host": broker_meta.host, "port": broker_meta.port})

    return {
        "cluster_id": metadata.cluster_id,
        "controller_id": metadata.controller_id,
        "brokers": brokers,
        "broker_count": len(brokers),
        "topic_count": len([t for t in metadata.topics if not t.startswith("__")]),
        "status": "healthy" if brokers else "no_brokers",
    }


# ── Knowledge base search (RAG) ─────────────────────────────────────────────


def search_knowledge_base(input_data):
    query = input_data.get("query", "")
    if not query:
        return {"error": "Missing 'query' parameter"}

    if not RAG_API_URL:
        return {"error": "RAG_API_URL is not configured"}

    try:
        req_body = json.dumps({
            "query": query,
            "top_k": 5,
            "session_id": "tool-exec",
        }).encode()

        req = urllib.request.Request(
            RAG_API_URL,
            data=req_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        documents = data.get("documents", [])
        results = [
            {
                "text": doc.get("text", ""),
                "score": doc.get("score", 0.0),
                "source": doc.get("category", ""),
            }
            for doc in documents
        ]

        print(f"[search_knowledge_base] query='{query}'")
        for i, doc in enumerate(results):
            preview = doc["text"][:120].replace("\n", " ")
            print(f"  [{i+1}] score={doc['score']:.3f} | {preview}...")
        return {"documents": results, "count": len(results)}

    except Exception as e:
        return {"error": f"Knowledge base search failed: {str(e)}"}


# ── Tool registry ────────────────────────────────────────────────────────────

TOOLS = {
    "kafka_list_topics": kafka_list_topics,
    "kafka_describe_topic": kafka_describe_topic,
    "kafka_list_consumer_groups": kafka_list_consumer_groups,
    "kafka_consumer_group_lag": kafka_consumer_group_lag,
    "kafka_broker_health": kafka_broker_health,
    "search_knowledge_base": search_knowledge_base,
}

# ── Process function ─────────────────────────────────────────────────────────


def process(key, value, ctx):
    request = json.loads(value)
    tool_name = request.get("name", "")

    handler = TOOLS.get(tool_name)
    if handler is None:
        ctx.error(f"Unknown tool: {tool_name}")
        return

    try:
        result = handler(request.get("input", {}))
        ctx.success(result)
    except Exception as e:
        ctx.error(str(e))


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    runner = ToolConsumerRunner(
        ToolConsumerConfig(
            agent_name=AGENT_NAME,
            brokers=KAFKA_BOOTSTRAP,
            process_fn=process,
        )
    )
    runner.start()
