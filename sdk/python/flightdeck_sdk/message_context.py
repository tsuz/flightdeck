import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from confluent_kafka import Consumer, Producer, TopicPartition

logger = logging.getLogger(__name__)


class KafkaMessageContext:
    def __init__(
        self,
        consumer: Consumer,
        output_producer: Producer,
        dlq_producer: Producer,
        output_topic: str,
        dlq_topic: str,
        topic: str,
        partition: int,
        offset: int,
        key: str | None,
        value: str | None,
        tool_use_id: str,
        incoming: dict,
    ):
        self._consumer = consumer
        self._output_producer = output_producer
        self._dlq_producer = dlq_producer
        self._output_topic = output_topic
        self._dlq_topic = dlq_topic
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._tool_use_id = tool_use_id
        self._incoming = incoming
        self._settled = False
        self._start_ms = time.time() * 1000

    def success(self, content: Any) -> None:
        """Publish the tool result to the output topic, then store offset for auto-commit."""
        self._ensure_not_settled()
        self._settled = True

        latency_ms = int(time.time() * 1000 - self._start_ms)

        payload = json.dumps({
            "session_id": self._incoming.get("session_id", self._key or ""),
            "tool_use_id": self._tool_use_id,
            "name": self._incoming.get("name", ""),
            "result": content,
            "latency_ms": latency_ms,
            "status": "success",
            "total_tools": self._incoming.get("total_tools", 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        self._output_producer.produce(
            topic=self._output_topic,
            key=self._key,
            value=payload,
            callback=self._on_output_delivered,
        )
        self._output_producer.flush()

    def error(self, reason: str) -> None:
        """Send the original message to the DLQ, then commit offset async."""
        self._ensure_not_settled()
        self._settled = True

        headers = [
            ("error.reason", reason.encode()),
            ("error.source.topic", self._topic.encode()),
            ("error.source.partition", str(self._partition).encode()),
            ("error.source.offset", str(self._offset).encode()),
        ]

        self._dlq_producer.produce(
            topic=self._dlq_topic,
            key=self._key,
            value=self._value,
            headers=headers,
            callback=self._on_dlq_delivered,
        )
        self._dlq_producer.flush()

    @property
    def settled(self) -> bool:
        return self._settled

    def _store_offset(self):
        tp = TopicPartition(self._topic, self._partition, self._offset + 1)
        self._consumer.store_offsets(offsets=[tp])

    def _on_output_delivered(self, err, msg):
        if err:
            logger.error("Failed to produce result: %s", err)
            return
        self._store_offset()

    def _on_dlq_delivered(self, err, msg):
        if err:
            logger.error("Failed to send to DLQ: %s", err)
            return
        self._store_offset()

    def _ensure_not_settled(self):
        if self._settled:
            raise RuntimeError("Message already settled (success or error was already called)")
