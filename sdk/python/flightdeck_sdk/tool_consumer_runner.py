import json
import logging
import signal
from dataclasses import dataclass
from typing import Callable, Optional

from confluent_kafka import Consumer, Producer

from .message_context import KafkaMessageContext

logger = logging.getLogger(__name__)

ProcessFn = Callable[[Optional[str], Optional[str], KafkaMessageContext], None]


@dataclass
class ToolConsumerConfig:
    agent_name: str
    brokers: str
    process_fn: ProcessFn
    poll_timeout_s: float = 1.0

    @property
    def group_id(self) -> str:
        return f"{self.agent_name}-tool-execution"

    @property
    def input_topic(self) -> str:
        return f"{self.agent_name}-tool-use"

    @property
    def output_topic(self) -> str:
        return f"{self.agent_name}-tool-use-result"

    @property
    def dlq_topic(self) -> str:
        return f"{self.agent_name}-tool-use-dlq"


class ToolConsumerRunner:
    def __init__(self, config: ToolConsumerConfig):
        self._config = config
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": config.brokers,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.store": False,
            "enable.auto.commit": True,
        })

        self._producer = Producer({
            "bootstrap.servers": config.brokers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4"
        })

    def start(self) -> None:
        self._running = True
        self._consumer.subscribe([self._config.input_topic])
        logger.info("ToolConsumerRunner started — listening on [%s]", self._config.input_topic)

        signal.signal(signal.SIGINT, lambda *_: self.stop())
        signal.signal(signal.SIGTERM, lambda *_: self.stop())

        while self._running:
            msg = self._consumer.poll(self._config.poll_timeout_s)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            key = msg.key().decode() if msg.key() else None
            value = msg.value().decode() if msg.value() else None

            # Parse incoming message
            try:
                parsed = json.loads(value) if value else {}
            except json.JSONDecodeError:
                parsed = {}

            tool_use_id = parsed.get("tool_use_id", "")

            ctx = KafkaMessageContext(
                consumer=self._consumer,
                output_producer=self._producer,
                dlq_producer=self._producer,
                output_topic=self._config.output_topic,
                dlq_topic=self._config.dlq_topic,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=key,
                value=value,
                tool_use_id=tool_use_id,
                incoming=parsed,
            )

            try:
                self._config.process_fn(key, value, ctx)

                if not ctx.settled:
                    logger.warning(
                        "Developer did not call success() or error() for offset %d — skipping",
                        msg.offset(),
                    )
            except Exception as e:
                logger.error("Uncaught exception at offset %d: %s", msg.offset(), e)
                if not ctx.settled:
                    ctx.error(f"Uncaught exception: {e}")

    def stop(self) -> None:
        logger.info("Shutting down ToolConsumerRunner...")
        self._running = False
        self._consumer.close()
        self._producer.flush()
