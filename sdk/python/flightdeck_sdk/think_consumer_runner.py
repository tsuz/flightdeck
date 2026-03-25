import json
import logging
import os
import signal
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from confluent_kafka import Consumer, Producer, TopicPartition

logger = logging.getLogger(__name__)


@dataclass
class ThinkConsumerConfig:
    agent_name: str
    brokers: str
    claude_api_key: str
    system_prompt: str
    tools: list[dict] = field(default_factory=list)
    claude_model: str = "claude-haiku-4-5-20251001"
    claude_max_tokens: int = 4096
    claude_api_url: str = "https://api.anthropic.com/v1/messages"
    poll_timeout_s: float = 1.0
    system_prompt_builder: Optional[Callable[[str, dict], str]] = None

    @property
    def group_id(self) -> str:
        return f"{self.agent_name}-think-consumer"

    @property
    def input_topic(self) -> str:
        return f"{self.agent_name}-enriched-message-input"

    @property
    def output_topic(self) -> str:
        return f"{self.agent_name}-think-request-response"

    @property
    def dlq_topic(self) -> str:
        return f"{self.agent_name}-think-dlq"


class ThinkConsumerRunner:
    # Token pricing from environment variables (per-token, not per-million)
    _input_price_str = os.environ.get("INPUT_TOKEN_PRICE")
    _output_price_str = os.environ.get("OUTPUT_TOKEN_PRICE")

    if _input_price_str and _output_price_str:
        INPUT_TOKEN_PRICE: Optional[float] = float(_input_price_str)
        OUTPUT_TOKEN_PRICE: Optional[float] = float(_output_price_str)
    else:
        logger.warning("INPUT_TOKEN_PRICE and/or OUTPUT_TOKEN_PRICE not set — cost will not be calculated")
        INPUT_TOKEN_PRICE = None
        OUTPUT_TOKEN_PRICE = None

    def __init__(self, config: ThinkConsumerConfig):
        self._config = config
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": config.brokers,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.store": False,
            "enable.auto.commit": True,
            "max.poll.interval.ms": 300000,
        })

        self._producer = Producer({
            "bootstrap.servers": config.brokers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
        })

    def start(self) -> None:
        self._running = True
        self._consumer.subscribe([self._config.input_topic])
        logger.info("ThinkConsumerRunner started — listening on [%s]", self._config.input_topic)

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

            try:
                self._process_record(key, value, msg.topic(), msg.partition(), msg.offset())
            except Exception as e:
                logger.error("Error processing offset %d: %s", msg.offset(), e)
                self._send_to_dlq(key, value, str(e), msg.topic(), msg.partition(), msg.offset())

    def stop(self) -> None:
        logger.info("Shutting down ThinkConsumerRunner...")
        self._running = False
        self._consumer.close()
        self._producer.flush()

    def _process_record(self, key: str | None, value: str | None, topic: str, partition: int, offset: int) -> None:
        context = json.loads(value) if value else {}

        session_id = key or context.get("sessionId", "")
        user_id = context.get("userId", "")
        history = context.get("history", [])
        latest_input = context.get("latestInput", {})
        memoir_context = context.get("memoirContext", "")

        # Build system prompt
        system_prompt = self._build_system_prompt(memoir_context, context)

        # Convert messages to Claude API format
        messages = self._to_claude_messages(history, latest_input)

        # Call Claude API
        response = self._call_claude(system_prompt, messages)

        # Parse response into ThinkResponse
        think_response = self._parse_response(response, session_id, user_id, latest_input)

        # Produce to output topic
        self._producer.produce(
            topic=self._config.output_topic,
            key=session_id,
            value=json.dumps(think_response),
            callback=lambda err, msg: (
                logger.error("Failed to produce response: %s", err) if err else None
            ),
        )
        self._producer.flush()

        # Store offset for auto-commit
        tp = TopicPartition(topic, partition, offset + 1)
        self._consumer.store_offsets(offsets=[tp])
        logger.debug("Stored offset %d for %s/%d", offset + 1, topic, partition)

    def _build_system_prompt(self, memoir_context: str, full_context: dict) -> str:
        if self._config.system_prompt_builder:
            prompt = self._config.system_prompt_builder(self._config.system_prompt, full_context)
        else:
            prompt = self._config.system_prompt

        if memoir_context:
            prompt += (
                "\n\nUser memoir (known facts about this user from previous sessions):\n"
                f"{memoir_context}\n"
                "Use the memoir to personalize your responses."
            )

        return prompt

    def _to_claude_messages(self, history: list[dict], latest_input: dict) -> list[dict]:
        messages: list[dict] = []

        for msg in history:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if role == "tool":
                # Convert tool results to user role with tool_result blocks
                tool_results = content if isinstance(content, list) else [content]
                blocks = []
                for result in tool_results:
                    if isinstance(result, dict) and "tool_use_id" in result:
                        blocks.append({
                            "type": "tool_result",
                            "tool_use_id": result["tool_use_id"],
                            "content": result.get("content", ""),
                        })
                    else:
                        blocks.append({"type": "text", "text": str(result)})
                messages.append({"role": "user", "content": blocks})
            elif role == "assistant":
                # Preserve structured content blocks (tool_use IDs)
                if isinstance(content, list):
                    messages.append({"role": "assistant", "content": content})
                else:
                    self._append_or_merge(messages, "assistant", str(content))
            else:
                self._append_or_merge(messages, "user", str(content))

        # Add latest input
        if latest_input:
            content = latest_input.get("content", "")
            self._append_or_merge(messages, "user", str(content))

        return messages

    def _append_or_merge(self, messages: list[dict], role: str, text: str) -> None:
        """Merge consecutive same-role text messages."""
        if messages and messages[-1]["role"] == role and isinstance(messages[-1]["content"], str):
            messages[-1]["content"] += "\n" + text
        else:
            messages.append({"role": role, "content": text})

    def _call_claude(self, system_prompt: str, messages: list[dict]) -> dict:
        body: dict[str, Any] = {
            "model": self._config.claude_model,
            "max_tokens": self._config.claude_max_tokens,
            "system": system_prompt,
            "messages": messages,
        }

        if self._config.tools:
            body["tools"] = self._config.tools

        data = json.dumps(body).encode()

        req = urllib.request.Request(
            self._config.claude_api_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "x-api-key": self._config.claude_api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            logger.error("Claude API error %d: %s", e.code, error_body)
            raise RuntimeError(f"Claude API error {e.code}: {error_body}") from e

    def _parse_response(self, response: dict, session_id: str, user_id: str, latest_input: dict) -> dict:
        usage = response.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        cost = (input_tokens / 1_000_000 * self.INPUT_TOKEN_PRICE + output_tokens / 1_000_000 * self.OUTPUT_TOKEN_PRICE) \
               if self.INPUT_TOKEN_PRICE is not None and self.OUTPUT_TOKEN_PRICE is not None \
               else None

        stop_reason = response.get("stop_reason", "end_turn")
        end_turn = stop_reason != "tool_use"

        content_blocks = response.get("content", [])
        messages: list[dict] = []
        tool_uses: list[dict] = []

        # Prepend latest input for downstream request-response pairing
        if latest_input:
            messages.append(latest_input)

        has_tool_use = any(b.get("type") == "tool_use" for b in content_blocks)

        if has_tool_use:
            # Store full structured content to preserve tool_use IDs
            assistant_msg = {
                "sessionId": session_id,
                "userId": user_id,
                "role": "assistant",
                "content": content_blocks,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            messages.append(assistant_msg)

        for block in content_blocks:
            block_type = block.get("type", "")

            if block_type == "text":
                if not has_tool_use:
                    messages.append({
                        "sessionId": session_id,
                        "userId": user_id,
                        "role": "assistant",
                        "content": block.get("text", ""),
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    })

            elif block_type == "tool_use":
                tool_uses.append({
                    "toolUseId": block.get("id", ""),
                    "toolId": block.get("name", ""),
                    "name": block.get("name", ""),
                    "input": block.get("input", {}),
                    "sessionId": session_id,
                    "totalTools": sum(1 for b in content_blocks if b.get("type") == "tool_use"),
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                })

        return {
            "sessionId": session_id,
            "userId": user_id,
            "cost": round(cost, 6) if cost is not None else None,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "messages": messages,
            "toolUses": tool_uses,
            "endTurn": end_turn,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def _send_to_dlq(self, key: str | None, value: str | None, reason: str, topic: str, partition: int, offset: int) -> None:
        headers = [
            ("error.reason", reason.encode()),
            ("error.source.topic", topic.encode()),
            ("error.source.partition", str(partition).encode()),
            ("error.source.offset", str(offset).encode()),
        ]

        self._producer.produce(
            topic=self._config.dlq_topic,
            key=key,
            value=value,
            headers=headers,
        )
        self._producer.flush()

        tp = TopicPartition(topic, partition, offset + 1)
        self._consumer.store_offsets(offsets=[tp])
