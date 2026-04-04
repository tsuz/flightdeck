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
    llm_provider: str = "claude"
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"
    gemini_max_tokens: int = 4096
    gemini_api_url: str = "https://generativelanguage.googleapis.com/v1beta"
    compaction_user_message_trigger: int = -1
    compaction_user_message_until: int = 2
    compaction_prompt: str = (
        "Summarize the following conversation concisely. "
        "If the conversation starts with a previous summary, incorporate and extend it "
        "rather than re-summarizing it. "
        "Preserve key facts, decisions, user preferences, and any context needed "
        "to continue the conversation naturally. Output only the summary."
    )

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

        provider = config.llm_provider.lower()
        if provider == "gemini":
            if not config.gemini_api_key:
                raise ValueError("gemini_api_key is required when llm_provider='gemini'")
            logger.info("Using Gemini LLM provider (model=%s)", config.gemini_model)
        else:
            if not config.claude_api_key:
                raise ValueError("claude_api_key is required when llm_provider='claude'")
            logger.info("Using Claude LLM provider (model=%s)", config.claude_model)

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
                self._emit_error_response(key, value, str(e), msg.topic(), msg.partition(), msg.offset())
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
        cumulative_cost = context.get("cost")

        # Check session budget
        budget_str = os.environ.get("BUDGET_PRICE_PER_SESSION")
        if budget_str and cumulative_cost is not None:
            budget = float(budget_str)
            if cumulative_cost >= budget:
                logger.warning(
                    "[%s] Session budget exceeded: $%.6f >= $%.2f",
                    session_id, cumulative_cost, budget,
                )
                budget_response = {
                    "sessionId": session_id,
                    "userId": user_id,
                    "cost": None,
                    "prevSessionCost": cumulative_cost,
                    "inputTokens": 0,
                    "outputTokens": 0,
                    "previousMessages": history,
                    "lastInputMessage": latest_input,
                    "lastInputResponse": [
                        {
                            "sessionId": session_id,
                            "userId": user_id,
                            "role": "assistant",
                            "content": f"You have used too many tokens. Session budget of ${budget:.2f} has been reached.",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        }
                    ],
                    "toolUses": [],
                    "endTurn": True,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                self._producer.produce(
                    topic=self._config.output_topic,
                    key=session_id,
                    value=json.dumps(budget_response),
                )
                self._producer.flush()
                tp = TopicPartition(topic, partition, offset + 1)
                self._consumer.store_offsets(offsets=[tp])
                return

        # Compact history if user message count exceeds trigger
        effective_history = history
        compacted_history = None
        trigger = self._config.compaction_user_message_trigger
        keep_last = self._config.compaction_user_message_until

        if trigger > 0 and effective_history:
            user_msg_count = sum(1 for m in effective_history if m.get("role") == "user")
            if user_msg_count >= trigger:
                split_idx = self._find_compaction_split_index(effective_history, keep_last)
                if split_idx > 0:
                    recent_messages = effective_history[split_idx:]
                    # Skip compaction if we're in an active tool loop
                    # (latest_input is a tool result)
                    mid_tool_loop = latest_input.get("role") == "tool" if latest_input else False
                    if not mid_tool_loop:
                        logger.info(
                            "[%s] Compacting history: %d user messages >= trigger %d, keeping from index %d",
                            session_id, user_msg_count, trigger, split_idx,
                        )
                        old_messages = effective_history[:split_idx]

                        provider = self._config.llm_provider.lower()
                        if provider == "gemini":
                            summary_input = self._to_gemini_messages(old_messages, {})
                            summary_resp = self._call_gemini(self._config.compaction_prompt, summary_input, include_tools=False)
                            summary_text = self._extract_gemini_text(summary_resp)
                        else:
                            summary_input = self._to_claude_messages(old_messages, {})
                            summary_resp = self._call_claude(self._config.compaction_prompt, summary_input, include_tools=False)
                            summary_text = self._extract_claude_text(summary_resp)

                        summary_msg = {
                            "sessionId": session_id,
                            "userId": user_id,
                            "role": "assistant",
                            "content": f"[Conversation Summary]\n{summary_text}",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        }
                        compacted_history = [summary_msg] + recent_messages
                        effective_history = compacted_history
                        logger.info(
                            "[%s] History compacted: %d messages → %d",
                            session_id, len(history), len(compacted_history),
                        )

        # Build system prompt
        system_prompt = self._build_system_prompt(memoir_context, context)

        provider = self._config.llm_provider.lower()

        if provider == "gemini":
            messages = self._to_gemini_messages(effective_history, latest_input)
            response = self._call_gemini(system_prompt, messages)
            think_response = self._parse_gemini_response(response, session_id, user_id, latest_input, effective_history)
        else:
            messages = self._to_claude_messages(effective_history, latest_input)
            response = self._call_claude(system_prompt, messages)
            think_response = self._parse_response(response, session_id, user_id, latest_input)
        think_response["prevSessionCost"] = cumulative_cost
        think_response["previousMessages"] = effective_history
        think_response["lastInputMessage"] = latest_input
        think_response["lastInputResponse"] = think_response.pop("messages", [])

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

    def _call_claude(self, system_prompt: str, messages: list[dict], *, include_tools: bool = True) -> dict:
        body: dict[str, Any] = {
            "model": self._config.claude_model,
            "max_tokens": self._config.claude_max_tokens,
            "system": system_prompt,
            "messages": messages,
        }

        if include_tools and self._config.tools:
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

    # ── Gemini helpers ────────────────────────────────────────────────────

    def _to_gemini_messages(self, history: list[dict], latest_input: dict) -> list[dict]:
        """Convert internal history to Gemini API contents format."""
        # Build tool_use_id → name mapping from assistant messages
        tool_id_to_name: dict[str, str] = {}
        for msg in history:
            if msg.get("role") == "assistant" and isinstance(msg.get("content"), list):
                for block in msg["content"]:
                    if isinstance(block, dict) and block.get("type") == "tool_use":
                        tid = block.get("id", "")
                        tname = block.get("name", "")
                        if tid and tname:
                            tool_id_to_name[tid] = tname

        contents: list[dict] = []

        for msg in history:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            if role == "tool":
                # Convert tool results to functionResponse parts
                parts = self._build_function_response_parts(content, tool_id_to_name)
                if parts:
                    contents.append({"role": "user", "parts": parts})
            elif role == "assistant":
                if isinstance(content, list):
                    # Structured content blocks — convert tool_use to functionCall
                    parts = []
                    for block in content:
                        if isinstance(block, dict):
                            if block.get("type") == "text":
                                text = block.get("text", "")
                                if text:
                                    parts.append({"text": text})
                            elif block.get("type") == "tool_use":
                                parts.append({"functionCall": {
                                    "name": block.get("name", ""),
                                    "args": block.get("input", {}),
                                }})
                    if parts:
                        contents.append({"role": "model", "parts": parts})
                else:
                    self._append_or_merge_gemini(contents, "model", str(content))
            else:
                self._append_or_merge_gemini(contents, "user", str(content))

        # Add latest input
        if latest_input:
            content = latest_input.get("content", "")
            self._append_or_merge_gemini(contents, "user", str(content))

        return contents

    def _append_or_merge_gemini(self, contents: list[dict], role: str, text: str) -> None:
        """Merge consecutive same-role text messages for Gemini."""
        if contents and contents[-1]["role"] == role:
            last_parts = contents[-1].get("parts", [])
            if last_parts and "text" in last_parts[-1]:
                last_parts.append({"text": text})
                return
        contents.append({"role": role, "parts": [{"text": text}]})

    def _build_function_response_parts(self, content: Any, tool_id_to_name: dict[str, str]) -> list[dict]:
        """Convert tool results to Gemini functionResponse parts."""
        parts: list[dict] = []
        results = content if isinstance(content, list) else [content]
        for result in results:
            if isinstance(result, dict) and "tool_use_id" in result:
                name = tool_id_to_name.get(result["tool_use_id"], "unknown")
                res_data = result.get("result", result.get("content", "{}"))
                if isinstance(res_data, str):
                    try:
                        res_data = json.loads(res_data)
                    except (json.JSONDecodeError, TypeError):
                        res_data = {"result": res_data}
                parts.append({"functionResponse": {"name": name, "response": res_data}})
        return parts

    def _call_gemini(self, system_prompt: str, contents: list[dict], *, include_tools: bool = True) -> dict:
        body: dict[str, Any] = {
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": contents,
            "generationConfig": {"maxOutputTokens": self._config.gemini_max_tokens},
        }

        if include_tools and self._config.tools:
            func_decls = []
            for tool in self._config.tools:
                decl: dict[str, Any] = {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                }
                if "input_schema" in tool:
                    decl["parameters"] = tool["input_schema"]
                func_decls.append(decl)
            body["tools"] = [{"function_declarations": func_decls}]

        data = json.dumps(body).encode()
        url = f"{self._config.gemini_api_url}/models/{self._config.gemini_model}:generateContent?key={self._config.gemini_api_key}"

        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            logger.error("Gemini API error %d: %s", e.code, error_body)
            raise RuntimeError(f"Gemini API error {e.code}: {error_body}") from e

    def _parse_gemini_response(self, response: dict, session_id: str, user_id: str,
                               latest_input: dict, history: list[dict]) -> dict:
        import uuid

        usage = response.get("usageMetadata", {})
        input_tokens = usage.get("promptTokenCount", 0)
        output_tokens = usage.get("candidatesTokenCount", 0)
        cost = (input_tokens / 1_000_000 * self.INPUT_TOKEN_PRICE + output_tokens / 1_000_000 * self.OUTPUT_TOKEN_PRICE) \
               if self.INPUT_TOKEN_PRICE is not None and self.OUTPUT_TOKEN_PRICE is not None \
               else None

        candidates = response.get("candidates", [])
        if not candidates:
            raise RuntimeError("No candidates in Gemini response")

        candidate = candidates[0]
        parts = candidate.get("content", {}).get("parts", [])

        has_function_call = any("functionCall" in p for p in parts)
        end_turn = not has_function_call

        messages: list[dict] = []
        tool_uses: list[dict] = []
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Build content blocks in Claude-compatible format for history preservation
        content_blocks: list[dict] = []

        for part in parts:
            if "text" in part:
                content_blocks.append({"type": "text", "text": part["text"]})
            elif "functionCall" in part:
                fc = part["functionCall"]
                tool_use_id = "toolu_" + uuid.uuid4().hex[:20]
                content_blocks.append({
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": fc.get("name", ""),
                    "input": fc.get("args", {}),
                })
                tool_uses.append({
                    "toolUseId": tool_use_id,
                    "toolId": fc.get("name", ""),
                    "name": fc.get("name", ""),
                    "input": fc.get("args", {}),
                    "sessionId": session_id,
                    "totalTools": sum(1 for p in parts if "functionCall" in p),
                    "timestamp": now,
                })

        if has_function_call:
            messages.append({
                "sessionId": session_id,
                "userId": user_id,
                "role": "assistant",
                "content": content_blocks,
                "timestamp": now,
            })
        else:
            for block in content_blocks:
                if block.get("type") == "text":
                    messages.append({
                        "sessionId": session_id,
                        "userId": user_id,
                        "role": "assistant",
                        "content": block.get("text", ""),
                        "timestamp": now,
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
            "timestamp": now,
        }

    @staticmethod
    def _find_compaction_split_index(history: list[dict], keep_last: int) -> int:
        """Find the index where to split history for compaction.
        Everything before this index is summarized; from this index onward is kept.
        Returns -1 if nothing to compact."""
        if not history or keep_last <= 0:
            return -1
        total_user = sum(1 for m in history if m.get("role") == "user")
        if total_user <= keep_last:
            return -1
        target = total_user - keep_last
        seen = 0
        for i, m in enumerate(history):
            if m.get("role") == "user":
                seen += 1
                if seen > target:
                    return i
        return -1

    @staticmethod
    def _has_tool_use_content(msg: dict) -> bool:
        content = msg.get("content")
        if isinstance(content, list):
            return any(
                isinstance(b, dict) and b.get("type") == "tool_use"
                for b in content
            )
        return False

    @staticmethod
    def _extract_claude_text(response: dict) -> str:
        blocks = response.get("content", [])
        return "\n".join(
            b.get("text", "") for b in blocks if b.get("type") == "text"
        )

    @staticmethod
    def _extract_gemini_text(response: dict) -> str:
        candidates = response.get("candidates", [])
        if not candidates:
            return ""
        parts = candidates[0].get("content", {}).get("parts", [])
        return "\n".join(p.get("text", "") for p in parts if "text" in p)

    def _emit_error_response(self, key: str | None, value: str | None, reason: str,
                               topic: str, partition: int, offset: int) -> None:
        """Emit an error response to the output topic so the user sees the failure."""
        try:
            session_id = key or ""
            user_id = ""
            if value:
                try:
                    ctx = json.loads(value)
                    user_id = ctx.get("userId", "")
                except (json.JSONDecodeError, TypeError):
                    pass

            error_response = {
                "sessionId": session_id,
                "userId": user_id,
                "cost": None,
                "prevSessionCost": None,
                "inputTokens": 0,
                "outputTokens": 0,
                "messages": [
                    {
                        "sessionId": session_id,
                        "userId": user_id,
                        "role": "assistant",
                        "content": f"Sorry, an error occurred while processing your request: {reason}",
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }
                ],
                "toolUses": [],
                "endTurn": True,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            self._producer.produce(
                topic=self._config.output_topic,
                key=session_id,
                value=json.dumps(error_response),
            )
            self._producer.flush()
            logger.info("[%s] Emitted error response to %s", session_id, self._config.output_topic)
        except Exception as ex:
            logger.error("Failed to emit error response: %s", ex)

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
