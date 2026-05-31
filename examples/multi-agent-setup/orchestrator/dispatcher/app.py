"""
Orchestrator dispatcher — the asynchronous multi-agent tool.

This is the Agent-A side of the multi-agent design. When the orchestrator LLM
calls `delegate_task`, the tool does NOT answer synchronously. Instead it:

  1. Mints an HMAC callback token correlating this exact tool call
     (session_id, tool_use_id, tool_id, name, total_tools). Only the
     orchestrator holds the secret, so only the orchestrator can verify it — the
     worker is a blind courier that just echoes the token back.

  2. POSTs the task to the worker agent's /api/chat with a transport-level
     `reply` descriptor naming the callback service to answer to (a logical name
     the worker resolves to the orchestrator's URL via ALLOWED_HOST_MAPPING),
     carrying the token as a bearer credential. The dispatcher never sends a URL,
     and the reply descriptor never enters the worker's prompt.

  3. Commits the tool-use offset WITHOUT producing a tool-use-result. This is the
     "pending" ack: the dispatch is acknowledged so it is not redelivered, but no
     result is published — the result will arrive later via the callback.

We use a plain Kafka consumer here (rather than the SDK's ToolConsumerRunner) so
we have explicit control over the "ack-without-result" semantics. A future SDK
`ctx.pending()` would replace this loop.

The worker processes the task as an ordinary chat. When it finishes, the worker's
chat-api OutputConsumer POSTs the answer back to the orchestrator's
/api/tools/response, which verifies the token and writes the tool-use-result —
completing the original tool call. If the worker never responds, the
orchestrator's aggregator times out and synthesizes an error result, so the turn
cannot hang.

Env vars:
    KAFKA_BOOTSTRAP_SERVERS  Kafka broker
    AGENT_NAME               orchestrator agent name (prefixes its topics)
    TOOL_CALLBACK_SECRET     shared HMAC secret (same as orchestrator chat-api)
    TARGET_CHAT_URL          worker /api/chat URL, e.g. http://worker-api:8000/api/chat
    CALLBACK_SERVICE         logical callback-service name the worker maps to the
                             orchestrator's URL via ALLOWED_HOST_MAPPING, e.g. "orchestrator"
    TOKEN_TTL_SEC            callback token lifetime in seconds (default 86400)
"""

import base64
import hashlib
import hmac
import json
import os
import time

import requests
from confluent_kafka import Consumer

from flightdeck_sdk import kafka_env_props

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
AGENT_NAME = os.environ["AGENT_NAME"]
SECRET = os.environ["TOOL_CALLBACK_SECRET"]
TARGET_CHAT_URL = os.environ["TARGET_CHAT_URL"]
CALLBACK_SERVICE = os.environ["CALLBACK_SERVICE"]
TOKEN_TTL_SEC = int(os.environ.get("TOKEN_TTL_SEC", "86400"))

INPUT_TOPIC = f"{AGENT_NAME}-tool-use"
GROUP_ID = f"{AGENT_NAME}-tool-execution"


def _b64url(raw: bytes) -> str:
    """base64url without padding — matches the FlightDeck token format."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def mint_token(session_id, tool_use_id, tool_id, name, total_tools):
    now = int(time.time())
    payload = {
        "session_id": session_id,
        "tool_use_id": tool_use_id,
        "tool_id": tool_id,
        "name": name,
        "total_tools": total_tools,
        "agent": AGENT_NAME,
        "iat": now,
        "exp": now + TOKEN_TTL_SEC,
    }
    # The HMAC is computed over the exact transmitted payload bytes, so the
    # verifier never re-serializes — JSON key ordering is irrelevant.
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    sig = hmac.new(SECRET.encode(), payload_bytes, hashlib.sha256).digest()
    return f"{_b64url(payload_bytes)}.{_b64url(sig)}"


def dispatch(tool_use):
    session_id = tool_use.get("session_id", "")
    tool_use_id = tool_use.get("tool_use_id", "")
    tool_id = tool_use.get("tool_id", "")
    name = tool_use.get("name", "")
    total_tools = tool_use.get("total_tools", 1)
    task = (tool_use.get("input") or {}).get("task", "")

    token = mint_token(session_id, tool_use_id, tool_id, name, total_tools)

    # A fresh session for the worker's sub-conversation. The worker keys its
    # reply-to state by THIS id; the orchestrator-side correlation lives in the
    # token (which carries the orchestrator session_id + tool_use_id), not here.
    worker_session = f"{session_id}--{tool_use_id}"

    body = {
        "session_id": worker_session,
        "content": task,
        "reply": {
            "callbackService": CALLBACK_SERVICE,
            "bearerToken": token,
        },
    }

    print(f"[{session_id}] delegate tool_use_id={tool_use_id} → worker "
          f"session={worker_session}: {task[:80]!r}", flush=True)
    resp = requests.post(TARGET_CHAT_URL, json=body, timeout=15)
    resp.raise_for_status()
    print(f"[{session_id}] worker accepted task (HTTP {resp.status_code}); "
          f"awaiting async callback", flush=True)


def main():
    consumer = Consumer({
        **kafka_env_props(),
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,  # we store offsets explicitly after acking
    })
    consumer.subscribe([INPUT_TOPIC])
    print(f"Orchestrator dispatcher started — listening on [{INPUT_TOPIC}]", flush=True)
    print(f"  target chat : {TARGET_CHAT_URL}", flush=True)
    print(f"  callback svc: {CALLBACK_SERVICE} (resolved by worker via ALLOWED_HOST_MAPPING)", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", flush=True)
                continue
            try:
                dispatch(json.loads(msg.value()))
            except Exception as e:
                # We still ack on failure: the orchestrator aggregator will time
                # out and synthesize an error result, so the turn cannot hang.
                # (Prefer NOT acking if you'd rather redeliver on transient errors.)
                print(f"dispatch failed: {e}", flush=True)
            # "pending" ack: record the source offset, publish NO tool-use-result.
            consumer.store_offsets(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
