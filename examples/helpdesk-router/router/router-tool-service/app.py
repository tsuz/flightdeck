"""
Router tool consumer — bridges the router agent to specialist agents.
Uses the FlightDeck SDK for the tool consumer loop.

When the router calls route_to(specialist, question), this service:
1. Maps the specialist short name (it/hr/facilities/finance) to the full agent name
2. Produces the question to that agent's message-input topic
3. Consumes the response from that agent's message-output topic
4. Returns the response as the tool result

Env vars:
    KAFKA_BOOTSTRAP_SERVERS  — Kafka broker
    AGENT_NAME               — router agent name
    SPECIALIST_MAP           — comma-separated short=full pairs, e.g. "it=it-agent,hr=hr-agent"
    RESPONSE_TIMEOUT_SECONDS — how long to wait for agent responses (default: 120)
"""

import json
import os
import uuid
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
AGENT_NAME = os.environ["AGENT_NAME"]
SPECIALIST_MAP_RAW = os.environ.get(
    "SPECIALIST_MAP",
    "it=it-agent,hr=hr-agent,facilities=facilities-agent,finance=finance-agent",
)
RESPONSE_TIMEOUT_SECONDS = int(os.environ.get("RESPONSE_TIMEOUT_SECONDS", "120"))

SPECIALIST_MAP = dict(
    pair.split("=", 1) for pair in SPECIALIST_MAP_RAW.split(",") if "=" in pair
)

agent_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})


def send_to_agent(agent_name, question, session_id):
    """Produce a message to an agent's input topic and wait for its response."""
    input_topic = f"{agent_name}-message-input"
    output_topic = f"{agent_name}-message-output"

    agent_session = f"router-{session_id}-{agent_name}-{uuid.uuid4().hex[:8]}"

    message = {
        "session_id": agent_session,
        "user_id": "router",
        "role": "user",
        "content": question,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {},
    }

    print(f"[{session_id}] Routing to {agent_name} (session={agent_session}): {question[:80]}...")

    agent_producer.produce(
        input_topic,
        key=agent_session.encode(),
        value=json.dumps(message).encode(),
    )
    agent_producer.flush()

    return wait_for_response(output_topic, agent_session, agent_name)


def wait_for_response(topic, session_id, agent_name):
    """Poll an agent's output topic for a response matching our session ID."""
    resp_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"router-resp-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    resp_consumer.subscribe([topic])

    deadline = time.time() + RESPONSE_TIMEOUT_SECONDS
    print(f"[{session_id}] Waiting for response from {agent_name} on {topic} (timeout={RESPONSE_TIMEOUT_SECONDS}s)")

    try:
        while time.time() < deadline:
            msg = resp_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            key = msg.key().decode() if msg.key() else ""
            if key != session_id:
                continue

            value = json.loads(msg.value())
            content = value.get("content", value.get("text", json.dumps(value)))
            print(f"[{session_id}] Got response from {agent_name} ({len(str(content))} chars)")
            return {"specialist": agent_name, "response": content, "status": "success"}

        print(f"[{session_id}] Timeout waiting for {agent_name}")
        return {"specialist": agent_name, "response": None, "status": "timeout"}

    finally:
        resp_consumer.close()


def route_to(input_data, session_id="direct"):
    specialist = input_data.get("specialist", "")
    question = input_data.get("question", "")

    if specialist not in SPECIALIST_MAP:
        return {
            "error": f"Unknown specialist: {specialist}. Available: {list(SPECIALIST_MAP.keys())}"
        }
    if not question:
        return {"error": "Missing 'question' parameter"}

    agent_name = SPECIALIST_MAP[specialist]
    return send_to_agent(agent_name, question, session_id)


TOOLS = {
    "route_to": route_to,
}


def process(key, value, ctx):
    request = json.loads(value)
    tool_name = request.get("name", "")
    session_id = key or "unknown"

    handler = TOOLS.get(tool_name)
    if handler is None:
        ctx.error(f"Unknown tool: {tool_name}")
        return

    try:
        result = handler(request.get("input", {}), session_id=session_id)
        ctx.success(result)
    except Exception as e:
        ctx.error(str(e))


if __name__ == "__main__":
    print(f"Router Tool Service")
    print(f"  Specialist map:   {SPECIALIST_MAP}")
    print(f"  Response timeout: {RESPONSE_TIMEOUT_SECONDS}s")

    runner = ToolConsumerRunner(
        ToolConsumerConfig(
            agent_name=AGENT_NAME,
            brokers=KAFKA_BOOTSTRAP,
            process_fn=process,
        )
    )
    runner.start()
