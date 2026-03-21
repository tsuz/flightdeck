"""
Example: Think consumer using the flightdeck SDK.

The developer provides a system prompt and tools — the framework handles
Claude API calls, message formatting, and Kafka plumbing.
"""
from flightdeck_sdk import ThinkConsumerRunner, ThinkConsumerConfig

SYSTEM_PROMPT = """You are a helpful order management assistant.
You can look up orders and check shipping status for customers."""

TOOLS = [
    {
        "name": "lookup_order",
        "description": "Look up an order by order ID",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "string",
                    "description": "The order ID to look up",
                }
            },
            "required": ["order_id"],
        },
    },
    {
        "name": "check_shipping",
        "description": "Check the shipping status for an order",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "string",
                    "description": "The order ID to check shipping for",
                }
            },
            "required": ["order_id"],
        },
    },
]

if __name__ == "__main__":
    runner = ThinkConsumerRunner(
        ThinkConsumerConfig(
            agent_name="order-agent",
            brokers="localhost:9092",
            claude_api_key="sk-ant-...",
            system_prompt=SYSTEM_PROMPT,
            tools=TOOLS,
        )
    )

    runner.start()
