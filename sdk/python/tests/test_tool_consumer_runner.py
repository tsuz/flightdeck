"""
Tests that the ToolConsumerRunner correctly parses incoming Kafka messages
and wires tool_use_id through to the context's success() output.

This is an integration-level test of the runner — it simulates the full
poll→parse→process→produce path with mocked Kafka, catching bugs like
reading the wrong field name from the message.
"""

import json
from unittest.mock import MagicMock, patch
from flightdeck_sdk.tool_consumer_runner import ToolConsumerRunner, ToolConsumerConfig


def make_kafka_message(value: dict, key: str = "session-1"):
    msg = MagicMock()
    msg.key.return_value = key.encode()
    msg.value.return_value = json.dumps(value).encode()
    msg.topic.return_value = "test-tool-use"
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    msg.error.return_value = None
    return msg


class TestToolUseIdParsing:
    def test_tool_use_id_from_real_message_flows_to_output(self):
        """
        Simulates a real Kafka message with tool_use_id and verifies
        the runner parses it and the output payload includes it.
        """
        incoming = {
            "tool_use_id": "toolu_01JDYBfV5Fuy1vKvtkagSJF2",
            "tool_id": "search_leads",
            "name": "search_leads",
            "input": {"limit": 15},
            "session_id": "session-123",
            "total_tools": 1,
        }

        captured_ctx = {}

        def process_fn(key, value, ctx):
            captured_ctx["ctx"] = ctx
            ctx.success({"leads": [], "count": 0})

        with patch("flightdeck_sdk.tool_consumer_runner.Consumer") as MockConsumer, \
             patch("flightdeck_sdk.tool_consumer_runner.Producer") as MockProducer:

            mock_consumer = MockConsumer.return_value
            mock_producer = MockProducer.return_value

            # Simulate: first poll returns our message, second poll triggers stop
            msg = make_kafka_message(incoming)
            call_count = 0

            def fake_poll(timeout):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return msg
                runner.stop()
                return None

            mock_consumer.poll.side_effect = fake_poll

            runner = ToolConsumerRunner(
                ToolConsumerConfig(
                    agent_name="test",
                    brokers="localhost:9092",
                    process_fn=process_fn,
                )
            )
            runner.start()

        # Verify the full output payload matches downstream expectations
        produce_kwargs = mock_producer.produce.call_args.kwargs
        payload = json.loads(produce_kwargs["value"])
        assert payload["tool_use_id"] == "toolu_01JDYBfV5Fuy1vKvtkagSJF2"
        assert payload["result"] == {"leads": [], "count": 0}
        assert payload["name"] == "search_leads"
        assert payload["session_id"] == "session-123"
        assert payload["status"] == "success"
        assert payload["total_tools"] == 1
        assert "latency_ms" in payload
        assert "timestamp" in payload

    def test_missing_tool_use_id_defaults_to_empty(self):
        """Messages without tool_use_id should not crash — just empty string."""
        incoming = {"name": "some_tool", "input": {}}

        def process_fn(key, value, ctx):
            ctx.success("ok")

        with patch("flightdeck_sdk.tool_consumer_runner.Consumer") as MockConsumer, \
             patch("flightdeck_sdk.tool_consumer_runner.Producer") as MockProducer:

            mock_consumer = MockConsumer.return_value
            mock_producer = MockProducer.return_value

            msg = make_kafka_message(incoming)
            call_count = 0

            def fake_poll(timeout):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return msg
                runner.stop()
                return None

            mock_consumer.poll.side_effect = fake_poll

            runner = ToolConsumerRunner(
                ToolConsumerConfig(
                    agent_name="test",
                    brokers="localhost:9092",
                    process_fn=process_fn,
                )
            )
            runner.start()

        payload = json.loads(mock_producer.produce.call_args.kwargs["value"])
        assert payload["tool_use_id"] == ""
        assert payload["status"] == "success"
