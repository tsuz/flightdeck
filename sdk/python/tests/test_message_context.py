import json
import pytest
from unittest.mock import MagicMock
from flightdeck_sdk.message_context import KafkaMessageContext


def make_ctx(
    tool_use_id="toolu_123",
    key="session-1",
    value='{"tool_use_id":"toolu_123","name":"lookup","input":{},"total_tools":1,"session_id":"session-1"}',
    topic="agent-tool-use",
    partition=2,
    offset=42,
    incoming=None,
):
    consumer = MagicMock()
    output_producer = MagicMock()
    dlq_producer = MagicMock()

    if incoming is None:
        try:
            incoming = json.loads(value) if value else {}
        except (json.JSONDecodeError, TypeError):
            incoming = {}

    ctx = KafkaMessageContext(
        consumer=consumer,
        output_producer=output_producer,
        dlq_producer=dlq_producer,
        output_topic="agent-tool-use-result",
        dlq_topic="agent-tool-use-dlq",
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        tool_use_id=tool_use_id,
        incoming=incoming,
    )
    return ctx, consumer, output_producer, dlq_producer


class TestSuccess:
    def test_produces_correct_payload_and_commits(self):
        ctx, consumer, producer, _ = make_ctx()

        ctx.success({"order_id": "123", "status": "shipped"})

        # Produced to the right topic with correct payload
        producer.produce.assert_called_once()
        kwargs = producer.produce.call_args.kwargs
        assert kwargs["topic"] == "agent-tool-use-result"
        assert kwargs["key"] == "session-1"
        payload = json.loads(kwargs["value"])
        assert payload["tool_use_id"] == "toolu_123"
        assert payload["result"] == {"order_id": "123", "status": "shipped"}
        assert payload["status"] == "success"
        assert payload["name"] == "lookup"
        assert payload["session_id"] == "session-1"
        assert payload["total_tools"] == 1
        assert "latency_ms" in payload
        assert "timestamp" in payload

        # Flushed and settled
        producer.flush.assert_called_once()
        assert ctx.settled is True

        # Delivery callback stores offset at offset+1 for auto-commit
        callback = kwargs["callback"]
        callback(None, MagicMock())
        store_kwargs = consumer.store_offsets.call_args.kwargs
        tp = store_kwargs["offsets"][0]
        assert tp.topic == "agent-tool-use"
        assert tp.partition == 2
        assert tp.offset == 43

    def test_delivery_failure_skips_store(self):
        ctx, consumer, producer, _ = make_ctx()

        ctx.success("done")

        callback = producer.produce.call_args.kwargs["callback"]
        callback("Broker down", None)

        consumer.store_offsets.assert_not_called()


class TestError:
    def test_sends_to_dlq_with_headers_and_commits(self):
        ctx, consumer, _, dlq_producer = make_ctx(
            key="sess-5", value='{"original":"msg"}', topic="my-topic", partition=3, offset=77
        )

        ctx.error("parse failed")

        # Produced to DLQ with original message and error headers
        dlq_producer.produce.assert_called_once()
        kwargs = dlq_producer.produce.call_args.kwargs
        assert kwargs["topic"] == "agent-tool-use-dlq"
        assert kwargs["key"] == "sess-5"
        assert kwargs["value"] == '{"original":"msg"}'
        header_dict = {k: v for k, v in kwargs["headers"]}
        assert header_dict["error.reason"] == b"parse failed"
        assert header_dict["error.source.topic"] == b"my-topic"
        assert header_dict["error.source.partition"] == b"3"
        assert header_dict["error.source.offset"] == b"77"

        # Flushed and settled
        dlq_producer.flush.assert_called_once()
        assert ctx.settled is True

        # Delivery callback stores offset at offset+1 for auto-commit
        callback = kwargs["callback"]
        callback(None, MagicMock())
        tp = consumer.store_offsets.call_args.kwargs["offsets"][0]
        assert tp.offset == 78

    def test_delivery_failure_skips_store(self):
        ctx, consumer, _, dlq_producer = make_ctx()

        ctx.error("fail")

        callback = dlq_producer.produce.call_args.kwargs["callback"]
        callback("DLQ broker down", None)

        consumer.store_offsets.assert_not_called()


class TestDoubleSettle:
    def test_cannot_settle_twice(self):
        """Any combination of success/error after the first call raises and does not produce again."""
        ctx, _, producer, dlq_producer = make_ctx()

        ctx.success("first")

        with pytest.raises(RuntimeError, match="already settled"):
            ctx.success("second")

        with pytest.raises(RuntimeError, match="already settled"):
            ctx.error("nope")

        # Only the original success produced anything
        assert producer.produce.call_count == 1
        assert dlq_producer.produce.call_count == 0

    def test_error_then_any_raises(self):
        ctx, _, producer, dlq_producer = make_ctx()

        ctx.error("first")

        with pytest.raises(RuntimeError, match="already settled"):
            ctx.error("second")

        with pytest.raises(RuntimeError, match="already settled"):
            ctx.success("nope")

        assert dlq_producer.produce.call_count == 1
        assert producer.produce.call_count == 0
