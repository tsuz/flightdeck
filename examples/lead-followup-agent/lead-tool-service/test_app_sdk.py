"""
Tests that the lead-followup-agent tool functions work correctly
through the flightdeck SDK's success()/error() flow.

No Kafka required — we mock the consumer/producers and invoke
the process function directly, just like the SDK runner would.
"""

import json
import pytest
from unittest.mock import MagicMock
from flightdeck_sdk.message_context import KafkaMessageContext

# Import the tool functions and process handler from the SDK-based app
from app import process, LEADS


def make_ctx(tool_use_id="toolu_test", key="session-1", value="{}"):
    consumer = MagicMock()
    output_producer = MagicMock()
    dlq_producer = MagicMock()

    try:
        incoming = json.loads(value) if value else {}
    except (json.JSONDecodeError, TypeError):
        incoming = {}

    ctx = KafkaMessageContext(
        consumer=consumer,
        output_producer=output_producer,
        dlq_producer=dlq_producer,
        output_topic="test-agent-tool-use-result",
        dlq_topic="test-agent-tool-use-dlq",
        topic="test-agent-tool-use",
        partition=0,
        offset=0,
        key=key,
        value=value,
        tool_use_id=tool_use_id,
        incoming=incoming,
    )
    return ctx, output_producer, dlq_producer


def get_success_result(producer):
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    return payload


class TestSearchLeads:
    def test_search_by_status(self):
        value = json.dumps({"name": "search_leads", "input": {"status": "dormant"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        assert ctx.settled is True
        payload = get_success_result(producer)
        assert payload["result"]["count"] > 0
        for lead in payload["result"]["leads"]:
            assert lead["status"] == "dormant"

    def test_search_by_industry(self):
        value = json.dumps({"name": "search_leads", "input": {"industry": "healthcare"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["count"] == 1
        assert payload["result"]["leads"][0]["company"] == "BrightHealth"

    def test_search_no_results(self):
        value = json.dumps({"name": "search_leads", "input": {"industry": "aerospace"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["count"] == 0
        assert payload["result"]["leads"] == []


class TestGetLeadDetails:
    def test_existing_lead(self):
        value = json.dumps({"name": "get_lead_details", "input": {"lead_id": "lead-001"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["name"] == "Jane Chen"
        assert payload["result"]["company"] == "Acme Corp"
        assert "activities" in payload["result"]

    def test_missing_lead(self):
        value = json.dumps({"name": "get_lead_details", "input": {"lead_id": "nonexistent"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        # Still success — the tool returns an error in content, not a framework error
        payload = get_success_result(producer)
        assert "not found" in payload["result"]["error"]


class TestUpdateLeadStatus:
    def test_updates_status_and_returns_old_and_new(self):
        # Reset state
        LEADS["lead-001"]["status"] = "dormant"

        value = json.dumps({"name": "update_lead_status", "input": {"lead_id": "lead-001", "status": "contacted"}})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["old_status"] == "dormant"
        assert payload["result"]["new_status"] == "contacted"
        assert LEADS["lead-001"]["status"] == "contacted"

        # Cleanup
        LEADS["lead-001"]["status"] = "dormant"


class TestLogActivity:
    def test_appends_activity(self):
        original_count = len(LEADS["lead-002"]["activities"])

        value = json.dumps({"name": "log_activity", "input": {
            "lead_id": "lead-002", "type": "call", "summary": "Follow-up call"
        }})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["activity"]["type"] == "call"
        assert payload["result"]["activity"]["summary"] == "Follow-up call"
        assert len(LEADS["lead-002"]["activities"]) == original_count + 1

        # Cleanup
        LEADS["lead-002"]["activities"].pop()


class TestDraftFollowupEmail:
    def test_returns_email_context(self):
        value = json.dumps({"name": "draft_followup_email", "input": {
            "lead_id": "lead-001", "reason": "new pricing"
        }})
        ctx, producer, _ = make_ctx(value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["result"]["to"] == "jane.chen@acmecorp.com"
        assert payload["result"]["reason"] == "new pricing"


class TestErrorHandling:
    def test_unknown_tool_sends_to_dlq(self):
        value = json.dumps({"name": "nonexistent_tool", "input": {}})
        ctx, producer, dlq_producer = make_ctx(value=value)

        process("session-1", value, ctx)

        # Should error (DLQ), not success
        assert ctx.settled is True
        producer.produce.assert_not_called()
        dlq_producer.produce.assert_called_once()
        headers = dict(dlq_producer.produce.call_args.kwargs["headers"])
        assert b"Unknown tool" in headers["error.reason"]

    def test_tool_use_id_attached_to_success(self):
        value = json.dumps({"name": "search_leads", "input": {}})
        ctx, producer, _ = make_ctx(tool_use_id="toolu_abc123", value=value)

        process("session-1", value, ctx)

        payload = get_success_result(producer)
        assert payload["tool_use_id"] == "toolu_abc123"
