import json
import pytest
from unittest.mock import MagicMock, patch
from flightdeck_sdk.think_consumer_runner import ThinkConsumerRunner, ThinkConsumerConfig


def make_runner(system_prompt="You are helpful.", tools=None, **kwargs):
    with patch("flightdeck_sdk.think_consumer_runner.Consumer"), \
         patch("flightdeck_sdk.think_consumer_runner.Producer"):
        config = ThinkConsumerConfig(
            agent_name="test-agent",
            brokers="localhost:9092",
            claude_api_key="test-key",
            system_prompt=system_prompt,
            tools=tools or [],
            **kwargs,
        )
        return ThinkConsumerRunner(config)


class TestTopicDerivation:
    def test_all_names_derived_from_agent_name(self):
        with patch("flightdeck_sdk.think_consumer_runner.Consumer"), \
             patch("flightdeck_sdk.think_consumer_runner.Producer"):
            config = ThinkConsumerConfig(
                agent_name="order-agent",
                brokers="localhost:9092",
                claude_api_key="key",
                system_prompt="test",
            )
        assert config.group_id == "order-agent-think-consumer"
        assert config.input_topic == "order-agent-enriched-message-input"
        assert config.output_topic == "order-agent-think-request-response"
        assert config.dlq_topic == "order-agent-think-dlq"


class TestBuildSystemPrompt:
    def test_base_prompt_with_memoir_and_custom_builder(self):
        """Base prompt alone, with memoir appended, and with a custom builder."""
        runner = make_runner(system_prompt="Base prompt.")

        # No memoir → base prompt only
        assert runner._build_system_prompt("", {}) == "Base prompt."

        # With memoir → appended
        result = runner._build_system_prompt("User likes cats.", {})
        assert "Base prompt." in result
        assert "User likes cats." in result
        assert "memoir" in result.lower()

        # Custom builder overrides base prompt construction
        runner2 = make_runner(
            system_prompt="Hello.",
            system_prompt_builder=lambda base, ctx: f"{base} Customer: {ctx.get('userId', '?')}",
        )
        result2 = runner2._build_system_prompt("", {"userId": "alice"})
        assert result2 == "Hello. Customer: alice"


class TestToClaudeMessages:
    def test_history_with_tool_round_trip(self):
        """Full conversation: user → assistant(tool_use) → tool_result → latest input."""
        runner = make_runner()
        messages = runner._to_claude_messages(
            history=[
                {"role": "user", "content": "Check order 123"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {"id": "123"}}
                ]},
                {"role": "tool", "content": [
                    {"tool_use_id": "toolu_1", "content": "shipped"}
                ]},
            ],
            latest_input={"content": "Thanks!"},
        )

        # 4 messages: user, assistant(structured), tool_result(as user), latest user
        assert len(messages) == 4
        assert messages[0] == {"role": "user", "content": "Check order 123"}
        # Assistant preserves structured content
        assert messages[1]["role"] == "assistant"
        assert isinstance(messages[1]["content"], list)
        # Tool result converted to user role with tool_result block
        assert messages[2]["role"] == "user"
        assert messages[2]["content"][0]["type"] == "tool_result"
        assert messages[2]["content"][0]["tool_use_id"] == "toolu_1"
        # Latest input appended
        assert messages[3]["content"] == "Thanks!"

    def test_merges_consecutive_same_role(self):
        runner = make_runner()
        messages = runner._to_claude_messages(
            history=[
                {"role": "user", "content": "Hi"},
                {"role": "user", "content": "Also this"},
            ],
            latest_input={},
        )
        assert len(messages) == 1
        assert "Hi" in messages[0]["content"]
        assert "Also this" in messages[0]["content"]


class TestParseResponse:
    @patch.dict("os.environ", {"INPUT_TOKEN_PRICE": "3", "OUTPUT_TOKEN_PRICE": "15"})
    def test_text_only_response_with_pricing(self):
        # Set class attrs with env vars (per million tokens)
        ThinkConsumerRunner.INPUT_TOKEN_PRICE = 3.0
        ThinkConsumerRunner.OUTPUT_TOKEN_PRICE = 15.0

        runner = make_runner()
        result = runner._parse_response(
            response={
                "content": [{"type": "text", "text": "Your order is shipped."}],
                "usage": {"input_tokens": 1_000_000, "output_tokens": 1_000_000},
                "stop_reason": "end_turn",
            },
            session_id="sess-1",
            user_id="user-1",
            latest_input={"content": "check order"},
        )

        assert result["sessionId"] == "sess-1"
        assert result["userId"] == "user-1"
        assert result["endTurn"] is True
        assert result["cost"] == pytest.approx(18.0)  # $3 input + $15 output
        assert result["inputTokens"] == 1_000_000
        assert result["outputTokens"] == 1_000_000

        # Reset class attrs
        ThinkConsumerRunner.INPUT_TOKEN_PRICE = None
        ThinkConsumerRunner.OUTPUT_TOKEN_PRICE = None

    def test_text_only_response_without_pricing(self):
        runner = make_runner()
        result = runner._parse_response(
            response={
                "content": [{"type": "text", "text": "Your order is shipped."}],
                "usage": {"input_tokens": 1_000_000, "output_tokens": 1_000_000},
                "stop_reason": "end_turn",
            },
            session_id="sess-1",
            user_id="user-1",
            latest_input={"content": "check order"},
        )

        assert result["cost"] is None  # No pricing env vars set
        assert len(result["toolUses"]) == 0
        # messages[0] = latest_input, messages[1] = assistant text
        assert result["messages"][0] == {"content": "check order"}
        assert result["messages"][1]["content"] == "Your order is shipped."
        assert result["messages"][1]["role"] == "assistant"

    def test_tool_use_response(self):
        runner = make_runner()
        result = runner._parse_response(
            response={
                "content": [
                    {"type": "text", "text": "Let me look that up."},
                    {"type": "tool_use", "id": "toolu_abc", "name": "lookup", "input": {"id": "123"}},
                ],
                "usage": {"input_tokens": 200, "output_tokens": 100},
                "stop_reason": "tool_use",
            },
            session_id="sess-1",
            user_id="user-1",
            latest_input={},
        )

        assert result["endTurn"] is False
        # Assistant message has full structured content blocks
        assistant_msg = result["messages"][0]
        assert assistant_msg["role"] == "assistant"
        assert isinstance(assistant_msg["content"], list)
        assert len(assistant_msg["content"]) == 2
        # Tool use extracted
        assert len(result["toolUses"]) == 1
        assert result["toolUses"][0]["toolUseId"] == "toolu_abc"
        assert result["toolUses"][0]["name"] == "lookup"
        assert result["toolUses"][0]["input"] == {"id": "123"}
        assert result["toolUses"][0]["totalTools"] == 1


class TestCallClaude:
    @patch("flightdeck_sdk.think_consumer_runner.urllib.request.urlopen")
    def test_sends_correct_request_and_parses_response(self, mock_urlopen):
        runner = make_runner(
            tools=[{"name": "lookup", "description": "Look up order", "input_schema": {}}],
            claude_model="claude-sonnet-4-20250514",
            claude_max_tokens=2048,
        )

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 10, "output_tokens": 5},
            "stop_reason": "end_turn",
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = runner._call_claude("system prompt", [{"role": "user", "content": "hello"}])

        # Response parsed correctly
        assert result["content"][0]["text"] == "hi"

        # Request built correctly
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data.decode())
        assert body["system"] == "system prompt"
        assert body["messages"] == [{"role": "user", "content": "hello"}]
        assert body["tools"] == [{"name": "lookup", "description": "Look up order", "input_schema": {}}]
        assert body["model"] == "claude-sonnet-4-20250514"
        assert body["max_tokens"] == 2048
        assert req.get_header("X-api-key") == "test-key"
        assert req.get_header("Anthropic-version") == "2023-06-01"

    @patch("flightdeck_sdk.think_consumer_runner.urllib.request.urlopen")
    def test_no_tools_omitted_from_request(self, mock_urlopen):
        runner = make_runner(tools=[])

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "content": [], "usage": {}, "stop_reason": "end_turn",
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        runner._call_claude("prompt", [])

        body = json.loads(mock_urlopen.call_args[0][0].data.decode())
        assert "tools" not in body


class TestEmitErrorResponse:
    def test_produces_error_message_with_end_turn(self):
        runner = make_runner()
        # Access the mocked producer
        producer = runner._producer

        value = json.dumps({"userId": "alice"})
        runner._emit_error_response(
            key="sess-42", value=value, reason="Gemini API error: HTTP 429",
            topic="input-topic", partition=0, offset=0,
        )

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "test-agent-think-request-response"
        assert call_kwargs["key"] == "sess-42"

        payload = json.loads(call_kwargs["value"])
        assert payload["sessionId"] == "sess-42"
        assert payload["userId"] == "alice"
        assert payload["endTurn"] is True
        assert payload["cost"] is None
        assert payload["inputTokens"] == 0
        assert payload["outputTokens"] == 0
        assert len(payload["messages"]) == 1
        msg = payload["messages"][0]
        assert msg["role"] == "assistant"
        assert "error occurred" in msg["content"]
        assert "HTTP 429" in msg["content"]
        producer.flush.assert_called_once()

    def test_handles_missing_user_id(self):
        runner = make_runner()
        producer = runner._producer

        runner._emit_error_response(
            key="sess-99", value="{}", reason="bad request",
            topic="input-topic", partition=0, offset=0,
        )

        payload = json.loads(producer.produce.call_args.kwargs["value"])
        assert payload["userId"] == ""
        assert payload["endTurn"] is True
        assert "bad request" in payload["messages"][0]["content"]

    def test_handles_null_key(self):
        runner = make_runner()
        producer = runner._producer

        runner._emit_error_response(
            key=None, value="{}", reason="fail",
            topic="input-topic", partition=0, offset=0,
        )

        payload = json.loads(producer.produce.call_args.kwargs["value"])
        assert payload["sessionId"] == ""
        assert payload["endTurn"] is True

    def test_handles_invalid_json_value(self):
        runner = make_runner()
        producer = runner._producer

        runner._emit_error_response(
            key="sess-1", value="not-json", reason="oops",
            topic="input-topic", partition=0, offset=0,
        )

        payload = json.loads(producer.produce.call_args.kwargs["value"])
        assert payload["sessionId"] == "sess-1"
        assert payload["userId"] == ""
        assert payload["endTurn"] is True


class TestParseGeminiResponse:
    def test_text_only_response(self):
        runner = make_runner(llm_provider="gemini", gemini_api_key="test-key")
        response = {
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello from Gemini"}],
                    "role": "model",
                },
                "finishReason": "STOP",
            }],
            "usageMetadata": {
                "promptTokenCount": 100,
                "candidatesTokenCount": 50,
            },
        }

        result = runner._parse_gemini_response(
            response, "sess-1", "user-1",
            latest_input={"content": "hi"}, history=[],
        )

        assert result["sessionId"] == "sess-1"
        assert result["endTurn"] is True
        assert result["inputTokens"] == 100
        assert result["outputTokens"] == 50
        assert len(result["toolUses"]) == 0
        # messages[0] = latest_input, messages[1] = assistant text
        assert result["messages"][1]["role"] == "assistant"
        assert result["messages"][1]["content"] == "Hello from Gemini"

    def test_function_call_response(self):
        runner = make_runner(llm_provider="gemini", gemini_api_key="test-key")
        response = {
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Let me check."},
                        {"functionCall": {"name": "kafka_broker_health", "args": {}}},
                    ],
                    "role": "model",
                },
                "finishReason": "STOP",
            }],
            "usageMetadata": {"promptTokenCount": 200, "candidatesTokenCount": 80},
        }

        result = runner._parse_gemini_response(
            response, "sess-2", "user-2",
            latest_input={}, history=[],
        )

        assert result["endTurn"] is False
        assert len(result["toolUses"]) == 1
        assert result["toolUses"][0]["name"] == "kafka_broker_health"
        assert result["toolUses"][0]["toolUseId"].startswith("toolu_")
        # Assistant message has structured content blocks
        assistant_msg = result["messages"][0]
        assert assistant_msg["role"] == "assistant"
        assert isinstance(assistant_msg["content"], list)

    def test_empty_candidates_raises(self):
        runner = make_runner(llm_provider="gemini", gemini_api_key="test-key")
        with pytest.raises(RuntimeError, match="No candidates"):
            runner._parse_gemini_response(
                {"candidates": []}, "s", "u",
                latest_input={}, history=[],
            )
