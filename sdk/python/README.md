# Flightdeck SDK (Python)

Kafka tool consumer framework for building think and agent tool executors. The SDK handles all Kafka plumbing — you only write the processing logic.

## Install

```bash
pip install flightdeck-sdk
```

Or install from source:

```bash
cd sdk/python
pip install .
```

Or directly from git:

```bash
pip install "flightdeck-sdk @ git+https://github.com/tsuz/flightdeck.git#subdirectory=sdk/python"
```

## Usage

```python
import json
from flightdeck_sdk import ToolConsumerRunner, ToolConsumerConfig


def lookup_order(order_id: str) -> dict:
    return {"order_id": order_id, "status": "shipped"}


def process(key, value, ctx):
    request = json.loads(value)

    if request["name"] == "lookup_order":
        result = lookup_order(request["input"]["order_id"])
        ctx.success(result)
    else:
        ctx.error(f"Unknown tool: {request['name']}")


runner = ToolConsumerRunner(
    ToolConsumerConfig(
        agent_name="order-agent",
        brokers="localhost:9092",
        process_fn=process,
    )
)

runner.start()
```

The `agent_name` automatically derives all Kafka names:

| Derived name | Pattern |
|---|---|
| Consumer group | `{agent_name}-tool-execution` |
| Input topic | `{agent_name}-tool-use-request` |
| Output topic | `{agent_name}-tool-use-response` |
| DLQ topic | `{agent_name}-tool-use-dlq` |

## API

### `ToolConsumerConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `agent_name` | `str` | Yes | Agent name — used to derive topic and group names |
| `brokers` | `str` | Yes | Kafka bootstrap servers (comma-separated) |
| `process_fn` | `Callable` | Yes | Your processing function |
| `poll_timeout_s` | `float` | No | Poll timeout in seconds (default: `1.0`) |

### `process_fn(key, value, ctx)`

Your processing function receives:

- `key` — Kafka message key (`str | None`)
- `value` — Kafka message value (`str | None`)
- `ctx` — Message context with two methods:

| Method | Description |
|---|---|
| `ctx.success(content)` | Publish result to output topic, commit offset async |
| `ctx.error(reason)` | Send original message to DLQ, commit offset async |

You must call exactly one of `ctx.success()` or `ctx.error()` per message. Calling neither logs a warning; calling both raises `RuntimeError`. Uncaught exceptions are automatically routed to the DLQ.

## Build & Publish

### Build the package

```bash
pip install build
python -m build
```

This creates `dist/flightdeck_sdk-0.1.0.tar.gz` and `dist/flightdeck_sdk-0.1.0-py3-none-any.whl`.

### Publish to PyPI

```bash
pip install twine
twine upload dist/*
```

### Publish to a private registry

```bash
twine upload --repository-url https://your-registry.example.com/simple/ dist/*
```

### Install from the built wheel (local testing)

```bash
pip install dist/flightdeck_sdk-0.1.0-py3-none-any.whl
```
