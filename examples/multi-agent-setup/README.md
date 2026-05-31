# Multi-Agent Setup — Async Tool Delegation with Reply Routing

Two FlightDeck agents on one Kafka cluster. The **orchestrator** delegates tasks
to the **worker** by calling a tool — but instead of blocking, the tool hands the
work off and the worker's answer flows back as the tool result *asynchronously*.

The whole point: **the orchestrator treats the worker as a generic async tool,
and the worker treats the request as a generic chat. Neither knows the other is
an agent.**

## How it works

```
  User ──▶ Orchestrator (Agent A)
                │  LLM calls delegate_task(task)
                ▼
        orchestrator-tool-use
                │
                ▼
        orchestrator-dispatcher                       ┌─ mint HMAC token
          • ctx "pending": ack offset, NO result  ────┤  (session_id, tool_use_id,
          • POST task to worker /api/chat              │   tool_id, name, total_tools)
            with a `reply` descriptor                  └─ token = capability, opaque to B
                │
                ▼
        Worker (Agent B) — an ordinary agent
          /api/chat stores reply-to (keyed by session) ──▶ worker-reply-to topic
          think → (its own tools, if any) → end turn
          EndTurnProcessor left-joins reply-to ──▶ worker-message-output (carries reply_to)
                │
                ▼
        worker-api OutputConsumer
          • sees reply_to → POST answer back to A's /api/tools/response
            Authorization: Bearer <token>,  body { "result": "<worker's answer>" }
          • on success: tombstone the reply-to route (one-shot)
                │
                ▼
        Orchestrator /api/tools/response
          • verify token → write tool-use-result (keyed by A's session)
                │
                ▼
        orchestrator aggregator
          • matches tool_use_id → turn completes  (or times out → error result)
                │
                ▼
  User ◀── Orchestrator synthesizes the worker's answer
```

## Design choices illustrated

- **Async, not blocking.** The dispatcher acks the `tool-use` offset *without*
  producing a `tool-use-result` (the "pending" ack) and returns immediately. No
  thread is held open waiting for the worker. Compare this to a synchronous
  bridge that blocks the tool consumer for the whole sub-call.

- **HMAC token = capability, secret stays with A.** The orchestrator mints a
  signed token carrying the correlation fields and hands it to the worker. The
  worker cannot read, forge, or alter it — it only echoes it back. Only the
  orchestrator holds `TOOL_CALLBACK_SECRET`, on the two components that need it
  (dispatcher to sign, chat-api to verify).

- **Reply routing is transport, not prompt.** The `reply` descriptor
  (`{ "callbackService": "...", "bearerToken": "..." }`) travels as a transport
  field on `/api/chat`, is stored on the worker's `reply-to` topic keyed by
  `session_id`, and is re-attached to the worker's output at end-turn by a
  left-join. The worker's LLM never sees it, and the worker has no "call back to
  the orchestrator" tool. Routing lives in the delivery layer, keeping the worker
  a vanilla agent.

- **SSRF-safe callbacks: the caller names a service, never a URL.** The descriptor
  carries only a logical `callbackService` name. The worker resolves it to a base
  URL from its own operator-controlled `ALLOWED_HOST_MAPPING`
  (`orchestrator:http://orchestrator-api:8000`) and POSTs to the fixed path
  `<baseUrl>/api/tools/response`. Because the destination host comes from worker
  config and never from caller input, an untrusted caller cannot steer the
  server-side callback at an internal host — the request is rejected (400 at
  `/api/chat`, and fail-closed at delivery) if the name is not in the mapping.

- **Free-form answer, A shapes it.** The worker returns ordinary prose. The
  worker's OutputConsumer wraps it under the fixed field name
  (`{ "result": "<answer>" }`) and the orchestrator turns it into a canonical tool
  result. No JSON contract is forced on the worker.

- **Failure is a timeout, not an error channel.** The worker never reports
  failure. If it crashes or never answers, the orchestrator's aggregator hits its
  deadline (`ASYNC_TOOL_TIMEOUT_MS`, default 5 min) and synthesizes an error
  result from the expected tool set, so the turn always completes.

- **One-shot, no double-delivery.** On a successful callback the worker tombstones
  its reply-to route; the topic is also compacted with a TTL
  (`REPLY_TO_STATE_TTL_MS`, default 24h) so stale routes are dropped.

## Services

| Service | Agent | Description |
|---------|-------|-------------|
| `orchestrator-api` | orchestrator | Chat API (user-facing) + `/api/tools/response` callback |
| `orchestrator-processing` | orchestrator | Kafka Streams pipeline |
| `orchestrator-think` | orchestrator | Claude API — decides when to `delegate_task` |
| `orchestrator-dispatcher` | orchestrator | The async tool: mints token, POSTs to worker, acks pending |
| `frontend` | orchestrator | Web UI |
| `worker-api` | worker | Chat API — accepts the task, delivers the reply via HTTP callback |
| `worker-processing` | worker | Kafka Streams pipeline |
| `worker-think` | worker | Claude API — answers the delegated task |

> The worker has an empty `tools.json` here, so it answers directly from the LLM.
> It is still a full agent — give it its own tools and tool service and nothing
> about the reply routing changes.

## Run

```bash
cp .env.example .env
# Edit .env: add CLAUDE_API_KEY and set TOOL_CALLBACK_SECRET (e.g. `openssl rand -hex 32`)

docker compose up --build
```

`chat-api`, `processing`, and `think-consumer` build from source in this repo
(this example demonstrates branch functionality not yet in the published images).
All three must share the same build — a source-built `processing` paired with a
published `think-consumer` mismatches the `ThinkResponse` schema and produces
empty content / lost history. The first build takes a few minutes.

Open [http://localhost](http://localhost) and try:

- "Ask the worker to write a haiku about Kafka."
- "Delegate this to the worker: summarize the tradeoffs of event-driven architecture in 3 bullets."
- "Have the worker explain what an HMAC is, simply."

Watch `docker compose logs -f orchestrator-dispatcher worker-api` to see the
dispatch → pending ack → worker answer → callback → tool result round-trip.

## Integration test

`integration-test.sh` brings the whole stack up, posts a message to
`orchestrator-api`, and verifies the round-trip — that the worker handled a
delegated sub-session and the orchestrator emitted a non-empty final answer —
then tears everything down. It takes `CLAUDE_API_KEY` from the environment:

```bash
CLAUDE_API_KEY=sk-ant-... ./integration-test.sh
```

The default prompt asks the worker to derive ∫₀^∞ x²/(eˣ−1) dx (= 2ζ(3) ≈
2.404114), so the printed answer is easy to eyeball. Override `TIMEOUT` or
`CLAUDE_MODEL` via env if needed.

## Troubleshooting

- **`/api/tools/response` returns 401** — `TOOL_CALLBACK_SECRET` differs between
  `orchestrator-api` and `orchestrator-dispatcher`, or the token expired.
- **Orchestrator replies "the tool failed / timed out"** — the worker didn't
  answer within `ASYNC_TOOL_TIMEOUT_MS`. Check the `worker-think` logs (API key,
  model) and `worker-api` logs (callback POST result).
- **Docker build fails compiling tests** — the service Dockerfiles compile the
  module's `src/test`. Unrelated work-in-progress test files in your working tree
  that don't compile will break the build; build from a clean checkout of the
  branch.
