# Per-User System Prompt

One agent, one deployment — but every user gets their own system prompt, keyed
by `user_id` in a compacted Kafka topic. A user with no record falls back to the
agent's default prompt, which here is deliberately useless: it answers
`I don't know.` to everything.

## What it does

Two users are seeded into `{AGENT_NAME}-user-settings`, a third is left out:

| user_id | system prompt | answer to *"my orders table is slow, what should I do?"* |
|---------|---------------|-----------------------------------------------------|
| `user_alice` | SQL tutor persona (seeded) | `[SQL-TUTOR] Add an index on …` |
| `user_bob` | haiku poet persona (seeded) | `[HAIKU]` + a three-line haiku |
| `user_carol` | *(no record)* | `I don't know.` |

Same agent, same question, same session pipeline — the only difference is which
system prompt the think-consumer resolves for that user.

## How it works

```
POST /api/chat {user_id: "user_alice", ...}
        │
        ▼
  message-input ──► processing (Kafka Streams)
                        │
                        │  re-key by user_id
                        │  left-join  {AGENT_NAME}-user-settings  (KTable, compacted)
                        │  re-key back to session_id
                        ▼
              enriched-message-input   { …, "system_prompt": "You are Ada …" }
                        │
                        ▼
                  think-consumer
                    system_prompt present ? use it : use SYSTEM_PROMPT_FILE
```

**The topic.** `{AGENT_NAME}-user-settings` is compacted and keyed by `user_id`:

```
key:   user_alice
value: {"system_prompt":"You are Ada, a senior database engineer …","updated_at":"2026-08-23T00:00:00Z"}
```

Compaction means the topic *is* the current settings table: the latest record
per user is retained forever, and a tombstone (null value) deletes a user's
override so they revert to the default prompt.

**The join.** With `ENABLE_USER_SETTING=true`, the processing app creates the
topic if missing, materializes it as a `KTable`, and left-joins it into
`FullSessionContext` during enrichment. Left-join is the point: a user with no
record still flows through, just with `system_prompt: null`.

**The resolution.** In the think-consumer, a non-blank `system_prompt` from the
context *replaces* the base prompt (`SYSTEM_PROMPT_FILE`, here
[`system-prompt.txt`](./system-prompt.txt)) entirely — it is not appended to it.
Null or blank falls back to the file.

**Timing.** The KTable is read at message-flow time, so a settings update takes
effect on the user's **next** turn, not the one already in flight.

## Configuration

| Parameter | Service | Value | Why |
|-----------|---------|-------|-----|
| `ENABLE_USER_SETTING` | `processing` | `true` | Creates the topic, materializes the KTable, joins it into the session context |
| `MEMOIR_ENABLED` | `processing` | `false` | Off, so the only per-user difference is the system prompt |
| `SYSTEM_PROMPT_FILE` | `think-consumer` | `/app/system-prompt.txt` | The default for users with no settings record |

`ENABLE_USER_SETTING` defaults to `false`; with it off, the topology never
references the topic at all and `system_prompt` stays null for everyone.

## Services

`chat-api`, `processing` and `think-consumer` are **built from source** — the
per-user system prompt is not in the published images yet, and all three must
share one build. Only the frontend uses a published image.

| Service | Description |
|---------|-------------|
| `api` | Chat API (REST + WebSocket) |
| `processing` | Kafka Streams pipeline with `ENABLE_USER_SETTING=true` |
| `think-consumer` | Claude API caller; resolves the per-user prompt |
| `user-settings-seed` | One-shot job that writes the two seeded users, then exits |
| `frontend` | Web UI |

## Run

```bash
cp .env.example .env
# Edit .env and add your CLAUDE_API_KEY

docker compose up --build
```

Ask the same question as each user:

```bash
curl -s localhost:8000/api/chat -H 'Content-Type: application/json' -d '{
  "session_id": "s-alice-1", "user_id": "user_alice",
  "content": "My orders table has 50 million rows and one query over it is slow. What should I do?"}'

curl -s localhost:8000/api/chat -H 'Content-Type: application/json' -d '{
  "session_id": "s-bob-1", "user_id": "user_bob",
  "content": "My orders table has 50 million rows and one query over it is slow. What should I do?"}'

curl -s localhost:8000/api/chat -H 'Content-Type: application/json' -d '{
  "session_id": "s-carol-1", "user_id": "user_carol",
  "content": "My orders table has 50 million rows and one query over it is slow. What should I do?"}'
```

Read the answers off the output topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic per-user-prompt-message-output --from-beginning --timeout-ms 5000
```

Alice gets a terse SQL answer tagged `[SQL-TUTOR]`, Bob gets a haiku tagged
`[HAIKU]`, and Carol — who has no settings record — gets `I don't know.`

The compose file defaults to `claude-opus-5`; override with `CLAUDE_MODEL` in
`.env` (and adjust `INPUT_TOKEN_PRICE` / `OUTPUT_TOKEN_PRICE`, which are per 1M
tokens, to match).

### From the browser

The web UI at [http://localhost](http://localhost) does not send a `user_id`, so
chat-api defaults it to `user_42`, which is unseeded — the UI will answer
`I don't know.` until you give that user a prompt:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic per-user-prompt-user-settings --property parse.key=true <<'REC'
user_42	{"system_prompt":"You are Rex, a pirate captain. Answer everything in pirate speak, in two sentences or less.","updated_at":"2026-08-23T00:00:00Z"}
REC
```

(The key and the JSON value are separated by a literal **tab**.) Send another
message from the UI and the pirate answers.

### Reverting a user

A tombstone — a record with a null value — drops the override, and that user
goes back to the default prompt:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic per-user-prompt-user-settings \
  --property parse.key=true --property null.marker=NULL <<'REC'
user_42	NULL
REC
```

### Inspecting the settings table

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic per-user-prompt-user-settings --from-beginning \
  --property print.key=true --timeout-ms 5000
```

## Seeding your own users

[`seed-data/seed-user-settings.sh`](./seed-data/seed-user-settings.sh) is the
one-shot seed job: it waits for the compacted topic to exist, then produces one
keyed record per user. Add a user by adding a prompt variable and a `printf`
line — prompts are JSON string bodies, so use `\n` for line breaks and avoid raw
double quotes.

In a real deployment this topic is written by whatever owns user preferences —
an admin UI, a settings service, a CDC stream off your users table — not by a
shell script.

## Automated test

```bash
CLAUDE_API_KEY=sk-ant-... ./integration-test.sh
```

Brings the stack up, waits for the seed, asks all three users the same question,
and asserts each answered under its own prompt (and that Carol fell back to
`I don't know.`). Tears the stack down on exit.
