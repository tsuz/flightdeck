#!/usr/bin/env bash
#
# Seeds the compacted {AGENT_NAME}-user-settings topic with one system prompt
# per user id.
#
# Record layout (key = user_id, value = JSON):
#
#   key:   user_alice
#   value: {"system_prompt":"You are Ada ...","updated_at":"2026-08-23T00:00:00Z"}
#
# The processing app materializes this topic as a KTable keyed by user_id and
# left-joins it into the session context, so a user with no record here keeps
# the default prompt (see ../system-prompt.txt) and a tombstone (null value)
# reverts a user back to it.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
AGENT="${AGENT_NAME:?AGENT_NAME must be set}"
TOPIC="${AGENT}-user-settings"
KAFKA_BIN=/opt/kafka/bin
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# ── Prompts ──────────────────────────────────────────────────────────────────
# Written as JSON string bodies, so: no raw double quotes, no tabs, and use \n
# for line breaks. Each begins with a literal tag so the persona in a reply is
# unambiguous (the integration test greps for it).

ALICE_PROMPT='You are Ada, a senior database engineer who helps exactly one user: Alice.\nYou answer ONLY questions about databases, SQL and query performance.\nBegin every reply with the exact tag [SQL-TUTOR] on the first line.\nKeep answers to at most three sentences.\nIf the question is not about databases, say that you only cover SQL topics.'

BOB_PROMPT='You are Basho, a haiku poet who helps exactly one user: Bob.\nWhatever you are asked, you reply with a single haiku about it: three lines of 5-7-5 syllables.\nBegin every reply with the exact tag [HAIKU] on the first line, then the haiku.\nNever write prose, never explain the haiku.'

# ── Wait for the topic ───────────────────────────────────────────────────────
# The processing app creates it with cleanup.policy=compact on startup when
# ENABLE_USER_SETTING=true. Seed into that topic rather than letting the
# producer auto-create a non-compacted one.
echo "[seed] waiting for topic ${TOPIC} on ${BOOTSTRAP}"
for _ in $(seq 1 60); do
  if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null \
       | grep -qx "$TOPIC"; then
    found=1
    break
  fi
  sleep 2
done

if [ -z "${found:-}" ]; then
  echo "[seed] topic not found after 120s — creating it compacted"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create \
    --topic "$TOPIC" --partitions 1 --replication-factor 1 \
    --config cleanup.policy=compact
fi

# ── Produce one settings record per user ─────────────────────────────────────
# parse.key=true with the default tab key separator: "<user_id>\t<json>".
echo "[seed] writing user settings to ${TOPIC}"
{
  printf '%s\t{"system_prompt":"%s","updated_at":"%s"}\n' user_alice "$ALICE_PROMPT" "$TS"
  printf '%s\t{"system_prompt":"%s","updated_at":"%s"}\n' user_bob   "$BOB_PROMPT"   "$TS"
} | "$KAFKA_BIN/kafka-console-producer.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --property parse.key=true

echo "[seed] done — current contents of ${TOPIC}:"
"$KAFKA_BIN/kafka-console-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --from-beginning --timeout-ms 5000 \
  --property print.key=true 2>/dev/null || true

echo "[seed] user_carol was deliberately left out — that user falls back to the default prompt"
