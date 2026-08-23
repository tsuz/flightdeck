#!/usr/bin/env bash
#
# Integration test for the per-user-system-prompt example.
#
# Brings up the stack, waits for the user-settings seed to finish, then asks the
# SAME question as three different users and verifies each one is answered under
# its own system prompt:
#
#   user_alice → seeded [SQL-TUTOR] prompt
#   user_bob   → seeded [HAIKU] prompt
#   user_carol → no settings record → default prompt → "I don't know."
#
# Usage:
#   CLAUDE_API_KEY=sk-ant-... ./integration-test.sh
#
# Optional env:
#   TIMEOUT        seconds to wait for all three answers (default: 300)
#   CLAUDE_MODEL   model override (default: compose default)
#
# Requires: docker compose, curl, python3.

set -euo pipefail

: "${CLAUDE_API_KEY:?CLAUDE_API_KEY must be set}"
export CLAUDE_API_KEY
[ -n "${CLAUDE_MODEL:-}" ] && export CLAUDE_MODEL

cd "$(dirname "$0")"

PROJECT="usersettings-it"
COMPOSE=(docker compose -p "$PROJECT")
API_URL="http://localhost:8000"
AGENT="${AGENT_NAME:-per-user-prompt}"
STAMP="$(date +%s)"
TIMEOUT="${TIMEOUT:-300}"
QUESTION="My orders table has 50 million rows and one query over it is slow. What should I do?"

USERS=(user_alice user_bob user_carol)

cleanup() {
  echo "--- tearing down ---"
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  [ -n "${OUTDIR:-}" ] && rm -rf "$OUTDIR" || true
}
trap cleanup EXIT

# Read a topic from the beginning with a short idle timeout (existing records only).
consume() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$1" \
    --from-beginning --timeout-ms 4000 2>/dev/null || true
}

echo "--- building & starting stack (first build can take several minutes) ---"
"${COMPOSE[@]}" up -d --build

echo "--- waiting for the user-settings seed to finish ---"
# The seed job prints "[seed] done" on its last successful step and exits.
seeded=""
for _ in $(seq 1 90); do
  if "${COMPOSE[@]}" logs user-settings-seed 2>/dev/null | grep -q "\[seed\] done"; then
    seeded=1
    break
  fi
  sleep 2
done
if [ -z "$seeded" ]; then
  echo "FAIL: the user-settings seed never completed"
  "${COMPOSE[@]}" logs user-settings-seed || true
  exit 1
fi
"${COMPOSE[@]}" logs user-settings-seed | tail -20

echo "--- waiting for api at $API_URL ---"
ready=""
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null -X OPTIONS "$API_URL/api/chat"; then ready=1; break; fi
  sleep 3
done
[ -n "$ready" ] || { echo "FAIL: api never became ready"; exit 1; }

# The settings KTable is read at message-flow time; give processing a moment to
# consume the seeded records before the first question arrives.
sleep 10

echo "--- asking the same question as ${#USERS[@]} different users ---"
for user in "${USERS[@]}"; do
  session="${user}-${STAMP}"
  payload=$(SESSION="$session" USER="$user" Q="$QUESTION" python3 -c \
    'import json,os; print(json.dumps({"session_id":os.environ["SESSION"],"user_id":os.environ["USER"],"content":os.environ["Q"]}))')
  curl -sf -X POST "$API_URL/api/chat" -H 'Content-Type: application/json' -d "$payload" >/dev/null
  echo "[sent] $user (session=$session)"
done

echo "--- awaiting answers (timeout ${TIMEOUT}s) ---"
# Answers are collected into files (one per user) so this script also runs on
# the bash 3.2 that ships with macOS — no associative arrays.
OUTDIR="$(mktemp -d)"
end=$((SECONDS + TIMEOUT))
while [ "$SECONDS" -lt "$end" ]; do
  output=$(consume "${AGENT}-message-output")
  got=0
  for user in "${USERS[@]}"; do
    if [ -s "$OUTDIR/$user" ]; then got=$((got+1)); continue; fi
    line=$(printf '%s\n' "$output" | grep "\"session_id\":\"${user}-${STAMP}\"" | tail -1 || true)
    [ -n "$line" ] || continue
    content=$(printf '%s' "$line" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("content",""))' 2>/dev/null || true)
    if [ -n "$content" ]; then
      printf '%s' "$content" > "$OUTDIR/$user"
      got=$((got+1))
      echo "[got] $user"
    fi
  done
  [ "$got" -eq "${#USERS[@]}" ] && break
  sleep 3
done

for user in "${USERS[@]}"; do
  if [ ! -s "$OUTDIR/$user" ]; then
    echo "FAIL: no answer for $user within ${TIMEOUT}s"
    "${COMPOSE[@]}" logs --tail=40 api processing think-consumer || true
    exit 1
  fi
done

for user in "${USERS[@]}"; do
  echo "=================== $user ==================="
  cat "$OUTDIR/$user"; echo
done
echo "============================================"

fails=0

if grep -q '\[SQL-TUTOR\]' "$OUTDIR/user_alice"; then
  echo "[ok] user_alice answered under the seeded SQL-tutor prompt"
else
  echo "[FAIL] user_alice did not use the [SQL-TUTOR] persona"; fails=$((fails+1))
fi

if grep -q '\[HAIKU\]' "$OUTDIR/user_bob"; then
  echo "[ok] user_bob answered under the seeded haiku prompt"
else
  echo "[FAIL] user_bob did not use the [HAIKU] persona"; fails=$((fails+1))
fi

if grep -qi "i don't know" "$OUTDIR/user_carol"; then
  echo "[ok] user_carol fell back to the default prompt"
else
  echo "[FAIL] user_carol did not fall back to the default prompt"; fails=$((fails+1))
fi

# Cross-check: the personas must not bleed between users.
if grep -q '\[SQL-TUTOR\]' "$OUTDIR/user_bob" || grep -q '\[HAIKU\]' "$OUTDIR/user_alice"; then
  echo "[FAIL] personas crossed between users"; fails=$((fails+1))
fi

if [ "$fails" -ne 0 ]; then
  echo "FAIL: $fails check(s) failed"
  exit 1
fi

echo "PASS: each user was answered under their own system prompt"
