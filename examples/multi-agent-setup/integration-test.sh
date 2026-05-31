#!/usr/bin/env bash
#
# Integration test for the multi-agent-setup example.
#
# Spins up the whole stack (orchestrator + worker + Kafka), sends a message to
# the orchestrator's /api/chat, and verifies the async multi-agent round-trip:
#   1. the worker handled a delegated sub-session, and
#   2. the orchestrator produced a non-empty final answer for our session.
#
# The result (the orchestrator's final answer) is printed on success.
#
# Usage:
#   CLAUDE_API_KEY=sk-ant-... ./integration-test.sh
#
# Optional env:
#   TOOL_CALLBACK_SECRET   shared HMAC secret (default: random)
#   TIMEOUT                seconds to wait for the answer (default: 240)
#   CLAUDE_MODEL           model override (default: compose default)
#
# Requires: docker compose, curl, python3.

set -euo pipefail

: "${CLAUDE_API_KEY:?CLAUDE_API_KEY must be set}"
export CLAUDE_API_KEY
export TOOL_CALLBACK_SECRET="${TOOL_CALLBACK_SECRET:-it-secret-${RANDOM}${RANDOM}}"
[ -n "${CLAUDE_MODEL:-}" ] && export CLAUDE_MODEL

cd "$(dirname "$0")"

PROJECT="multiagent-it"
COMPOSE=(docker compose -p "$PROJECT")
ORCH_URL="http://localhost:8000"
SESSION="it-$(date +%s)"
TIMEOUT="${TIMEOUT:-300}"
PROMPT="Compute the exact closed-form value of the integral ∫₀^∞ (x² / (eˣ − 1)) dx. Show the full derivation: rewrite the integrand using the geometric series expansion of 1/(eˣ − 1), interchange sum and integral with justification, and reduce to a product of a Gamma function and a Riemann zeta value. State the final answer in terms of ζ(3). Then independently confirm the numerical value to 6 decimal places using a direct numerical integration, and report both numbers side by side so they can be compared."

cleanup() {
  echo "--- tearing down ---"
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
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

echo "--- waiting for orchestrator-api at $ORCH_URL ---"
ready=""
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null -X OPTIONS "$ORCH_URL/api/chat"; then ready=1; break; fi
  sleep 3
done
[ -n "$ready" ] || { echo "FAIL: orchestrator-api never became ready"; exit 1; }

echo "--- sending message (session=$SESSION) ---"
# Build the JSON body with python3 so the Unicode prompt (∫, ζ, −, …) is encoded safely.
payload=$(SESSION="$SESSION" PROMPT="$PROMPT" python3 -c \
  'import json,os; print(json.dumps({"session_id":os.environ["SESSION"],"content":os.environ["PROMPT"]}))')
curl -sf -X POST "$ORCH_URL/api/chat" \
  -H 'Content-Type: application/json' \
  -d "$payload"
echo

echo "--- awaiting orchestrator final answer (timeout ${TIMEOUT}s) ---"
final=""
worker_seen=""
end=$((SECONDS + TIMEOUT))
while [ "$SECONDS" -lt "$end" ]; do
  # 1. Did the worker handle a delegated sub-session ({SESSION}--{tool_use_id})?
  if [ -z "$worker_seen" ] && consume worker-message-output | grep -q "${SESSION}--"; then
    worker_seen=1
    echo "[ok] worker handled a delegated sub-session"
  fi

  # 2. Did the orchestrator emit a non-empty final answer for our session?
  line=$(consume orchestrator-message-output | grep "\"session_id\":\"${SESSION}\"" | tail -1 || true)
  if [ -n "$line" ]; then
    content=$(printf '%s' "$line" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("content",""))' 2>/dev/null || true)
    if [ -n "$content" ]; then final="$content"; break; fi
  fi

  sleep 3
done

if [ -z "$final" ]; then
  echo "FAIL: no non-empty orchestrator response for ${SESSION} within ${TIMEOUT}s"
  echo "--- recent logs ---"
  "${COMPOSE[@]}" logs --tail=40 \
    orchestrator-api orchestrator-think orchestrator-dispatcher worker-api worker-think || true
  exit 1
fi

echo "=================== ORCHESTRATOR ANSWER ==================="
echo "$final"
echo "=========================================================="

# Informational: the closed form is Γ(3)·ζ(3) = 2ζ(3) ≈ 2.404114. LLM phrasing
# varies, so this is a soft signal, not a hard assertion.
if printf '%s' "$final" | grep -qiE 'ζ\(3\)|zeta\(3\)|2\.40411'; then
  echo "[ok] answer references the expected result (ζ(3) / ≈2.404114)"
else
  echo "[warn] answer did not obviously reference ζ(3) / 2.404114 — review the output above"
fi

if [ -z "$worker_seen" ]; then
  echo "FAIL: got an answer but the worker never received a delegated task"
  exit 1
fi

echo "PASS: multi-agent round-trip verified (orchestrator → worker → callback → answer)"
