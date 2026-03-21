#!/usr/bin/env bash
set -euo pipefail

ARB="${ARB:-http://localhost:8080}"
CHAT_MODEL="${CHAT_MODEL:-qwen3.5:9b}"
RERANK_MODEL="${RERANK_MODEL:-Qwen/Qwen3-Reranker-0.6B}"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

say() {
  printf '\n==> %s\n' "$*"
}

fail() {
  printf '\n[FAIL] %s\n' "$*" >&2
  exit 1
}

check_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

check_cmd curl
check_cmd jq

say "1) Health check"
curl -fsS "$ARB/healthz" | tee "$TMPDIR/health.json" | jq .
jq -e '.ok == true' "$TMPDIR/health.json" >/dev/null || fail "healthz did not return ok=true"

say "2) Chat request through arbiter -> Ollama"
curl -fsS \
  -H 'Content-Type: application/json' \
  -d "{
    \"model\": \"$CHAT_MODEL\",
    \"stream\": false,
    \"messages\": [
      {\"role\": \"user\", \"content\": \"Reply with exactly: arbiter-chat-ok\"}
    ]
  }" \
  "$ARB/api/chat" | tee "$TMPDIR/chat.json" | jq .

CHAT_TEXT="$(jq -r '.message.content // empty' "$TMPDIR/chat.json" | tr -d '\r')"
echo "Chat content: $CHAT_TEXT"
[[ -n "$CHAT_TEXT" ]] || fail "No chat content returned"

say "3) Rerank request through arbiter -> vLLM"
curl -fsS \
  -H 'Content-Type: application/json' \
  -d "{
    \"model\": \"$RERANK_MODEL\",
    \"query\": \"How do I reset my password?\",
    \"documents\": [
      \"To reset your password, go to Settings > Security and click Reset Password.\",
      \"Our offices are closed on Swedish public holidays.\",
      \"Password reset emails expire after 15 minutes.\"
    ],
    \"top_n\": 2,
    \"return_documents\": true
  }" \
  "$ARB/v1/rerank" | tee "$TMPDIR/rerank.json" | jq .

jq -e '.results | type == "array" and length > 0' "$TMPDIR/rerank.json" >/dev/null ||
  fail "Rerank returned no results"

TOP_INDEX="$(jq -r '.results[0].index' "$TMPDIR/rerank.json")"
TOP_SCORE="$(jq -r '.results[0].relevance_score' "$TMPDIR/rerank.json")"
echo "Top rerank result: index=$TOP_INDEX score=$TOP_SCORE"

if [[ "$TOP_INDEX" != "0" && "$TOP_INDEX" != "2" ]]; then
  fail "Unexpected top rerank result. Expected document 0 or 2 to rank first."
fi

say "4) Verify arbiter becomes usable for chat again after rerank"
curl -fsS \
  -H 'Content-Type: application/json' \
  -d "{
    \"model\": \"$CHAT_MODEL\",
    \"stream\": false,
    \"messages\": [
      {\"role\": \"user\", \"content\": \"Reply with exactly: arbiter-post-rerank-ok\"}
    ]
  }" \
  "$ARB/api/chat" | tee "$TMPDIR/chat2.json" | jq .

CHAT2_TEXT="$(jq -r '.message.content // empty' "$TMPDIR/chat2.json" | tr -d '\r')"
echo "Post-rerank chat content: $CHAT2_TEXT"
[[ -n "$CHAT2_TEXT" ]] || fail "No post-rerank chat content returned"

say "5) Final health snapshot"
curl -fsS "$ARB/healthz" | tee "$TMPDIR/health-final.json" | jq .

say "All basic checks passed"
