#!/usr/bin/env bash
# Phase 3: generation speed through llama-swap with the Phase 0 prompt (compare with llama-swap-results.md).
set -euo pipefail
BASE=${1:-http://localhost:11435}
shift || true
MODELS=("${@:-qwen3.8-27b gemma4-26b qwen3.6-35b-a3b}")
PROMPT='Write a C# Unity MonoBehaviour that snaps a transform to an isometric grid. Explain briefly.'
for m in ${MODELS[@]}; do
  curl -s "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg m "$m" '{model:$m,messages:[{role:"user",content:"hi"}],max_tokens:1}')" >/dev/null
  curl -s "$BASE/v1/chat/completions" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg m "$m" --arg p "$PROMPT" '{model:$m,messages:[{role:"user",content:$p}],max_tokens:512}')" \
    | jq -c '{model, tokens: .usage.completion_tokens, tps: .timings.predicted_per_second, prompt_tps: .timings.prompt_per_second}'
  rocm-smi --showmeminfo vram | grep 'GPU\[0\].*Used'
done
