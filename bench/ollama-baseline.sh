#!/usr/bin/env bash
# Phase 0 baseline: Ollama generation speed with a fixed prompt.
set -euo pipefail
PROMPT='Write a C# Unity MonoBehaviour that snaps a transform to an isometric grid. Explain briefly.'
for m in qwen3.8:latest gemma4:26b qwen3.5:latest; do
  # warm load, then measured run
  curl -s localhost:11434/api/generate -d "{\"model\":\"$m\",\"prompt\":\"hi\",\"stream\":false,\"options\":{\"num_predict\":1}}" >/dev/null
  curl -s localhost:11434/api/generate -d "$(jq -n --arg m "$m" --arg p "$PROMPT" '{model:$m,prompt:$p,stream:false,options:{num_predict:512}}')" \
    | jq -c '{model, eval_count, prompt_eval_count, tps: (.eval_count / (.eval_duration/1e9)), prompt_tps: (.prompt_eval_count/(.prompt_eval_duration/1e9))}'
  docker exec ollama ollama ps | sed 1d
  rocm-smi --showmeminfo vram | grep 'Used'
done
