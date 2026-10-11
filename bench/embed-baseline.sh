#!/usr/bin/env bash
# Phase 0 baseline: embeddings from Ollama for fixed synthetic texts.
set -euo pipefail
cd "$(dirname "$0")"
TEXTS=$(cat embed-texts.json)
while read -r model name; do
  curl -s localhost:11434/api/embed -d "{\"model\":\"$model\",\"input\":$TEXTS}" > "embed-baseline-$name.json"
  jq -c '{model, n: (.embeddings|length), dim: (.embeddings[0]|length)}' "embed-baseline-$name.json"
done <<'L'
bge-m3:latest bge-m3
snowflake-arctic-embed2:latest snowflake
L
