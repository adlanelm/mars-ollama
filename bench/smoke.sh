#!/usr/bin/env bash
# Phase 3 functional smoke tests: vision, tool calling, granite (docling helpers). Synthetic inputs only.
set -euo pipefail
BASE=${BASE:-http://localhost:11435}
cd "$(dirname "$0")"
IMG="data:image/png;base64,$(base64 -w0 test-page.png)"
chat() { curl -s "$BASE/v1/chat/completions" -H 'Content-Type: application/json' -d @-; }

vision() { # model prompt max_tokens
  jq -n --arg m "$1" --arg p "$2" --arg img "$IMG" --argjson n "$3" \
    '{model:$m,max_tokens:$n,messages:[{role:"user",content:[{type:"image_url",image_url:{url:$img}},{type:"text",text:$p}]}]}' \
    | chat | jq -r --arg m "$1" '"== \($m) vision (\(.usage.prompt_tokens) prompt tok)\n\(.choices[0].message.content // .error.message)"'; }

tool() { # model
  jq -n --arg m "$1" '{model:$m,max_tokens:2048,messages:[{role:"user",content:"What is the coolant pressure on deck 3? Use the tool."}],
    tools:[{type:"function",function:{name:"read_sensor",description:"Read a ship sensor",
      parameters:{type:"object",properties:{deck:{type:"integer"},sensor:{type:"string",enum:["coolant_pressure","temperature"]}},required:["deck","sensor"]}}}]}' \
    | chat | jq -c --arg m "$1" '{model:$m, finish:.choices[0].finish_reason, calls:[.choices[0].message.tool_calls[]?.function|{name,arguments}]}'; }

for m in ${MODELS:-qwen3.8-27b qwen3.6-35b-a3b gemma4-26b}; do
  vision "$m" "Transcribe the table and name the shape and its colour. Be brief." 2048
  tool "$m"
done
vision granite-vision "Describe this image briefly." 300
vision granite-docling "Convert this page to docling." 1500
