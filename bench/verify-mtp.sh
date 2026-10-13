#!/usr/bin/env bash
# verify-mtp.sh <label> [extra flags]  -- gemma4 Vulkan, 4 prompts x 400 tokens greedy, saves outputs + timings
set -uo pipefail
label=$1; shift; extra="$*"; D=$(dirname "$0")
for m in qwen3.8-27b qwen3.6-35b-a3b gemma4-26b granite-vision granite-docling; do curl -s -X POST localhost:11434/api/models/unload/$m >/dev/null; done
curl -s -X POST localhost:8188/free -H 'Content-Type: application/json' -d '{"unload_models":true,"free_memory":true}' >/dev/null
sleep 2
docker rm -f lsbench >/dev/null 2>&1
docker run -d --name lsbench --entrypoint /app/llama-server --device /dev/dri --device /dev/kfd --group-add 983 --group-add 987 \
  -e GGML_VK_VISIBLE_DEVICES=0 --memory 20g -p 127.0.0.1:18080:8080 -v $HOME/llm/models:/models:ro -v $HOME/llm/llama-cache:/cache:ro \
  ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 --host 0.0.0.0 --port 8080 \
  -m /models/unsloth/gemma-4-26B-A4B-it-qat-GGUF/gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf \
  -fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -ctxcp 8 -cram 2048 -lm dio --jinja --no-webui -t 8 -tb 8 -ub 2048 -b 2048 -c 131072 $extra >/dev/null
until curl -sf localhost:18080/health >/dev/null; do sleep 0.5; docker ps -q -f name=lsbench | grep -q . || { echo dead; exit 1; }; done
n=$(jq length $D/verify-mtp-prompts.json)
for i in $(seq 0 $((n-1))); do
  j=$(jq -c --argjson i $i '{messages:[{role:"user",content:.[$i]}],max_tokens:400,temperature:0,cache_prompt:false}' $D/verify-mtp-prompts.json | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @-)
  echo "$j" | jq -r '.choices[0].message | (.reasoning_content // "") + "\n=====\n" + (.content // "")' > ${LOGDIR:-/tmp}/multi-$label-$i.txt
  echo "$j" | jq -c --arg l $label --argjson i $i '{l:$l,i:$i,n:.usage.completion_tokens,tg:.timings.predicted_per_second,dn:.timings.draft_n,da:.timings.draft_n_accepted}'
done
docker rm -f lsbench >/dev/null
