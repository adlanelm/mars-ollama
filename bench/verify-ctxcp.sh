#!/usr/bin/env bash
# verify-ctxcp.sh <label> <model-id> [extra]  -- growing multi-turn prompts on slot 0; logs RSS and checkpoint lines
set -uo pipefail
label=$1 id=$2; shift 2; extra="$*"; D=$(dirname "$0")
CHAT="-fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -cram 2048 -lm dio --jinja --no-webui"
case $id in
  qwen3.6-35b-a3b) args="-m /models/unsloth/Qwen3.6-35B-A3B-GGUF/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf $CHAT -t 8 -tb 8 -ub 2048 -b 2048 -c 131072" ;;
  qwen3.8-27b) args="-m /models/unsloth/Qwen3.8-27B-GGUF/Qwen3.8-27B-UD-Q3_K_XL.gguf $CHAT -c 49152 -ub 256" ;;
  gemma4-26b) args="-m /models/unsloth/gemma-4-26B-A4B-it-qat-GGUF/gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf $CHAT -t 8 -tb 8 -ub 2048 -b 2048 -c 131072" ;;
esac
for m in qwen3.8-27b qwen3.6-35b-a3b gemma4-26b granite-vision granite-docling; do curl -s -X POST localhost:11434/api/models/unload/$m >/dev/null; done
curl -s -X POST localhost:8188/free -H 'Content-Type: application/json' -d '{"unload_models":true,"free_memory":true}' >/dev/null
sleep 2; docker rm -f lsbench >/dev/null 2>&1
docker run -d --name lsbench --entrypoint /app/llama-server --device /dev/dri --device /dev/kfd --group-add 983 --group-add 987 \
  -e GGML_VK_VISIBLE_DEVICES=0 --memory 20g -p 127.0.0.1:18080:8080 -v $HOME/llm/models:/models:ro \
  ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 --host 0.0.0.0 --port 8080 -lv 4 $args $extra >/dev/null
until curl -sf localhost:18080/health >/dev/null; do sleep 0.5; docker ps -q -f name=lsbench | grep -q . || { echo dead; exit 1; }; done
pid=$(docker inspect -f '{{.State.Pid}}' lsbench)
rss() { awk '/VmRSS/ {print int($2/1024)}' /proc/$pid/status; }
echo "$label $id load rss=$(rss)"
# build a growing conversation: each turn adds ~1.3k tokens of text
msgs='[]'
for k in $(seq 1 12); do
  chunk=$(tail -c +$(( (k-1)*5000 % 26000 + 1 )) $D/verify-prompt.txt | head -c 5000)
  msgs=$(jq -c --arg c "Turn $k notes: $chunk" '. + [{role:"user",content:$c}]' <<<"$msgs")
  j=$(jq -n -c --argjson m "$msgs" '{messages:$m,max_tokens:8,temperature:0,id_slot:0,chat_template_kwargs:{enable_thinking:false}}' | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @-)
  a=$(jq -r '.choices[0].message.content // ""' <<<"$j")
  msgs=$(jq -c --arg a "$a" '. + [{role:"assistant",content:$a}]' <<<"$msgs")
  echo "turn $k prompt=$(jq .usage.prompt_tokens <<<"$j") cached=$(jq .timings.cache_n <<<"$j") rss=$(rss)"
done
docker logs lsbench 2>&1 | grep -i checkpoint | tail -40 > ${LOGDIR:-/tmp}/ckpt-$label-$id.log
docker rm -f lsbench >/dev/null
