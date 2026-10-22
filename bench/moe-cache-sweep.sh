#!/usr/bin/env bash
# moe-cache-sweep.sh <model-id> <cache MiB...>: production flags of the model + --moe-cache-mib N, pp/tg on verify-prompt and a 2k-token generation
set -uo pipefail
D=$(dirname "$0"); R=$D/..; S=${LOGDIR:-/tmp}; mkdir -p "$S"; id=$1; shift
IMG=$(grep -m1 -oP 'image: \Kghcr.io/mostlygeek/llama-swap:\S+' $R/docker-compose.yaml)
CHAT="-fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -ctxcp 8 -cram 2048 -lm dio --jinja --no-webui"; MOE="-t 8 -tb 8 -ub 2048 -b 2048 -c 131072"
case $id in
  qwen3.6-35b-a3b) M=/models/unsloth/Qwen3.6-35B-A3B-GGUF; args="-m $M/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf --mmproj $M/mmproj-F16.gguf $CHAT $MOE"; samp='{"temperature":1.0,"top_p":0.95,"top_k":20,"min_p":0,"presence_penalty":1.5}';;
  ornith-1.5-35b-a3b) M=/models/ornith-ai/Ornith-1.5-35B-A3B-GGUF; args="-m $M/Ornith-1.5-35B-Q4_K_M.gguf --mmproj $M/mmproj-Ornith-1.5-35B-BF16.gguf $CHAT $MOE"; samp='{"temperature":0.6,"top_p":0.95,"top_k":20,"min_p":0}';;
  gemma4-26b) M=/models/unsloth/gemma-4-26B-A4B-it-qat-GGUF; args="-m $M/gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf --mmproj $M/mmproj-BF16.gguf $CHAT $MOE --spec-type draft-mtp --spec-draft-model $M/mtp-gemma-4-26B-A4B-it.gguf --spec-draft-n-max 4 --fit-target 2048"; samp='{"temperature":1.0,"top_p":0.95,"top_k":64}';;
  *) echo unknown; exit 1;;
esac
for m in qwen3.6-35b-a3b ornith-1.5-35b-a3b qwen3.8-27b muse-glimmer-30b gemma4-26b granite-vision granite-docling; do curl -s -X POST localhost:11434/api/models/unload/$m >/dev/null; done
docker ps -q -f name=comfyui | grep -q . && { echo "stop ComfyUI first"; exit 1; }; sleep 3
vram() { rocm-smi --showmeminfo vram 2>/dev/null | awk '/GPU\[0\].*Total Used/ {print int($NF/1048576)}'; }
for c in "$@"; do
  docker rm -f lsbench >/dev/null 2>&1
  docker run -d --name lsbench --entrypoint /app/llama-server --device /dev/dri --device /dev/kfd --group-add 983 --group-add 987 -e GGML_VK_VISIBLE_DEVICES=0 --memory 20g \
    -p 127.0.0.1:18080:8080 -v $HOME/llm/models:/models:ro $IMG --host 0.0.0.0 --port 8080 -lv 4 $args --moe-cache-mib $c >/dev/null
  ok=1; until curl -sf localhost:18080/health >/dev/null; do sleep 0.5; docker ps -q -f name=lsbench | grep -q . || { ok=0; break; }; done
  docker logs lsbench > $S/sw-$id-$c.log 2>&1
  [[ $ok == 1 ]] || { echo "{\"id\":\"$id\",\"c\":$c,\"error\":\"$(grep -m1 -E ' E ' $S/sw-$id-$c.log | cut -c1-120)\"}"; continue; }
  ov=$(grep -oP 'Vulkan0 .*: \K\d+ layers \(\d+ overflowing\)' $S/sw-$id-$c.log | tail -1)
  r=(); for i in 1 2; do r+=("$(jq -n --rawfile p $R/bench/verify-prompt.txt '{messages:[{role:"user",content:$p}],max_tokens:256,temperature:0,ignore_eos:true,cache_prompt:false}' \
    | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @- | jq -c '{pp:(.timings.prompt_per_second|round),tg:(.timings.predicted_per_second*10|round/10)}')"); done
  long=$(jq -n --argjson s "$samp" '{messages:[{role:"user",content:"Implement a red-black tree in Python with insert, delete and an invariant checker, then explain it."}],max_tokens:2048,cache_prompt:false} + $s' \
    | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @- | jq -c '{n:.usage.completion_tokens,tg:(.timings.predicted_per_second*10|round/10),dn:.timings.draft_n,da:.timings.draft_n_accepted}')
  echo "{\"id\":\"$id\",\"c\":$c,\"layers\":\"$ov\",\"vram\":$(vram),\"short\":[$(IFS=,; echo "${r[*]}")],\"long\":$long}"
  docker rm -f lsbench >/dev/null
done
