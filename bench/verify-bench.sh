#!/usr/bin/env bash
# verify-bench.sh <label> <image> <model-id> [extra flags...]
# Starts a throwaway llama-server container with the production flags of <model-id>,
# measures load time, VRAM, RSS, then a fixed-prompt pp/tg run (3x), then removes the container.
set -uo pipefail
label=$1 image=$2 id=$3; shift 3
extra="$*"
D=$(dirname "$0")
OUT=${OUT:-/tmp/verify-bench.jsonl}
PROMPT_FILE=$D/verify-prompt.txt # fixed prompt: first 14000 bytes of docs/models.md (~5.5k tokens)
[[ -f $PROMPT_FILE ]] || { echo "Read the following technical notes and then write a detailed summary."; head -c 14000 "$D/../docs/models.md"; printf "\n\nSummary:\n"; } > "$PROMPT_FILE"
CHAT="-fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -ctxcp 8 -cram 2048 -lm dio --jinja --no-webui"
MOE="-t 8 -tb 8 -ub 2048 -b 2048"
case $id in
  qwen3.8-27b) args="-m /models/unsloth/Qwen3.8-27B-GGUF/Qwen3.8-27B-UD-Q3_K_XL.gguf --mmproj /models/unsloth/Qwen3.8-27B-GGUF/mmproj-F16.gguf $CHAT -c 49152 --no-mmproj-offload -ub 256" ;;
  qwen3.6-35b-a3b) args="-m /models/unsloth/Qwen3.6-35B-A3B-GGUF/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf --mmproj /models/unsloth/Qwen3.6-35B-A3B-GGUF/mmproj-F16.gguf $CHAT $MOE -c 131072" ;;
  gemma4-26b) args="-m /models/unsloth/gemma-4-26B-A4B-it-qat-GGUF/gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf --mmproj /models/unsloth/gemma-4-26B-A4B-it-qat-GGUF/mmproj-BF16.gguf $CHAT $MOE -c 131072" ;;
  *) echo "unknown id"; exit 1 ;;
esac
vram() { rocm-smi --showmeminfo vram 2>/dev/null | awk '/GPU\[0\].*Total Used/ {print int($NF/1048576)}'; }
# safety: nothing else big on the GPU
curl -s -X POST localhost:8188/free -H 'Content-Type: application/json' -d '{"unload_models":true,"free_memory":true}' >/dev/null || true
for m in qwen3.8-27b qwen3.6-35b-a3b gemma4-26b granite-vision granite-docling colibri-big; do
  curl -s -X POST localhost:11434/api/models/unload/$m >/dev/null || true
done
sleep 3
v0=$(vram)
[[ $v0 -gt 2600 ]] && { echo "VRAM busy before start: ${v0} MiB, abort"; exit 2; }
docker rm -f lsbench >/dev/null 2>&1
t0=$(date +%s.%N)
docker run -d --name lsbench --entrypoint /app/llama-server \
  --device /dev/dri --device /dev/kfd --group-add 983 --group-add 987 \
  -e GGML_VK_VISIBLE_DEVICES=0 -e HIP_VISIBLE_DEVICES=0 \
  --memory 20g -p 127.0.0.1:18080:8080 \
  -v $HOME/llm/models:/models:ro -v $HOME/llm/llama-cache:/cache:ro \
  "$image" --host 0.0.0.0 --port 8080 -lv 3 $args $extra >/dev/null
ok=0
for i in $(seq 1 600); do
  if curl -sf localhost:18080/health >/dev/null 2>&1; then ok=1; break; fi
  if ! docker ps -q -f name=lsbench | grep -q .; then break; fi
  sleep 0.5
done
t1=$(date +%s.%N)
load=$(python3 -c "print(round($t1-$t0,1))")
if [[ $ok != 1 ]]; then
  echo "load failed"; docker logs lsbench 2>&1 | tail -30 > ${LOGDIR:-/tmp}/fail-$label-$id.log; docker rm -f lsbench >/dev/null; exit 3
fi
v1=$(vram)
pid=$(docker inspect -f '{{.State.Pid}}' lsbench)
rss=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$pid/status 2>/dev/null)
docker logs lsbench > ${LOGDIR:-/tmp}/full-$label-$id.log 2>&1; grep -i -E "fit|offloaded|CPU_Mapped|ROCm0 model buffer|Vulkan0 model buffer|KV buffer|compute buffer|draft|mtp" ${LOGDIR:-/tmp}/full-$label-$id.log > ${LOGDIR:-/tmp}/log-$label-$id.txt
# fixed prompt, 3 runs, no prompt cache
res=()
for r in 1 2 3; do
  j=$(jq -n --rawfile p "$PROMPT_FILE" '{messages:[{role:"user",content:$p}],max_tokens:256,temperature:0,ignore_eos:true,cache_prompt:false}' \
    | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @-)
  res+=("$(echo "$j" | jq -c '{pn:.timings.prompt_n,pp:.timings.prompt_per_second,tg:.timings.predicted_per_second,dn:.timings.draft_n,da:.timings.draft_n_accepted}')")
  [[ $r == 1 ]] && echo "$j" | jq -r '.choices[0].message | (.reasoning_content // "") + "\n=====\n" + (.content // "")' > ${LOGDIR:-/tmp}/out-$label-$id.txt
done
v2=$(vram)
rss2=$(awk '/VmRSS/ {print int($2/1024)}' /proc/$pid/status 2>/dev/null)
docker rm -f lsbench >/dev/null
line=$(jq -n -c --arg label "$label" --arg id "$id" --arg extra "$extra" --argjson load "$load" \
  --argjson v0 "$v0" --argjson v1 "$v1" --argjson v2 "$v2" --argjson rss "${rss:-0}" --argjson rss2 "${rss2:-0}" \
  --argjson runs "[$(IFS=,; echo "${res[*]}")]" \
  '{label:$label,id:$id,extra:$extra,load_s:$load,vram_before:$v0,vram_loaded:$v1,vram_after_runs:$v2,rss_mib:$rss,rss_after_mib:$rss2,runs:$runs}')
echo "$line" | tee -a $OUT
