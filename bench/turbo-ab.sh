#!/usr/bin/env bash
# turbo-ab.sh <label> <gguf under ~/llm/models> <sampling json> [extra flags]
# qwen3.8-27b production flags in a throwaway lsbench container; turbo-prompts.json in two passes:
# greedy 400 tok (speed, MTP acceptance) and sampled up to 16k tok (thinking length, answers). Outputs in $LOGDIR.
set -uo pipefail
label=$1 gguf=$2 samp=$3; shift 3; extra="$*"; D=$(dirname "$0"); L=${LOGDIR:-/tmp}; mkdir -p "$L"
for m in qwen3.8-27b qwen3.6-35b-a3b gemma4-26b granite-vision granite-docling; do curl -s -X POST localhost:11434/api/models/unload/$m >/dev/null; done
docker ps -q -f name=comfyui | grep -q . && { echo "stop ComfyUI first"; exit 1; }
sleep 2
docker rm -f lsbench >/dev/null 2>&1
docker run -d --name lsbench --entrypoint /app/llama-server --device /dev/dri --device /dev/kfd --group-add 983 --group-add 987 \
  -e GGML_VK_VISIBLE_DEVICES=0 --memory 20g -p 127.0.0.1:18080:8080 -v $HOME/llm/models:/models:ro \
  ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 --host 0.0.0.0 --port 8080 -m /models/$gguf \
  -fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -ctxcp 8 -cram 2048 -lm dio --jinja --no-webui \
  ${CTX:--c 49152} -ub 256 -lv 4 $extra >/dev/null
until curl -sf localhost:18080/health >/dev/null; do sleep 0.5; docker ps -q -f name=lsbench | grep -q . || { docker logs lsbench 2>&1 | tail; echo dead; exit 1; }; done

vram() { rocm-smi --showmeminfo vram --json 2>/dev/null | grep '^{' | jq '.card0["VRAM Total Used Memory (B)"]|tonumber/1073741824*100|round/100'; }
peak=0
n=$(jq length $D/turbo-prompts.json)
for pass in greedy sampled; do
  if [ $pass = greedy ]; then opts='{"max_tokens":400,"temperature":0}'; else opts=$(jq -c '. + {max_tokens:16384}' <<<"$samp"); fi
  for i in $(seq 0 $((n-1))); do
    t0=$(date +%s%N)
    j=$(jq -c --argjson i $i --argjson o "$opts" '{messages:[{role:"user",content:.[$i]}],cache_prompt:false} + $o' $D/turbo-prompts.json \
        | curl -s localhost:18080/v1/chat/completions -H 'Content-Type: application/json' -d @-)
    wall=$(awk -v a=$t0 -v b=$(date +%s%N) 'BEGIN{printf "%.1f", (b-a)/1e9}')
    v=$(vram); peak=$(awk -v a=$v -v b=$peak 'BEGIN{print (a>b)?a:b}')
    rc=$(echo "$j" | jq -r '.choices[0].message.reasoning_content // ""')
    rt=$(jq -n --arg c "$rc" '{content:$c}' | curl -s localhost:18080/tokenize -H 'Content-Type: application/json' -d @- | jq '.tokens|length')
    echo "$j" | jq -r '.choices[0].message | (.reasoning_content // "") + "\n=====\n" + (.content // "")' > "$L/turbo-$label-$pass-$i.txt"
    echo "$j" | jq -c --arg l $label --arg p $pass --argjson i $i --argjson rt $rt --arg w $wall \
      '{l:$l,p:$p,i:$i,think:$rt,n:.usage.completion_tokens,fin:.choices[0].finish_reason,wall:($w|tonumber*10|round/10),
        tg:(.timings.predicted_per_second*10|round/10),pp:(.timings.prompt_per_second|round),dn:.timings.draft_n,da:.timings.draft_n_accepted}'
  done
done
echo "{\"l\":\"$label\",\"peak_vram_gib\":$peak}"
docker logs lsbench > "$L/full-$label.log" 2>&1
grep -i -E "projected|will leave|offloaded|model buffer|KV buffer|compute buffer|draft|mtp|spec" "$L/full-$label.log" | sed "s/^/[$label] /"
docker rm -f lsbench >/dev/null
