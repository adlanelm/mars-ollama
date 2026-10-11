#!/usr/bin/env bash
# Phase 3 fit check: a near-full-context prompt per model; reports prompt size/speed, VRAM, RAM, swap.
set -uo pipefail
BASE=${BASE:-http://localhost:11435}
run() { # model repeat_count
  local body; body=$(python3 -c "
import json,sys
s='Log entry: the reactor hums behind a frosted window on deck three while the hatch stays sealed. '
print(json.dumps({'model':sys.argv[1],'max_tokens':32,'messages':[{'role':'user','content':s*int(sys.argv[2])+'\nHow many log entries are there, roughly? Answer in one short sentence.'}]}))" "$1" "$2")
  local t0=$SECONDS r; r=$(curl -s "$BASE/v1/chat/completions" -H "Content-Type: application/json" -d @- <<<"$body")
  echo "$r" | jq -c --arg m "$1" --argjson s $((SECONDS-t0)) '{model:$m, secs:$s, prompt_n:.timings.prompt_n, pp:.timings.prompt_per_second, tg:.timings.predicted_per_second, err:.error.message}'
  echo "  vram=$(( $(cat /sys/class/drm/card1/device/mem_info_vram_used)/1048576 ))MiB gtt=$(( $(cat /sys/class/drm/card1/device/mem_info_gtt_used)/1048576 ))MiB avail=$(free -m | awk '/Mem:/{print $7}')MiB nvme_swap=$(swapon --show=NAME,USED --noheadings | awk '/nvme/{print $2}')"
}
run qwen3.8-27b ${N38:-2300}
run qwen3.6-35b-a3b ${N36:-6000}
run gemma4-26b ${NG:-6000}
