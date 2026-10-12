#!/usr/bin/env bash
# Run one ACE-Step generation batch on the GPU, with a VRAM watchdog. See docs/acestep.md.
#   acestep/run.sh --caption "..." --duration 75 --seeds 11,22,33 --name station [--lm acestep-5Hz-lm-0.6B | --no-lm]
# Output: ~/llm/acestep/out/<name>_s<seed>.{wav,json}; peaks log: ~/llm/acestep/out/<name>.peaks.log
# Not covered by gpu-guard/gpu_arbiter: refuses to start while a chat model is loaded or a ComfyUI container is up.
# /ckpt is mounted rw: upstream overwrites the HF copies of acestep-v15-turbo/*.py with the pinned repo's acestep/models/turbo/*.py.
# The watchdog kills the container if whole-card VRAM stays above VRAM_KILL_MIB (default 15360) for 2 s.
set -euo pipefail
IMAGE=${ACESTEP_IMAGE:-acestep-rocm:1.5-ca1e85f}
LLM=${LLM_URL:-http://localhost:11434}
KILL=${VRAM_KILL_MIB:-15360}
CARD=/sys/class/drm/card1/device
OUT=~/llm/acestep/out; mkdir -p "$OUT" ~/llm/acestep/cache
name=gen; for ((i = 1; i <= $#; i++)); do [[ ${!i} == --name ]] && { j=$((i + 1)); name=${!j}; }; done

for c in comfyui comfyui-trellis; do
  [[ $(docker inspect -f '{{.State.Running}}' $c 2>/dev/null) == true ]] && { echo "$c is running: stop it first" >&2; exit 1; }
done
busy=$(curl -sf "$LLM/running" | python3 -c 'import json,sys; print(" ".join(m["model"] for m in json.load(sys.stdin).get("running",[]) if m["model"] not in ("bge-m3","bge-reranker-v2-m3","snowflake-arctic-embed2")))' || true)
[[ -z $busy ]] || { echo "llama-swap has $busy loaded: POST $LLM/api/models/unload/<id> first" >&2; exit 1; }

docker rm -f acestep >/dev/null 2>&1 || true
docker run -d --name acestep --device /dev/kfd --device /dev/dri --group-add 983 --group-add 987 --ipc host \
  --security-opt seccomp=unconfined --user 1000:1000 \
  -v ~/llm/acestep/checkpoints:/ckpt -v "$OUT":/out -v ~/llm/acestep/cache:/cache \
  "$IMAGE" "$@" >/dev/null
echo "started; watchdog at $KILL MiB" >&2
over=0; vpeak=0; t0=$SECONDS
while [[ $(docker inspect -f '{{.State.Running}}' acestep 2>/dev/null) == true ]]; do
  v=$(( $(cat $CARD/mem_info_vram_used) / 1048576 )); (( v > vpeak )) && vpeak=$v
  echo "$(date +%T) vram=${v}MiB peak=${vpeak}MiB" >> "$OUT/$name.peaks.log"
  if (( v > KILL )); then over=$((over + 1)); else over=0; fi
  (( over >= 2 )) && { echo "VRAM ${v} MiB > $KILL MiB: killing" >&2; docker kill acestep >/dev/null; }
  sleep 1
done
docker logs acestep 2>&1 | grep -E '^\[gen\]|Error|Traceback' || true
echo "wall $((SECONDS - t0)) s, whole-card VRAM peak ${vpeak} MiB (log $OUT/$name.peaks.log)" >&2
docker rm acestep >/dev/null
