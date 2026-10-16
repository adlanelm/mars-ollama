#!/usr/bin/env bash
# Run SkinTokens/TokenRig (PR #14 ROCm path) on a mesh file or directory, with a VRAM watchdog. See docs/skintokens.md.
#   skintokens/run.sh IN_DIR OUT_DIR [demo.py args, e.g. --use_transfer]
# IN_DIR is mounted read-only at /in, OUT_DIR at /out; every .glb/.fbx/.obj in IN_DIR is rigged to OUT_DIR/<name>.glb.
# Refuses to start while a chat model is loaded or a ComfyUI container is up (not covered by gpu-guard/gpu_arbiter).
# Kills the container if whole-card VRAM stays above VRAM_KILL_MIB (default 15360) for 2 s. Log: OUT_DIR/run.log
set -euo pipefail
IMAGE=${SKINTOKENS_IMAGE:-skintokens-rocm:pr14-b576ed8}
LLM=${LLM_URL:-http://localhost:11434}
KILL=${VRAM_KILL_MIB:-15360}
CARD=/sys/class/drm/card1/device
IN=$(realpath "$1"); OUT=$(realpath -m "$2"); shift 2; mkdir -p "$OUT" ~/llm/skintokens/cache
for c in comfyui comfyui-trellis acestep; do
  [[ $(docker inspect -f '{{.State.Running}}' $c 2>/dev/null) == true ]] && { echo "$c is running: stop it first" >&2; exit 1; }
done
busy=$(curl -sf "$LLM/running" | python3 -c 'import json,sys; print(" ".join(m["model"] for m in json.load(sys.stdin).get("running",[]) if m["model"] not in ("bge-reranker-v2-m3","snowflake-arctic-embed2")))' || true)
[[ -z $busy ]] || { echo "llama-swap has $busy loaded: POST $LLM/api/models/unload/<id> first" >&2; exit 1; }

docker rm -f skintokens >/dev/null 2>&1 || true
docker run -d --name skintokens --device /dev/kfd --device /dev/dri --group-add 983 --group-add 987 --ipc host \
  --security-opt seccomp=unconfined --user 1000:1000 \
  -v ~/llm/skintokens/experiments:/opt/skintokens/experiments:ro -v ~/llm/skintokens/models:/opt/skintokens/models:ro \
  -v ~/llm/skintokens/cache:/cache -v "$IN":/in:ro -v "$OUT":/out \
  "$IMAGE" --input /in --output /out "$@" >/dev/null
over=0; vpeak=0; t0=$SECONDS
while [[ $(docker inspect -f '{{.State.Running}}' skintokens 2>/dev/null) == true ]]; do
  v=$(( $(cat $CARD/mem_info_vram_used) / 1048576 )); (( v > vpeak )) && vpeak=$v
  echo "$(date +%T) vram=${v}MiB peak=${vpeak}MiB" >> "$OUT/peaks.log"
  if (( v > KILL )); then over=$((over + 1)); else over=0; fi
  (( over >= 2 )) && { echo "VRAM ${v} MiB > $KILL MiB: killing" >&2; docker kill skintokens >/dev/null; }
  sleep 1
done
docker logs -t skintokens > "$OUT/run.log" 2>&1
code=$(docker inspect -f '{{.State.ExitCode}}' skintokens); docker rm skintokens >/dev/null
echo "exit $code, wall $((SECONDS - t0)) s, whole-card VRAM peak ${vpeak} MiB (logs in $OUT)" >&2
