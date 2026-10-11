#!/usr/bin/env bash
# Sample GPU VRAM, container RAM (cgroup memory.peak) and host RAM/NVMe swap once a second while a container runs.
# Stops by itself when the container exits or after MAX seconds (bounded; no pgrep loops).
#   trellis/sample-peaks.sh [container=comfyui-trellis] [max_seconds=3600] > peaks.log
set -uo pipefail
C=${1:-comfyui-trellis}; MAX=${2:-3600}
CARD=/sys/class/drm/card1/device
id=$(docker inspect -f '{{.Id}}' "$C") || exit 1
CG=/sys/fs/cgroup/system.slice/docker-$id.scope
vpeak=0; hmin=999999; nvme=0
for ((i = 0; i < MAX; i++)); do
  [[ $(docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null) == true ]] || break
  v=$(( $(cat $CARD/mem_info_vram_used) / 1048576 ))
  avail=$(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)
  sw=$(swapon --show=NAME,USED --bytes --noheadings | awk '/nvme/{print int($2/1048576)}')
  (( v > vpeak )) && vpeak=$v; (( avail < hmin )) && hmin=$avail; (( ${sw:-0} > nvme )) && nvme=${sw:-0}
  cpeak=$(( $(cat $CG/memory.peak 2>/dev/null || echo 0) / 1048576 ))
  echo "$(date +%T) vram=${v}MiB vram_peak=${vpeak}MiB ctr_ram_peak=${cpeak}MiB host_avail_min=${hmin}MiB nvme_swap_max=${nvme}MiB"
  sleep 1
done
