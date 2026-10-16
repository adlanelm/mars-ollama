#!/bin/bash
# Picture-description comparison: one conversion per variant with Open WebUI's form fields + do_picture_description.
# Prints time, peak whole-card VRAM and every description. Synthetic PDFs only (bench/make-synthetic-pdf.py).
# Usage: bench/docling-picdesc.sh URL PDF VARIANT...
#   VARIANT = api:<llama-swap model id>  (engine "api" via http://ollama:11434)  |  preset:<docling preset id>  (local, in docling)
set -euo pipefail
URL=$1 PDF=$2; shift 2
VRAM=$(for c in /sys/class/drm/card*/device; do [ -r "$c/mem_info_vram_total" ] && echo "$(cat "$c/mem_info_vram_total") $c" || true; done | sort -n | tail -1 | cut -d' ' -f2)/mem_info_vram_used
PROMPT="Describe this image in a few sentences."
tmp=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$tmp"' EXIT

for v in "$@"; do
  kind=${v%%:*} id=${v#*:}
  if [ "$kind" = api ]; then
    cfg=$(jq -cn --arg m "$id" --arg p "$PROMPT" '{engine_options: {engine_type: "api", url: "http://ollama:11434/v1/chat/completions",
      headers: {}, params: {model: $m, max_tokens: 200}, timeout: 120, concurrency: 4},
      model_spec: {name: $m, default_repo_id: $m, prompt: $p, response_format: "plaintext"}, prompt: $p, picture_area_threshold: 0.05}')
    sel=(-F "picture_description_custom_config=$cfg")
  else
    sel=(-F "picture_description_preset=$id")
  fi
  (peak=0; while :; do u=$(cat "$VRAM"); [ "$u" -gt "$peak" ] && peak=$u && echo "$peak" > "$tmp/peak"; sleep 0.5; done) & sampler=$!
  t0=$(date +%s.%N)
  code=$(curl -s -o "$tmp/out.json" -w '%{http_code}' "$URL/v1/convert/file" \
    -F "files=@$PDF;type=application/pdf" -F image_export_mode=placeholder -F "md_page_break_placeholder=$(printf '\f')" \
    -F to_formats=md -F to_formats=json -F do_ocr=true -F table_mode=accurate -F do_picture_description=true "${sel[@]}" || echo 000)
  t1=$(date +%s.%N)
  kill $sampler; wait $sampler 2>/dev/null || true
  echo "## $v"
  echo "HTTP $code, status $(jq -r '.status // "?"' "$tmp/out.json" 2>/dev/null), $(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}') s," \
       "peak VRAM $(awk '{printf "%.2f", $1/2^30}' "$tmp/peak") GiB, errors: $(jq -c '.errors // []' "$tmp/out.json" 2>/dev/null | cut -c1-300)"
  jq -r '.document.json_content.pictures[]? | "- p\(.prov[0].page_no // "?"): " +
    ((.meta.description.text // ([.annotations[]? | select(.kind=="description") | .text] | first) // "(none)") | gsub("\n"; " "))' "$tmp/out.json" 2>/dev/null
  echo
done
