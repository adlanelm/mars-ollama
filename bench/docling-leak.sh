#!/bin/bash
# Leak/VRAM bench: does a long-lived docling-serve creep? Converts each PDF argument in turn with Open WebUI's form fields
# (sync /v1/convert/file, image_export_mode=placeholder, \f page breaks, md+json) and prints one markdown row per conversion:
# container memory after the run, peak memory during it, whole-card VRAM after it. Synthetic PDFs only (bench/make-synthetic-pdf.py).
# Usage: bench/docling-leak.sh URL CONTAINER PDF...   e.g. bench/docling-leak.sh http://localhost:5001 docling a.pdf a.pdf big.pdf
# Env: PICDESC=1 adds picture description via granite-vision (swaps the llama-swap chat model). SETTLE=5 s to wait before sampling.
set -euo pipefail
URL=$1 CTR=$2; shift 2
SETTLE=${SETTLE:-5}
VRAM=$(for c in /sys/class/drm/card*/device; do [ -r "$c/mem_info_vram_total" ] && echo "$(cat "$c/mem_info_vram_total") $c" || true; done | sort -n | tail -1 | cut -d' ' -f2)/mem_info_vram_used
tmp=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$tmp"' EXIT

mib() { # container memory in MiB (cgroup usage as docker stats reports it)
  docker stats --no-stream --format '{{.MemUsage}}' "$CTR" | awk '{v=$1; u=v; gsub(/[0-9.]/,"",u); gsub(/[A-Za-z]/,"",v);
    printf "%d", v * (u=="GiB" ? 1024 : u=="KiB" ? 1/1024 : u=="B" ? 1/1048576 : 1)}'
}
vram() { awk '{printf "%.2f", $1/2^30}' "$VRAM"; }

extra=()
if [ "${PICDESC:-0}" = 1 ]; then
  extra=(-F do_picture_description=true
         -F 'picture_description_api={"url":"http://ollama:11434/v1/chat/completions","params":{"model":"granite-vision"},"timeout":120,"prompt":"Describe this image in a few sentences."}')
fi

echo "| # | file | pages | HTTP | status | s | md chars | \\f | mem after (MiB) | peak (MiB) | VRAM after (GiB) |"
echo "|---|---|---|---|---|---|---|---|---|---|---|"
echo "| 0 | idle | | | | | | | $(mib) | | $(vram) |"
i=0
for pdf in "$@"; do
  i=$((i + 1))
  : > "$tmp/peak"
  (while :; do mib >> "$tmp/peak"; echo >> "$tmp/peak"; sleep 1; done) & sampler=$!
  t0=$(date +%s.%N)
  code=$(curl -s -o "$tmp/out.json" -w '%{http_code}' "$URL/v1/convert/file" \
    -F "files=@$pdf;type=application/pdf" -F image_export_mode=placeholder -F "md_page_break_placeholder=$(printf '\f')" \
    -F to_formats=md -F to_formats=json -F do_ocr=true -F table_mode=accurate -F pdf_backend=dlparse_v4 "${extra[@]}" || echo 000)
  t1=$(date +%s.%N)
  kill $sampler; wait $sampler 2>/dev/null || true
  sleep "$SETTLE"
  read -r status chars ff < <(jq -r '[.status // "?", (.document.md_content // "" | length), (.document.md_content // "" | split("\f") | length - 1)] | @tsv' "$tmp/out.json" 2>/dev/null || echo "? 0 0")
  pages=$(python3 -c 'import re,sys; print(len(re.findall(rb"/Type\s*/Page[^s]", open(sys.argv[1],"rb").read())))' "$pdf")
  peak=$(sort -n "$tmp/peak" | tail -1)
  printf '| %d | %s | %s | %s | %s | %.1f | %s | %s | %s | %s | %s |\n' "$i" "$(basename "$pdf")" "$pages" "$code" "$status" \
    "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')" "$chars" "$ff" "$(mib)" "$peak" "$(vram)"
done
