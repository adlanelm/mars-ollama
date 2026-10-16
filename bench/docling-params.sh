#!/bin/bash
# Convert one PDF the way Open WebUI's DoclingLoader does: its defaults (image_export_mode=placeholder, \f page breaks, md+json)
# overridden by a DOCLING_PARAMS JSON file. Lists become repeated form fields, objects become JSON strings.
# Writes OUTDIR/{response.json,content.md} and prints status, time, errors and the timings docling reports.
# Usage: bench/docling-params.sh URL PDF PARAMS.json OUTDIR
set -euo pipefail
URL=$1 PDF=$2 PARAMS=$3 OUT=$4
mkdir -p "$OUT"
form=(-F "files=@$PDF;type=application/pdf")
while IFS= read -r arg; do form+=(-F "$arg"); done < <(
  jq -r --arg ff "$(printf '\f')" '({image_export_mode: "placeholder", md_page_break_placeholder: $ff, to_formats: ["md", "json"]} + .)
    | to_entries[] | .key as $k | .value
    | if type == "array" then .[] | "\($k)=\(.)" elif type == "object" then "\($k)=\(tojson)" else "\($k)=\(.)" end' "$PARAMS")
t0=$(date +%s.%N)
code=$(curl -s -o "$OUT/response.json" -w '%{http_code}' "$URL/v1/convert/file" "${form[@]}" || echo 000)
t1=$(date +%s.%N)
jq -r '.document.md_content // ""' "$OUT/response.json" > "$OUT/content.md" 2>/dev/null || true
echo "HTTP $code, status $(jq -r '.status // "?"' "$OUT/response.json" 2>/dev/null), $(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}') s," \
     "errors: $(jq -c '.errors // [] | map(.error_message // .)' "$OUT/response.json" 2>/dev/null | cut -c1-400)," \
     "json: $(jq -r 'if .document.json_content then "yes" else "no" end' "$OUT/response.json" 2>/dev/null)"
