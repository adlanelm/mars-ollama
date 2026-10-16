# Client cutover

Status: **planned**; no client has been changed yet (Phase 4). The server side was validated in Phase 3 on `:11435` ([../bench/llama-swap-results.md](../bench/llama-swap-results.md)). Items marked **[verify]** are still unconfirmed; resolve them in Phase 4 of [ROADMAP.md](../ROADMAP.md). URLs below use `:11434`, the port after cutover.

llama-swap has **no Ollama-native API** (`/api/*`). All clients move to the OpenAI-compatible `/v1`. Server config: [llama-swap-config.md](llama-swap-config.md).

Do this in Phase 4, after Phase 3 passes. Before changing anything, screenshot the current settings (ROADMAP Phase 0, task 4).

## Open WebUI

Open WebUI runs in the homelab k3s. Its connection, RAG and reranker settings live in its database and are set through the admin UI. Nothing in the homelab repo needs to change.

1. **Connections** (Admin, Connections):
   - Disable the Ollama connection.
   - Add an OpenAI connection with URL `http://<host>:11434/v1`, provider `llama.cpp`, auth None.
2. **Embeddings** (Admin, Documents):
   - Engine: OpenAI. Base URL: `http://<host>:11434/v1`.
   - Model: `bge-m3` or `snowflake-arctic-embed2`. Keep what each knowledge base uses today.
   - **Do not re-index** if the cosine check passes (at least 0.999 against the Ollama vectors for all 5 texts, ROADMAP Phase 3). If a model fails the check, re-index the knowledge bases that use it.
3. **Reranking** (Admin, Documents):
   - Engine: External. URL: `http://<host>:11434/v1/rerank`. Model: `bge-reranker-v2-m3`.
   - Open WebUI expects `results[].index` and `results[].relevance_score`. Resolved: the llama.cpp response matches. Test with:
     ```bash
     curl -s http://<host>:11434/v1/rerank -H 'Content-Type: application/json' \
       -d '{"model":"bge-reranker-v2-m3","query":"isometric camera","documents":["orthographic camera at 30 degrees","banana bread recipe"],"top_n":2}' | jq .
     ```
   - **Relevance threshold:** keep it at **0**, or set it in logit units. llama.cpp rerank scores are raw logits (they can be negative, not 0-1), and Open WebUI only filters when the threshold is not 0, so a value like 0.3 would silently drop good chunks.
   - How the reranker was configured before migration is **not recoverable** (Ollama data removed, no screenshot). Ollama has no rerank API, so the old `linux6200/bge-reranker-v2-m3` cannot have been serving real reranking. Current traffic: Open WebUI calls `/v1/rerank`, median 1.9 s (2026-10-13). Check in Admin → Settings → Documents: engine External, `http://mars.internal:11434/v1/rerank`, model `bge-reranker-v2-m3`, relevance threshold 0.
   - The reranker runs on the GPU (about 370 MiB), about 0.2 s for 10 chunks.
4. **Task model** (Admin, Interface): set it to **current model**.
   - Why: otherwise title and tag generation request a different model on every message, and llama-swap swaps the chat model out and back in.
5. **Per-model advanced params for coding** (the model's settings in Workspace):
   - Qwen3.6: temperature 0.6, presence penalty 0. The server default (temp 1.0, presence 1.5) is the general thinking setting.
   - Other server defaults match the official model cards (see below), so no change is needed for chat.
6. **Update presets** that still use `qwen3.5:*` or `glm-4.7-flash:*`. Those names have no alias.

Server sampling defaults for reference (clients can override per request):

| Model | Server default (thinking) | Override for |
|---|---|---|
| `qwen3.8-27b` | temp 1.0, top_p 0.95, top_k 20, min_p 0, presence 0 | Non-thinking: 0.7 / 0.8 / 20, presence 1.5 |
| `qwen3.6-35b-a3b` | temp 1.0, top_p 0.95, top_k 20, min_p 0, presence 1.5 | **Coding: temp 0.6, presence 0.** Non-thinking: 0.7 / 0.8, presence 1.5 |
| `gemma4-26b` | temp 1.0, top_p 0.95, top_k 64 | none |
| `granite-vision`, `granite-docling` | temp 0 | none |

Thinking output goes into `reasoning_content` (`--jinja`, `--reasoning-format auto`), which Open WebUI shows as a collapsible thinking block.

## Docling

Open WebUI (jupiter, no GPU) sends documents to **docling-proxy** on mars: `http://mars.internal:8000`. The proxy runs each file in a short-lived docling-serve child on the GPU and keeps one warm child for 30 s. VRAM goes back to the desktop baseline after each job. A plain long-lived docling-serve is not an option on 1.37: it keeps about 2.2 GiB of VRAM, which halves qwen3.8's speed (32.4 → 15.0 tok/s), and every large PDF ratchets its RAM up. Measurements: [../bench/docling-2026-10-16.md](../bench/docling-2026-10-16.md). PDFs over 50 pages are split and merged automatically (`DOCLING_PROXY_MAX_PAGES_PER_PART`).

**Picture description is configured on the server.** `DOCLING_SERVE_CUSTOM_PICTURE_DESCRIPTION_PRESETS` and `DOCLING_SERVE_DEFAULT_PICTURE_DESCRIPTION_PRESET` in `docker-compose.yaml` define the preset:
- Model: `granite-vision-4.1` (Granite Vision 4.1 4B Q8_0) through llama-swap's `/v1/chat/completions`, with 4 concurrent requests, 300 tokens, and pictures of at least 5% of the page.
- Clients only send `do_picture_description=true`. No escaped JSON is needed in Open WebUI.
- 4.1 reads chart labels and most values. Small bars tend to come out as 0. Granite Vision 3.3 (`granite-vision`) only says "a bar chart".
- Running the model locally in docling (preset `granite_vision`, transformers) is no better, and it leaves about 7 GiB of VRAM held in docling.

### Open WebUI settings (Admin → Settings → Documents)

- Content extraction engine: **Docling**, URL `http://mars.internal:8000`, no API key.
- `DOCLING_PARAMS` (flat keys only; Open WebUI passes them through as form fields and overrides its own defaults):
  ```json
  {"do_ocr": true, "force_ocr": false, "ocr_engine": "easyocr", "ocr_lang": ["en", "sv"],
   "pdf_backend": "dlparse_v4", "table_mode": "accurate", "do_table_structure": true,
   "do_picture_description": true}
  ```
  - Leave out `image_export_mode`, `to_formats` and `md_page_break_placeholder`. Open WebUI sets them itself (`placeholder`, `md`+`json`, `\f`) and uses the page breaks for per-page documents.
  - `force_ocr: true` re-OCRs text PDFs as well: 157 s instead of about 10 s for 10 synthetic pages. Use it only for documents with a broken text layer.
- RAG: keep the Markdown header splitter on, chunk size 1000 / overlap 100, and hybrid search with the `bge-reranker-v2-m3` reranker (threshold 0, see above). Leave full-context mode off, because a large PDF would overflow the context.
- Timeouts: Open WebUI's request has no client timeout, and the proxy allows `DOCLING_SERVE_MAX_SYNC_WAIT=36000` s. A 200-page synthetic PDF takes about 66 s without picture descriptions.

**Side effect:** granite-vision-4.1 is in the `chat` group, so an upload with pictures unloads the chat model. The next chat message waits about 15-17 s for it to reload, and granite unloads after 120 s.

### VLM pipeline (granite-docling)

Whole-page conversion through `granite-docling` is still available per request. `pipeline=vlm` with `vlm_pipeline_model_api` → `http://ollama:11434/v1/chat/completions`, model `granite-docling`, `response_format: doctags`. The field is deprecated but works on 1.37. The output is clean: docling strips granite-docling's trailing `<|end_of_text|>` ([verification](../bench/verification-2026-10-13.md)).

## Page Assist (Firefox)

Add an "OpenAI compatible" provider with base URL `http://localhost:11434/v1`. CORS is verified: the server answers the preflight and responses for a `moz-extension://` origin with `Access-Control-Allow-Origin: *`.

## Generic clients

- **Continue.dev and other editors:** an OpenAI-compatible provider at `http://<host>:11434/v1`. Use `qwen3.8-27b` for coding and `qwen3.6-35b-a3b` for long context. For agent or coding configs with Qwen3.6, set temperature 0.6 and presence penalty 0.
- **curl and scripts:** call `/v1/chat/completions` with the model id or an alias:
  ```bash
  curl -s http://<host>:11434/v1/chat/completions -H 'Content-Type: application/json' -d '{
    "model":"qwen3.8-27b","messages":[{"role":"user","content":"Say hi"}],"max_tokens":64}' | jq .
  ```
- **Anthropic-style clients:** `/v1/messages` is available.
- Ollama-native calls (`/api/generate`, `/api/embed`, `/api/tags`) no longer work. Move them to `/v1/chat/completions`, `/v1/embeddings` and `/v1/models`.
- Check what is loaded with `curl http://<host>:11434/running`.

## Alias table

Old Ollama names keep working through llama-swap aliases, for docling params, scripts and Open WebUI presets.

| Old Ollama name | New id |
|---|---|
| `qwen3.8:latest`, `qwen3.8`, `qwen3.8:64k`, `qwen3.8:128k` | `qwen3.8-27b` (48k context; the old 64k/128k tags map here) |
| `qwen3.6` | `qwen3.6-35b-a3b` |
| `gemma4:26b`, `gemma4:26b-64k`, `gemma4:26b-128k` | `gemma4-26b` |
| `ibm/granite3.3-vision:2b` | `granite-vision` |
| `ibm/granite-docling:258m` | `granite-docling` |
| `bge-m3:latest` | `bge-m3` |
| `snowflake-arctic-embed2:latest`, `snowflake-arctic-embed2:latest_8192` | `snowflake-arctic-embed2` |
| `linux6200/bge-reranker-v2-m3:latest` | `bge-reranker-v2-m3` |
| `qwen3.5:*` | **dropped, no alias** (replaced by `qwen3.6-35b-a3b`) |
| `glm-4.7-flash:*` | **dropped, no alias** |

## Rollback

See ROADMAP [Phase 4](../ROADMAP.md): stop llama-swap, start the legacy Ollama, then revert the client settings from the Phase 0 screenshots (about 1 minute).

```bash
docker compose stop llama-swap
docker compose --profile legacy up -d ollama
```

*Removed 2026-10-13 (Phase 5 step 2): the `legacy` service, its data and image are gone, so there is no Ollama rollback any more.*

## Sources
- https://docs.openwebui.com/getting-started/quick-start/connect-a-provider/starting-with-llama-cpp
- https://docs.openwebui.com/reference/env-configuration
- https://raw.githubusercontent.com/open-webui/open-webui/main/backend/open_webui/retrieval/models/external.py
- https://github.com/docling-project/docling-serve/blob/main/docs/usage.md
- https://github.com/docling-project/docling-serve/blob/main/docs/configuration.md
- https://github.com/n4ze3m/page-assist
- https://github.com/mostlygeek/llama-swap
