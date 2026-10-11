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
   - Still **[verify]**: how the reranker was configured before migration. Ollama has no rerank API, so the old `linux6200/bge-reranker-v2-m3` cannot have been serving real reranking.
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

In Open WebUI's `DOCLING_PARAMS` (and any docling-proxy client), point the picture-description API and the VLM API at the in-network alias:

- URL: `http://ollama:11434/v1/chat/completions` (the `ollama` network alias on the `llama-swap` service)
- Models: `granite-vision` for picture description, `granite-docling` for the VLM pipeline

Indicative `DOCLING_PARAMS` fragment, using the current field names:

```json
{
  "picture_description_api": {
    "url": "http://ollama:11434/v1/chat/completions",
    "params": { "model": "granite-vision" }
  },
  "vlm_pipeline_model_api": {
    "url": "http://ollama:11434/v1/chat/completions",
    "params": { "model": "granite-docling" }
  }
}
```

granite-docling output ends with `<|end_of_text|>` after `</doctag>`. Whether docling strips or tolerates it is **[verify]** at cutover (Phase 4).

**Deprecation note.** The docling-serve fields `picture_description_api` and the VLM API fields are deprecated in favour of `picture_description_custom_config` and `vlm_pipeline_custom_config` with engine `"api"`. **[verify the exact JSON]** against the docling-serve usage and configuration docs before use. The field names and nesting in the fragment above are not confirmed.

The compose file already sets `DOCLING_SERVE_ALLOW_CUSTOM_VLM_CONFIG` and `DOCLING_SERVE_ALLOW_CUSTOM_PICTURE_DESCRIPTION_CONFIG` to `"true"` and `DOCLING_SERVE_ENABLE_REMOTE_SERVICES` to `"true"` on `docling-proxy`.

Resolved: granite-vision and granite-docling both work in llama.cpp (docling needs `--special`, which is set in the config). **Fallback** if that regresses: use docling-serve's local engine (transformers on the GPU it already uses). That drops the LLM-server dependency.

Granite models are in the `chat` group, so a docling job swaps the chat model out. Avoid large docling jobs while a 14 GB chat model is loaded, or unload first.

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

## Sources
- https://docs.openwebui.com/getting-started/quick-start/connect-a-provider/starting-with-llama-cpp
- https://docs.openwebui.com/reference/env-configuration
- https://raw.githubusercontent.com/open-webui/open-webui/main/backend/open_webui/retrieval/models/external.py
- https://github.com/docling-project/docling-serve/blob/main/docs/usage.md
- https://github.com/docling-project/docling-serve/blob/main/docs/configuration.md
- https://github.com/n4ze3m/page-assist
- https://github.com/mostlygeek/llama-swap
