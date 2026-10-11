# GPU switching

Status: `bin/gpu-mode` exists; the `comfyui` service does not yet (Phase 6 is pending). llama-swap behaviour measured in Phase 3 ([../bench/llama-swap-results.md](../bench/llama-swap-results.md)). Items marked **[verify]** are still unconfirmed.

See also: [architecture.md](architecture.md), [colibri.md](colibri.md), Phase 6 in [../ROADMAP.md](../ROADMAP.md).

## VRAM consumers

The RX 7800 XT has 16 GB, with about 1.13-1.2 GB used by the desktop at idle.

| Consumer | Size | Notes |
|---|---|---|
| llama-swap chat model | about 12-15 GB | One at a time (exclusive group `chat`) |
| ComfyUI | e.g. Krea 2 Turbo fp8 about 13 GB | Runs as a container |
| docling-proxy | varies | Layout and OCR also use the GPU. Avoid large jobs while a 14 GB chat model is loaded, or unload first |
| Desktop | about 1.13-1.2 GB | Frees if the monitor moves to the iGPU |
| Unity and Blender | another 1-3 GiB | Open while game-dev work is going on |

Goal: exactly one large VRAM consumer at a time.

## How llama-swap switches LLMs

- **Exclusive chat group:** `chat` has `swap: true, exclusive: true`. Requesting one member unloads the others, including `colibri-big`.
- **Persistent aux:** `aux` has `persistent: true, swap: false, exclusive: false`. Always loaded: the embedders run on the CPU, the **reranker on the GPU (about 370 MiB)**. They are loaded once at startup by `hooks.on_startup.preload`.
- **`ttl`:** idle models unload after 900 s (chat), 120 s (granite), never for aux (0).
- **Manual unload:** `POST /api/models/unload` unloads **everything, including the persistent aux models**, and preload does **not** re-run. Re-warm each aux model with one request (`/v1/embeddings` for `bge-m3` and `snowflake-arctic-embed2`, `/v1/rerank` for `bge-reranker-v2-m3`); otherwise the first RAG request pays the load time. `bin/gpu-mode comfy` does this.
- A swap takes about 10-20 s from page cache.

So `gpu-mode` only handles the **non-LLM** consumers.

## `bin/gpu-mode` spec

| Subcommand | Action |
|---|---|
| `comfy` | `POST /api/models/unload`, re-warm the three aux models (one request each), then start ComfyUI with `docker compose --profile comfy up -d comfyui` if that service is defined |
| `llm` | `POST /free` to ComfyUI, or `llm --stop` to stop its container. llama-swap loads chat models on the next request |
| `status` | Print: VRAM/GTT, `/running`, ComfyUI state, free RAM, swap |

HTTP calls:

| Target | Call |
|---|---|
| llama-swap unload | `POST http://localhost:11434/api/models/unload` (then re-warm aux) |
| llama-swap loaded models | `GET http://localhost:11434/running` |
| ComfyUI free VRAM | `POST <comfy>/free` body `{"unload_models":true}` |

The `comfyui` service uses `profiles: ["comfy"]`, so it doesn't start automatically. The ComfyUI URL (`COMFY_URL`) defaults to port 8188 **[verify]**. `LLM_URL` defaults to `:11434`, so before cutover run it with `LLM_URL=http://localhost:11435`.

### Implementation

See [`bin/gpu-mode`](../bin/gpu-mode) (`gpu-mode {llm [--stop]|comfy|status}`). The part that matters after an unload:

```bash
# /api/models/unload also unloads the persistent aux group and preload does not re-run: one request each brings them back
curl -sf "$LLM/v1/embeddings" -H 'Content-Type: application/json' -d '{"model":"bge-m3","input":"warm"}'
curl -sf "$LLM/v1/embeddings" -H 'Content-Type: application/json' -d '{"model":"snowflake-arctic-embed2","input":"warm"}'
curl -sf "$LLM/v1/rerank" -H 'Content-Type: application/json' -d '{"model":"bge-reranker-v2-m3","query":"warm","documents":["warm"]}'
```

The re-warmed reranker puts about 370 MiB back on the GPU, so ComfyUI has that much less VRAM.

Acceptance: `gpu-mode comfy`, then `gpu-mode llm`, then `gpu-mode status` leaves exactly one large VRAM consumer, and the NVMe swap stays at 0 B.

## Game-dev session guidance

Unity and Blender take another 1-3 GiB of VRAM. Qwen3.8 then can't keep 48k on the GPU, and `--fit` spills dense layers to RAM, which is slow.

- While the editors are open, use **qwen3.6-35b-a3b**: `--fit` moves experts instead, which degrades gracefully.
- Or use Claude Code.
- Run colibri only with Unity, Blender and ComfyUI closed ([colibri.md](colibri.md)).

## Troubleshooting

| Symptom | Check |
|---|---|
| **OOM** at load or during generation | `gpu-mode status`. Look for a second VRAM consumer (ComfyUI, docling job, Unity, Blender). Run `gpu-mode llm` or close the editors, then retry. `--fit` adapts at load time, so reload the model |
| **Slow tokens** | Run `swapon --show`. If the NVMe partition is in use, RAM is exhausted: unload models, close apps, use a smaller context or qwen3.6 instead of colibri. Also check `--fit` moved layers (dense qwen3.8) to the CPU |
| **Model stuck loading** | Large models load slowly from cold disk. Raise `healthCheckTimeout` (`600` in the live config) and check `docker logs llama-swap`. For `-hf` downloads, pre-pull with `llm-pull` so the first request doesn't download |
