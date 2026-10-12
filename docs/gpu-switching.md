# GPU switching

Status: switching is automatic since 2026-10-12 ([below](#automatic-arbitration-llama-swap---comfyui)). `bin/gpu-mode` and the `comfyui` service (`comfyui-rocm:0.39.2`, profile `comfy`, port 127.0.0.1:8188) exist; Phase 6 is done, see [../bench/comfyui-results.md](../bench/comfyui-results.md). llama-swap behaviour measured in Phase 3 ([../bench/llama-swap-results.md](../bench/llama-swap-results.md)). Items marked **[verify]** are still unconfirmed.

See also: [architecture.md](architecture.md), [colibri.md](colibri.md), Phase 6 in [../ROADMAP.md](../ROADMAP.md).

## VRAM consumers

The RX 7800 XT has 16 GB, with about 1.13-1.2 GB used by the desktop at idle.

| Consumer | Size | Notes |
|---|---|---|
| llama-swap chat model | about 12-15 GB | One at a time (exclusive group `chat`) |
| ComfyUI | measured 14-16 GiB whole-card during generation (Krea 2 Turbo fp8 takes the full card) | Runs as a container |
| docling-proxy | varies | Layout and OCR also use the GPU. Avoid large jobs while a 14 GB chat model is loaded, or unload first |
| Desktop | about 1.13-1.2 GB | Frees if the monitor moves to the iGPU |
| Unity and Blender | another 1-3 GiB | Open while game-dev work is going on |

Goal: exactly one large VRAM consumer at a time.

### ComfyUI `--reserve-vram 1.5`

Added 2026-10-13 to the `comfyui` service (`command:` in [../docker-compose.yaml](../docker-compose.yaml), the Dockerfile CMD plus `--reserve-vram 1.5`; the two lists must be kept in step). Reason: Krea 2 Turbo 1024 px peaked at 16.19 GiB whole-card (desktop included) and Flux.2 klein 512 at 16.2 GiB on a 16 GiB card, so the KDE desktop was squeezed. Measured with `RUNS=3 bin/comfy-bench krea2_turbo` (6 steps, 1024 px, HTTP API, no LLM loaded, container recreated per trial), whole-card `mem_info_vram_used` sampled every 0.5 s:

| `--reserve-vram` | Peak VRAM (GiB) | Peak GTT (GiB) | Timed images (s) |
|---|---|---|---|
| none | 15.78 | 0.90 | 30.9, 33.6 |
| 1.5 | 15.15, 14.83, 14.55 (three runs) | 0.93-1.40 | 31.7, 32.6 / 39.4, 34.2 / 33.8, 33.1 |
| 2.0 | 15.79, 15.80 (two runs) | 1.4-1.75 | 30.4, 30.5 / 33.2, 32.8 |
| 3.0 | not usable: system RAM ran out (available 0 GiB) while the model was being offloaded, run aborted | - | - |

- **Value 1.5:** peak drops by about 0.6-1.2 GiB to 14.5-15.2 GiB, time per image unchanged within noise (about 31-34 s, the 39.4 s is a single outlier). 2.0 showed no gain over no flag in two runs: the peak is set by the VAE decode and the torch cache, which the reserve does not cover, so more reserve only adds offloading and RAM use. Do not raise it above 1.5 without watching free RAM.
- **Not repeated:** Flux.2 klein 512 with the flag, since the setting is global to the service.
- **`comfyui-trellis`: not set.** TRELLIS peaks at 4.1 GiB, far below the card.

## Automatic arbitration (llama-swap <-> ComfyUI)

Added 2026-10-12. Neither side needs `gpu-mode` any more; it stays as manual override and status view.

| Side | Mechanism | What it does |
|---|---|---|
| llama-swap | [`llama-swap/gpu-guard`](../llama-swap/gpu-guard), the `cmd` prefix (macro `${gpu_guard}`) of every chat-group model. llama-swap v263 has no pre-start hook (only `hooks.on_startup`) | Before the server starts: wait while `comfyui` or `comfyui-trellis` has a running/pending job or a prompt being admitted (`GET /queue`, `GET /gpu-arbiter`), then `POST /free {"unload_models":true,"free_memory":true}` to each that is up, wait until torch holds < 512 MiB, then `exec` the server. Stopped containers are skipped. Max wait 600 s (`GPU_GUARD_MAX_WAIT`), then exit 1: the request fails and the log says `[gpu-guard] giving up` |
| ComfyUI | [`comfyui/gpu_arbiter`](../comfyui/gpu_arbiter/__init__.py), our own node package (MIT, no nodes, no deps), bind-mounted read-only into both containers. An aiohttp middleware on `POST /prompt` and `/api/prompt` | Before the prompt is queued: wait while a guarded model is loading or an in-flight llama-swap request (inflight snapshot on `/api/events`) targets a non-aux model, then `POST /api/models/unload/<id>` for each running guarded model and wait until sysfs shows the VRAM released. The aux models (no `gpu-guard` in their cmd) stay loaded, so nothing needs re-warming. Max wait 600 s (`GPU_ARBITER_MAX_WAIT`), then HTTP 503 `gpu_busy`. llama-swap unreachable: prompts pass unchanged |

- **Race:** the node sets `admitting` (visible on `GET /gpu-arbiter`), then checks llama-swap again before unloading. An LLM request is in flight before gpu-guard runs, so one side always sees the other; on a tie the LLM goes first. A streaming answer is never cut off: the prompt waits for it.
- **Network:** both ComfyUI services join the `ollama` network (`http://llama-swap:11434`; llama-swap reaches `http://comfyui:8188` and `http://comfyui-trellis:8188`). `healthCheckTimeout` is 1230 s to cover the guard wait plus a cold load.
- **Why VRAM overflowed before:** ComfyUI's free-memory figure (HIP) does not count llama.cpp's Vulkan buffers, so it loaded everything on top of a resident LLM and spilled into GTT. The node therefore reads `mem_info_vram_used` from sysfs.
- **Not arbitrated:** `comfyui` vs `comfyui-trellis` (free or stop one first), docling-proxy's own GPU use, Unity/Blender.
- **Krita:** unchanged URL and websocket. While an LLM request is running, a Krita job sits in "waiting" until it finishes; Krita sends `POST /prompt` without a timeout.

Measured 2026-10-12 (qwen3.6-35b-a3b, Flux.2 Klein 4B 512 px, desktop baseline about 0.9-1.3 GiB):

| Switch | Added latency | VRAM / GTT |
|---|---|---|
| LLM loaded and idle -> ComfyUI prompt | 0.3-0.5 s (unload; 14.9 GiB -> 0.9 GiB) | job peak 16.2 GiB VRAM (the same as ComfyUI alone: 16.2), GTT flat at about 0.5 GiB |
| ComfyUI holding models -> chat | 2.8-6.2 s `/free` (weights move to RAM), plus the normal 15-17 s load | LLM peak 15.2 GiB; its own GTT is about 11.9 GiB as before |
| Chat sent 1.5 s into a running job | waits for the job (5 s), then `/free` + load: 24.6 s total | no overlap |
| Prompt sent 2 s into a 13 s LLM stream | prompt queued 1.2 s after the stream ended (12.5 s wait) | stream completed, then unload |

Thrash cost: alternating requests pay a full LLM load (about 15 s) and a ComfyUI model reload from RAM every time.

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
| `llm` | `POST /free` to ComfyUI, or `llm --stop` to stop its container. Without `--stop` the idle container keeps about 0.1 GiB and VRAM returns to baseline within seconds. llama-swap loads chat models on the next request |
| `status` | Print: VRAM/GTT, `/running`, ComfyUI state (torch-held VRAM, queue, arbiter idle/waiting/unloading), free RAM, swap |

HTTP calls:

| Target | Call |
|---|---|
| llama-swap unload | `POST http://localhost:11434/api/models/unload` (then re-warm aux) |
| llama-swap loaded models | `GET http://localhost:11434/running` |
| ComfyUI free VRAM | `POST <comfy>/free` body `{"unload_models":true}` |

The `comfyui` service uses `profiles: ["comfy"]`, so it doesn't start automatically. The ComfyUI URL (`COMFY_URL`) defaults to port 8188 (verified). `LLM_URL` defaults to `:11434`, so before cutover run it with `LLM_URL=http://localhost:11435`.

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
| **Chat request or ComfyUI prompt hangs** | Expected while the other side works (max 600 s). `docker logs llama-swap \| grep gpu-guard`, `docker logs comfyui \| grep gpu_arbiter`, `bin/gpu-mode status` |
| **OOM** at load or during generation | `gpu-mode status`. Look for a second VRAM consumer (ComfyUI, docling job, Unity, Blender). Run `gpu-mode llm` or close the editors, then retry. `--fit` adapts at load time, so reload the model |
| **Slow tokens** | Run `swapon --show`. If the NVMe partition is in use, RAM is exhausted: unload models, close apps, use a smaller context or qwen3.6 instead of colibri. Also check `--fit` moved layers (dense qwen3.8) to the CPU |
| **Model stuck loading** | Large models load slowly from cold disk. Raise `healthCheckTimeout` (`1230` in the live config: 600 s gpu-guard wait + cold load) and check `docker logs llama-swap`. For `-hf` downloads, pre-pull with `llm-pull` so the first request doesn't download |
