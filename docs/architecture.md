# Architecture: current vs target stack

Status: llama-swap is **running side by side with Ollama on `:11435`** (Phases 0-3 done, 2026-10-11). Cutover to `:11434` (Phase 4) is pending, so the target stack below is not yet in place. Measurements: [../bench/llama-swap-results.md](../bench/llama-swap-results.md). Items marked **[verify]** are still unconfirmed.

See also: [../ROADMAP.md](../ROADMAP.md), [colibri.md](colibri.md), [gpu-switching.md](gpu-switching.md).

## Current stack

| Component | Where | Settings |
|---|---|---|
| `ollama` (**removed 2026-10-13**, replaced by llama-swap) | container `ollama`, port `11434` | Image `ollama/ollama:rocm` (0.35.1), `OLLAMA_VULKAN=1`, flash attention on, q8_0 KV cache, `OLLAMA_MAX_LOADED_MODELS=2`, `OLLAMA_KEEP_ALIVE=35`, `mem_limit: 28g`, `cpus: "12.8"`, models set `num_thread 13`. Data in `$HOME/llm/ollama/data` (78 GB) |
| `docling-proxy` | container `docling-proxy`, host port `8000` (container `8080`) | **Also uses the GPU** (`/dev/dri`, `/dev/kfd`) for layout and OCR. Output archive in `$HOME/llm/docling` |
| Open WebUI | homelab k3s (not in this repo) | Connection, RAG and reranker settings live in its database and are set through the UI |

Both containers share the compose network `ollama`.

## Target stack

llama-swap replaces the Ollama container **in place**. It keeps host port `11434` and the network alias `ollama`, so existing clients keep the same host and port. All clients move from `/api/*` to `/v1`, because llama-swap has no Ollama-native API.

```
 Clients
 ┌────────────┐ ┌─────────┐ ┌─────────────┐ ┌───────────────────┐
 │ Open WebUI │ │ docling │ │ Page Assist │ │ Continue / scripts│
 │ (k3s)      │ │ proxy   │ │ (Firefox)   │ │                   │
 └─────┬──────┘ └────┬────┘ └──────┬──────┘ └─────────┬─────────┘
       └─────────────┴──────┬──────┴──────────────────┘
                            │  /v1/...   host :11434  (container :8080,
                            ▼            network alias "ollama")
                 ┌─────────────────────┐
                 │      llama-swap     │  starts, stops and proxies
                 └──────────┬──────────┘  llama-server processes
          ┌─────────────────┴───────────────────────────┐
          ▼                                             ▼
 group `chat`  (swap: true, exclusive: true)    group `aux`  (persistent)
 one member loaded at a time                    always loaded
 ├─ qwen3.8-27b                                 ├─ bge-m3 (CPU)
 ├─ qwen3.6-35b-a3b                             ├─ snowflake-arctic-embed2 (CPU)
 ├─ gemma4-26b                                  └─ bge-reranker-v2-m3 (GPU, ~370 MiB)
 ├─ granite-vision
 ├─ granite-docling
 └─ colibri-big  (coli serve, internal ${PORT})
```

- Requesting a `chat` member unloads the current one. The `aux` models stay loaded (always loaded; embedders on CPU, reranker on GPU). `POST /api/models/unload` is the exception: it unloads them too, and preload does not re-run, so re-warm each with one request ([gpu-switching.md](gpu-switching.md)).
- Requesting `colibri-big` therefore unloads the chat model automatically, with no manual step.
- `ttl`: 900 s for chat models, 120 s for the granite models, 0 for aux.
- Config: [llama-swap-config.md](llama-swap-config.md) (live file `llama-swap/config.yaml`). Today the service has no `ollama` alias and listens on `11435`; the alias and `11434` come at cutover.

## Machine facts

| Part | Spec | Implication |
|---|---|---|
| CPU | Ryzen 7 7700, **8 physical cores** / 16 threads | MoE expert offload uses CPU threads. Measured: `-t` 6, 8 and 12 are within noise, `-t 8` is pinned |
| dGPU | RX 7800 XT, 16 GB (15.98 GiB), gfx1101 | The desktop uses about **1.13-1.2 GB** when idle, so about 14.8 GiB is usable. Moving the monitor to the iGPU frees it |
| RAM | 30 GiB | Host uses about 10 GiB with Ollama idle. CPU embedders take part of the rest (measured: bge-m3 1.3 GiB, snowflake 1.3 GiB, reranker 0.5 GiB RSS ≈ 3.1 GiB). At most about 12-13 GiB is left for MoE experts. Measured with qwen3.6 at 126k context: 11.4 GB RAM still available |
| Swap | zram 30 GiB (priority 100) + NVMe partition 30 GiB (priority -1) | Avoid filling zram, or the system pages to NVMe |
| Disk | NVMe `/home`: **324 GB free** of 777 GB (checked 2026-10-11); `/` has 23 GB free | Only the NVMe is usable for models and colibri. About 345 GB free after migration |

## VRAM and RAM budget

Usable VRAM is about 14.85 GiB. `--fit` (default on) keeps a 1 GiB margin per device. KV is at q8_0.

| Model | Weights | mmproj | KV at context | Placement | `--fit` |
|---|---|---|---|---|---|
| `qwen3.8-27b` (dense, UD-Q3_K_XL) | 13.15 GB (12.24 GiB) | 0.93 GB F16, **on CPU** (`--no-mmproj-offload`, frees 0.86 GiB) | 34 KiB/token: 48k ctx, about 1.6 GiB | 100% GPU, all 65 layers (no `-ngl`; `--fit` decides). Measured VRAM 15.2 GB at 48k | `-ub 256` leaves room for the GPU reranker. Spills dense layers if Unity or Blender take VRAM |
| `qwen3.6-35b-a3b` (MoE, UD-Q4_K_XL) | 22.36 GB (20.8 GiB) | 0.90 GB F16 | 10.6 KiB/token: 128k, about 1.33 GiB | Measured: experts of blk 20-39 (20 layers) in pinned host RAM (GTT 11.7 GB) | Picks the split at load time. Don't pin `-ngl` or `--n-cpu-moe` |
| `gemma4-26b` (MoE, QAT UD-Q4_K_XL) | 14.25 GB (13.27 GiB) | 1.19 GB BF16 | about 10.6 KiB/token: 128k, about 1.33 GiB (measured: K and V stored separately) | Measured: experts of blk 24-29 (6 of 30 layers) in host RAM | Picks the split at load time. `--swa-full` stays off |
| `granite-vision` | 2.69 GB Q8_0 | 0.89 GB | `-c 32768`, `-np 4 --kv-unified` (one image is about 5.2k tokens) | GPU | Chat group member |
| `granite-docling` | 0.33 GB BF16 | 0.19 GB | `-c 32768`, `-np 4 --kv-unified --special` | GPU | Chat group member |
| Aux: bge-m3, snowflake | 1.16 GB each (F16) | none | `-c 8192` | **CPU** | n/a |
| Aux: bge-reranker-v2-m3 | 0.64 GB Q8_0 | none | `-c 8192` | **GPU**, about 370 MiB VRAM (0.2 s vs about 4 s on the CPU) | n/a |

Qwen3.8 KV note: 32k is about 1.06 GiB and 64k about 2.1 GiB. The 48k entry gets vision because the encoder runs on the CPU.

## Ports

| Port | Service | Notes |
|---|---|---|
| `11434` | Ollama today; llama-swap (container `8080`) after cutover | At cutover llama-swap takes this host port and the network alias `ollama` |
| `11435` | llama-swap (container `8080`), **current** | Phase 2 side by side, without the `ollama` alias. Goes away at cutover |
| `8000` | docling-proxy (container `8080`) | Unchanged. Also uses the GPU |
| `${PORT}` | colibri | Internal, assigned by llama-swap. Bound to `127.0.0.1`, not published |

## Volumes and paths

| Path | Purpose |
|---|---|
| `~/llm/models` (mounted `/models:ro`) | GGUF files managed by `llm-pull` (about 60 GB) |
| `~/llm/llama-cache` (mounted `/cache`, `LLAMA_CACHE=/cache`, env name confirmed) | Tier 2 `-hf` experiments, read-write |
| `~/llm/colibri` | colibri binary and big-model dir |
| `./llama-swap/` (the **directory**, mounted `/config:ro`; entrypoint `-config /config/config.yaml -listen :11434`, no hot reload) | llama-swap config. A directory, because a single-file mount goes stale when an editor replaces the inode |
| `./llama-swap/models.lock.yaml` | Model manifest: pinned revision (commit SHA), files, aliases. Committed to git |
| `bin/llm-pull` | Downloader. Spec: [model-downloads.md](model-downloads.md) |
| `bin/gpu-mode` | GPU switching. Spec: [gpu-switching.md](gpu-switching.md) |

## Design decisions

**Why llama.cpp + llama-swap instead of Ollama**
- **MoE expert offload.** `--n-cpu-moe` puts only experts in RAM, which beats Ollama's layer split.
- **Exact fit.** `--fit` picks the split from the VRAM free at load time. That matters because Unity and Blender also use the GPU.
- **Newer models.** The new GGUFs, including the QAT-based Gemma quant, are available.
- **A working reranker.** Ollama has no rerank API, so the current `bge-reranker-v2-m3` cannot have been reranking. llama-server `--reranking` gives `/v1/rerank`.
- **One endpoint.** llama-swap serves every model, including colibri, on one OpenAI-compatible API, with groups replacing `MAX_LOADED_MODELS`.

**Why Vulkan instead of ROCm here**
- Ollama already runs here with `OLLAMA_VULKAN=1`, and flash attention plus q8_0 KV are supported on Vulkan. The RX 7800 XT is on the llama.cpp FA scoreboard.
- Image: `ghcr.io/mostlygeek/llama-swap:v264-vulkan-b11580` (llama.cpp build 11580; v263-vulkan-b11541 until 2026-10-13). Resolved: it bundles `llama-server` at this pinned tag.
- ROCm image `v263-rocm-b11541` benchmarked 2026-10-13: Vulkan stays (ROCm −17%/−20% tg on gemma4/qwen3.8, +8% on qwen3.6, slower loads, wrong free-VRAM report for `--fit`). See [bench/verification-2026-10-13.md](../bench/verification-2026-10-13.md).
- MTP (`--spec-type draft-mtp`) stays off. Tested 2026-10-13: Gemma 4's draft works on Vulkan and is faster, but `--fit` doesn't budget its VRAM.

**Why the embedders run on the CPU and the reranker on the GPU**
- Every VRAM GiB matters: qwen3.8 only fits with Q3 weights and 48k context. The embedders are small (568M parameters) and stay loaded (`persistent`), so the CPU is enough.
- The reranker is latency-sensitive: 10 chunks of about 350 tokens take 0.2-0.65 s on the GPU against 3.7-4.9 s on the CPU. It costs about 370 MiB of VRAM, which is why qwen3.8 uses `-ub 256` to keep all 65 layers on the GPU.

**Why the Ollama blobs are reused for embedders**
- The `bge-m3` (`sha256-daec91ff...`) and `snowflake-arctic-embed2` (`sha256-8c625c95...`) blobs are GGUF files with F16 weights, so the vectors stay identical and Qdrant needs no re-index. Any re-quant would force one.
- Copy them with `llm-pull import-ollama`, don't move them. The snowflake blob needs a charsmap fixup (applied automatically). Parity PASS: min cosine 0.999969 (bge-m3) and 0.999952 (snowflake) on 5 fixed texts, so no re-index.

## Known risks

| Risk | Mitigation |
|---|---|
| Qwen3.8 is dense and must fit 100% in VRAM. Unity or Blender (1-3 GiB) makes `--fit` spill layers | Use qwen3.6 or Claude Code while editors are open ([gpu-switching.md](gpu-switching.md)) |
| MoE experts compete for RAM (about 12-13 GiB available) | `-cram 2048`, `-ctxcp 8`, `-lm dio`. NVMe swap stayed at 0 B in the full-context tests. Per checkpoint: qwen3.6 62.8 MiB, qwen3.8 149.6 MiB, gemma4 106.3 MiB |
| The migration could be *slower* than Ollama (no MTP) | Measured faster: qwen3.8 32.4 vs 6.6 tok/s, gemma4 43-47 vs 33.0 tok/s; qwen3.5 baseline not measured (Ollama hung) |
| Embedder blobs may not load or may differ | Resolved: parity PASS (see above) |
| granite-docling and granite-vision multimodal support in llama.cpp | Resolved: both work (docling needs `--special`). Fallback if it regresses: docling-serve local engine |
| `/v1/rerank` response shape vs Open WebUI | Resolved: `results[].index` + `relevance_score` match. Scores are raw logits, keep Open WebUI's relevance threshold at 0 ([clients.md](clients.md)) |
| Open WebUI task model requests another model and swaps on every message | Set Task model = "current model" |
| `GGML_VK_VISIBLE_DEVICES` index for the 7800 XT | Resolved: `0` (Vulkan0 = RX 7800 XT, Vulkan1 = 610M) |
| Mounting `docker.sock` into llama-swap (colibri option B) is root-equivalent | Prefer option A ([colibri.md](colibri.md)) |
| docling-proxy also uses the GPU | Avoid large docling jobs while a 14 GB chat model is loaded |
| llama-swap v263 config keys | Resolved: `macros`, `hooks.on_startup.preload` work; groups live under `routing.router.settings.groups` |
| `POST /api/models/unload` also unloads the persistent aux models | Re-warm with one request each (`bin/gpu-mode comfy` does this) |
