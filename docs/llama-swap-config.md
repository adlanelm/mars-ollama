# llama-swap config and compose service

Status: **implemented through Phase 3** (2026-10-11), running side by side with Ollama on `:11435`. Cutover to `:11434` is Phase 4. Measurements: [bench/llama-swap-results.md](../bench/llama-swap-results.md). Items marked **[verify]** are still unconfirmed.

Related: [clients.md](clients.md) for the client side, [ROADMAP.md](../ROADMAP.md) for the phases.

## 1. `llama-swap/config.yaml`

The live file is [`llama-swap/config.yaml`](../llama-swap/config.yaml). It is the source of truth; this doc only explains it. Schema reference: llama-swap v263 `docs/config.example.yaml`. Resolved: the v263 keys (`macros`, `ttl`, `aliases`, `hooks.on_startup.preload`) work as used, and **groups moved to `routing.router.settings.groups`** (the old top-level `groups:` no longer applies).

Notes:
- Model paths follow `/models/<org>/<repo>/<file>` (the `llm-pull` layout). The embedders are copies of the Ollama blobs under `/models/ollama/<model>/<model>-F16.gguf` (see [model-downloads.md](model-downloads.md)).
- `--fit` (default on, 1 GiB margin) decides `-ngl` and the MoE expert split, so none of the big chat models has `-ngl` or `--n-cpu-moe`.
- Sampling flags are server defaults. Clients can override them per request.
- MTP (`--spec-type draft-mtp`) is **on for `gemma4-26b`** since 2026-10-13 (draft `mtp-gemma-4-26B-A4B-it.gguf` pinned in `models.lock.yaml`, role `draft`; `--spec-draft-n-max 4 --fit-target 2048`). First check after enabling: 78-83 tok/s on short answers, 75-78% acceptance, 14.7 GiB VRAM in use. Off for the other models. `--fit` ignores the draft's VRAM, hence the 2 GiB margin ([bench/verification-2026-10-13.md](../bench/verification-2026-10-13.md)).
- `sendLoadingState: false`, because loading text would end up in `reasoning_content`.

Annotated excerpt (abridged; see the real file for everything):

```yaml
macros:
  chat_common: >-          # all chat models
    -fa on -ctk q8_0 -ctv q8_0 -np 2 --kv-unified -ctxcp 8 -cram 2048 -lm dio --jinja --no-webui
  moe_common: >-           # qwen3.6, gemma4
    -t 8 -tb 8 -ub 2048 -b 2048 --moe-cache-mib 3072
  aux_common: >-           # CPU embedders
    -dev none -ngl 0 -c 8192 -b 8192 -ub 8192 -t 4 -cram 0 --no-webui

models:
  "qwen3.8-27b":           # no -ngl: --fit puts all 65 layers on the GPU
    cmd: |
      llama-server --port ${PORT} -m ... --mmproj ...
      ${chat_common} -c 49152 --no-mmproj-offload -ub 256 ...

routing:
  router:
    use: group
    settings:
      groups:              # v263: under routing.router.settings
        chat: { swap: true, exclusive: true, members: [qwen3.8-27b, ...] }
        aux:  { persistent: true, swap: false, exclusive: false, members: [snowflake-arctic-embed2, bge-reranker-v2-m3] }

hooks:
  on_startup:
    preload: [snowflake-arctic-embed2, bge-reranker-v2-m3]
```

### Flags per entry

| Entry | Flags beyond the macros | Why |
|---|---|---|
| `qwen3.8-27b` | `-c 49152 --no-mmproj-offload -ub 256` | No `-ngl 99`: `--fit` decides. Since the reranker moved to the GPU it places 63 of 65 layers (2026-10-13); `-c 40960` would fit all 65 (+25% tg). `-ub 256` leaves room for the GPU reranker (about 370 MiB), at -8% prompt speed (367 vs 398 tok/s at ub 512) |
| `qwen3.8-27b-gsq` | `-c 65536 --no-mmproj-offload -ub 256` | GSQ IQ3_S: all layers on the GPU at 64k with the reranker loaded (stock flips between 32 and 22 tok/s by free VRAM at load). pi's qwen3.8 |
| `flint-40b-a2.5b` | `-t 8 -tb 8 -ub 2048 -b 2048 --moe-cache-mib 1024 -c 131072` | Not `${moe_common}`: its 3072 MiB expert cache (and 2048) end in `ErrorDeviceLost` at load; 1024 is +11% tg over none. Template ignores `reasoning_effort`/`enable_thinking`; cap thinking with `thinking_budget_tokens` (0 = off) |
| `qwen3.6-35b-a3b` | `${moe_common} -c 131072` | `-t 8 -tb 8 -ub 2048 -b 2048` (measured, see below). `--fit` puts the experts of blk 20-39 on the CPU |
| `gemma4-26b` | `${moe_common} -c 131072` | `--fit` puts the experts of blk 24-29 (6 of 30 layers) on the CPU. `--swa-full` stays off |
| `granite-vision` | `-np 4 --kv-unified -c 32768 -ngl 99 --temp 0` | One image is about 5.2k tokens (tiling), so the 4 slots share one unified 32k KV pool |
| `granite-vision-4.1` | `-np 4 --kv-unified -c 32768 -ngl 99 --temp 0` | Same layout as `granite-vision`. docling-proxy's default picture-description model since 2026-10-16 ([../bench/docling-2026-10-16.md](../bench/docling-2026-10-16.md)) |
| `granite-docling` | `-np 4 --kv-unified --special -c 32768 -ngl 99 --temp 0` | `--special`: the DocTags tags (`<text>`, `<otsl>`, ...) are special tokens; without `--special` the output lacks them |
| `snowflake-arctic-embed2` | `${aux_common} --embeddings` | CPU only (`-dev none -ngl 0`) |
| `bge-reranker-v2-m3` | `-ngl 99 -fa on -c 8192 -b 8192 -ub 2048 -t 4 -cram 0 --reranking` | **GPU**, about 370 MiB |

### Flag explanations

- **`--kv-unified`**: with an explicit `-np 2`, llama.cpp otherwise gives every slot `c/np` of the context (48k becomes 2x24k, 128k becomes 2x64k). With a unified KV buffer the slots share one pool of the full size.
- **`-lm dio`** (`--load-mode dio`, direct I/O): replaces `--no-mmap`, which **no longer exists** in llama.cpp b11541 (`-lm/--load-mode {auto,none,mmap,mlock,mmap+mlock,dio}`). With mmap, CPU-side weights (MoE experts) live in the page cache. After a model swap the kernel evicted qwen3.6's experts, and they were re-read from NVMe during inference: 3.8-14 tok/s prompt and 16-19 tok/s generation. With direct I/O, bypassing the page cache: 31.5 tok/s consistently.
- **`-ub 2048`** on the MoE models: prompt 1040 tok/s, against 750 at 1024 and 470 at 512. Generation is unchanged. `-t` 6, 8 and 12 were within noise, so 8 (physical cores) stays.
- **`--moe-cache-mib 3072`** on the MoE models: a VRAM cache for the experts `--fit` keeps on the CPU. Generation on a 2-4k-token answer: gemma4 61 → 109 tok/s, qwen3.6 36 → 49, ornith 37.5 → 44. Prompt speed drops 0-4%. `--fit` counts the cache: it moves 6-7 more expert layers to the CPU and keeps the same free margin, so `--fit-target` stays unchanged. 3584 was no faster; qwen3.6 was unstable at 3968, slowed to 12 tok/s at 4096 and failed with `ErrorDeviceLost` at 6144 ([sweep](../bench/moe-cache-2026-10-22.md)).
- **Reranker on the GPU**: 10 chunks of about 350 tokens take 0.2-0.65 s on the dGPU, against 3.7-4.9 s on the CPU (`-t 8` / `-t 4`) and 5.8 s on the 610M iGPU. Over the 3 s limit on the CPU, so it moved to the GPU. This is also why qwen3.8 uses `-ub 256`.

## 2. Compose service

The live file is [`docker-compose.yaml`](../docker-compose.yaml), service `llama-swap`. Infrastructure changes need a merge request with peer review before applying.

| Setting | Value | Why |
|---|---|---|
| `image` | `ghcr.io/mostlygeek/llama-swap:v265-vulkan-b11804` | Pinned (v264-vulkan-b11580 until 2026-10-22, v263-vulkan-b11541 until 2026-10-13). Bundles `llama-server` (llama.cpp build 11804, Vulkan, RADV NAVI32). Resolved: llama-server is bundled at this tag. Vulkan beat the ROCm image (`v263-rocm-b11541`) on 2 of 3 models, and ROCm misreports free VRAM to `--fit` ([verification](../bench/verification-2026-10-13.md)) |
| `entrypoint` | `["/app/llama-swap", "-config", "/config/config.yaml", "-listen", ":11434"]` (no `-watch-config`, see below) | Overrides the image default so the config lives under `/config` and is reloaded on change |
| volume `./llama-swap:/config:ro,Z` | the **directory**, not the file | A single-file bind mount points at an inode. Editors replace the file with a new inode, so the container keeps seeing the old, stale content. A directory mount follows the file name |
| `GGML_VK_VISIBLE_DEVICES=0` | | Resolved: `Vulkan0` is the RX 7800 XT, `Vulkan1` the 610M iGPU. Never select 1 |
| `LLAMA_CACHE=/cache` | | Confirmed env name; only used by `-hf` experiments |
| `group_add: ["983", "987"]` | numeric host GIDs (video, render) | The image has no `video` or `render` group names, so names fail |
| `ports` | `11435:8080` now | Phase 2, side by side with Ollama. Becomes `11434:8080` at cutover |
| `mem_limit: 28g`, `cpus: "12.8"` | | Same as the Ollama service |
| volumes `/models:ro`, `/cache` | | Models read-only, `-hf` cache read-write |

At cutover (Phase 4, not applied yet):
- Change the port to `11434:8080`.
- Add the network alias `ollama` on the `ollama` network, so docling-proxy and other in-network clients still resolve `ollama`. Until then the service has no alias.
- Move the `ollama` service under `profiles: ["legacy"]`.

Rollback: `docker compose stop llama-swap && docker compose --profile legacy up -d ollama`. Stop llama-swap first, because both claim host port 11434 and the `ollama` alias. *Removed 2026-10-13 (Phase 5 step 2): the `legacy` service, its data and image are gone, so there is no Ollama rollback any more.*

The Qwen3.8 upgrade from the plan applies only if the monitor is moved to the iGPU (+1.13 GiB VRAM). Then swap the file for UD-IQ4_XS (14.25 GB) and keep the same flags and `-c 49152`. Not tested; the reranker's 370 MiB on the GPU also counts against it **[verify]**.

## 3. Notes

### `--fit` behaviour
- `--fit` is on by default. It picks `-ngl` and the MoE expert split from the VRAM free **at load time** and keeps a 1 GiB margin per device (`--fit-target 1024`). Record the split: `docker logs llama-swap | grep -i -E 'fit|offload|n_cpu_moe'` (`logToStdout: "proxy,upstream"` makes llama-server logs appear there).
- It adapts to the desktop (about 1.13 GiB idle) and to Unity or Blender using 1-3 GiB.
- `qwen3.8-27b` is dense and must be 100% on the GPU. `--fit` fits all 65 layers at 48k when Unity and Blender are closed (VRAM 15.2 GB in the full-context test). With the editors open it would spill layers to RAM, which is slow. Use Qwen3.6 then.
- Tuning is pinned (Phase 3 done): `-ub` and `-t` above.

### Swapping and groups
- `chat` (`swap: true`, `exclusive: true`): one chat-group model at a time. A swap takes about 10-20 s.
- `aux` (`persistent: true`, `swap: false`, `exclusive: false`): always loaded. Embedders on CPU, reranker on GPU. Loaded at startup by `hooks.on_startup.preload`, and not unloaded by chat swaps.
- `ttl`: 900 s for chat, 120 s for granite, 0 (never) for aux.
- Granite is in the `chat` group, so a docling job swaps the chat model out.
- Open WebUI's task model must be "current model", otherwise title and tag generation swap the chat model on every message ([clients.md](clients.md)).
- `colibri-big` joins `chat` in Phase 7, so requesting it unloads the chat model automatically.
- `healthCheckTimeout: 1230` covers the gpu-guard wait for ComfyUI (max 600 s, [gpu-switching.md](gpu-switching.md#automatic-arbitration-llama-swap---comfyui)) plus a cold load of a 20 GB model (600 s). GPU models start as `${gpu_guard} llama-server ...`.

### Adding a model
1. Download it: `llm-pull add <repo> <quant>` (see [model-downloads.md](model-downloads.md)). It prints a ready snippet and pins the revision in `models.lock.yaml`.
2. Add an entry under `models:` with `cmd: llama-server --port ${PORT} -m /models/<org>/<repo>/<file> ...`, using the macros, `aliases` and `ttl`.
3. Add it to the `chat` group `members`, or to `aux` plus the preload list (small model).
4. Run `docker compose restart llama-swap`, then check `curl -s localhost:11435/v1/models | jq '.data[].id'` (`:11434` after cutover).
5. For a quick experiment, use `-hf unsloth/<Model>-GGUF:<QUANT>` in `cmd` (downloads into `/cache`). After the first load add `--offline`.

### API endpoints (port 11435 now, 11434 after cutover)
| Endpoint | Use |
|---|---|
| `/v1/*` | OpenAI-compatible: `/v1/models`, `/v1/chat/completions`, `/v1/embeddings` |
| `/v1/rerank` | Reranking. Returns `results[].index` + `relevance_score` (raw logits, can be negative) |
| `/v1/messages` | Anthropic-compatible messages API |
| `/running` | Which models are loaded right now |
| `/api/models/unload` | `POST`: unloads **all** models, **including the persistent aux models**. Preload does **not** re-run, so re-warm each aux model with one request (`bin/gpu-mode comfy` does this) |
| `/api/*` (Ollama) | **Not available.** All clients must move to `/v1` |

## Sources
- https://github.com/mostlygeek/llama-swap
- [bench/llama-swap-results.md](../bench/llama-swap-results.md)

### Applying config changes: restart, no hot reload
`-watch-config` was removed from the entrypoint on 2026-10-11. A reload restarts the running models, and under live traffic that caused two failures:
- the restarted process tried to bind its port while the old one still held it (`couldn't bind HTTP server socket`), so the model stayed down;
- a second reload killed snowflake mid-request, and Open WebUI's queued embedding requests got 502s.

Apply edits to `llama-swap/config.yaml` with `docker compose restart llama-swap`, when no client is busy (`curl -s localhost:11434/running`).
