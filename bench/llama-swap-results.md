# Phase 0–3 results (2026-10-11)

Image `ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541` (llama.cpp build 11541, Vulkan, RADV NAVI32).
Sweep write-up: `qwen3.6-sweep.md`.

## Generation speed, Phase 0 prompt (512 tokens)

| Model | Ollama (Phase 0) | llama-swap | Notes |
|---|---|---|---|
| qwen3.8 27B dense | 6.6 tok/s (17 GB Q4_K_M, 22% on CPU, 4k ctx) | **32.4 tok/s** (UD-Q3_K_XL, 100% GPU, 48k ctx) | ~5x |
| gemma4 26B-A4B | 33.0 tok/s (Q4_0, 4k ctx) | **43–47 tok/s** (QAT UD-Q4_K_XL, 128k ctx) | ~1.4x |
| qwen3.5 35B-A3B → qwen3.6 35B-A3B | not measured: Ollama hung on qwen3.5 ("Stopping...") | **31.5 tok/s** (UD-Q4_K_XL, 128k ctx) | |

## Full-context fit check (`fullctx.sh`), no OOM, NVMe swap 0 B

| Model | Prompt | Prompt speed | Generation at that depth | Memory |
|---|---|---|---|---|
| qwen3.8-27b | 48,368 tok | 367 tok/s (ub 256) / 398 (ub 512) | 26.4 tok/s | VRAM 15.2 GB, all 65 layers on GPU |
| qwen3.6-35b-a3b | 126,026 tok | 734 tok/s | 24.6 tok/s | experts of 20 layers in pinned host RAM (GTT 11.7 GB), 11.4 GB RAM available |
| gemma4-26b | 120,033 tok | 801 tok/s | 29.7 tok/s | experts of 6 of 30 layers in host RAM, 17.6 GB RAM available |

`--fit` splits (from `llama fit-params`): qwen3.6 → experts of blk 20–39 on CPU; gemma4 → experts of blk 24–29 on CPU.

## Functional checks

| Check | Result |
|---|---|
| Embedding parity vs Ollama (5 texts) | bge-m3 min cos **0.999969**, snowflake **0.999952** → PASS, no Qdrant re-index |
| snowflake-arctic-embed2 Ollama blob | llama.cpp rejected it (charsmap stored as base64 char array). Fixed by `tools/fix-gguf-charsmap.py` (metadata only, tensors identical), applied automatically by `llm-pull import-ollama` |
| `/v1/rerank` shape | `results[].index` + `relevance_score` → matches Open WebUI external reranker. Scores are raw logits (can be negative); Open WebUI only filters when *Relevance Threshold* ≠ 0, so keep it 0 |
| Rerank latency, 10 chunks × ~350 tok | CPU `-t 4` 4.9 s, `-t 8` 3.7 s, 610M iGPU 5.8 s, **dGPU 0.2–0.65 s** (~370 MiB) → reranker on GPU |
| Vision (table + shape) | qwen3.8, qwen3.6, gemma4 all correct |
| Tool call | qwen3.8, qwen3.6, gemma4 all emit `read_sensor({"deck":3,"sensor":"coolant_pressure"})` |
| granite-vision | describes the page (needs ~5.2k tokens/image → unified 32k KV) |
| granite-docling | full DocTags incl. `<otsl>` table, needs `--special` |
| CORS (Page Assist, `moz-extension://` origin) | `Access-Control-Allow-Origin: *` on preflight and responses |

## Tuning decisions (pinned in `llama-swap/config.yaml`)

| Setting | Value | Evidence |
|---|---|---|
| `-t` (MoE) | 8 | 6/8/12 threads within noise (`qwen3.6-sweep.md`) |
| `-ub` (MoE) | 2048 | prompt 1040 vs 750 (1024) vs 470 (512) tok/s, generation unchanged |
| `-ub` (qwen3.8) | 256 | keeps all 65 layers on GPU with the reranker in VRAM; −8% prompt speed |
| `--kv-unified` | on for chat | with explicit `-np 2` llama.cpp otherwise splits 48k into 2×24k slots |
| `-lm dio` | on for chat | with mmap, after a gemma→qwen3.6 swap the CPU-side experts were evicted from page cache and re-read from NVMe during inference: 3.8–14 tok/s prompt, 16–19 tok/s generation. With direct I/O: 31.5 tok/s consistently |
| `--no-mmproj-offload` | qwen3.8 only | no measurable effect on MoE models once the mmap issue was fixed |

## Findings that changed the plan

- llama-swap v263 moved groups to `routing.router.settings.groups`.
- `--no-mmap` no longer exists in llama.cpp b11541: it is `-lm/--load-mode {auto,none,mmap,mlock,mmap+mlock,dio}`.
- A llama-swap ROCm image exists (`v263-rocm-b11541`); not benchmarked yet.
- `POST /api/models/unload` also unloads the persistent aux group, and preload does not re-run. Re-warm with one request per aux model (see `bin/gpu-mode`).
- A single-file bind mount of `config.yaml` goes stale when an editor replaces the inode; the compose file mounts the `llama-swap/` directory instead.
- The image has no `video`/`render` group names; compose uses the host GIDs (983, 987).

## Optional items (resolved 2026-10-13, [verification-2026-10-13.md](verification-2026-10-13.md))

- MTP (Gemma 4 draft) works on Vulkan: +34% tg at 5.6k depth, +40–75% on short answers, 60–83% acceptance; coherent but not bit-identical at temperature 0; `--fit` doesn't budget the draft. Not applied.
- ROCm (`v263-rocm-b11541`) vs Vulkan: Vulkan stays (ROCm −17% gemma4 tg, −20% qwen3.8 tg, +8% qwen3.6 tg, slower loads, misreports free VRAM).
- `-ctxcp` per checkpoint: qwen3.6 62.8 MiB, qwen3.8 149.6 MiB, gemma4 106.3 MiB (worst case 8 × 2 slots: 1.0 / 2.3 / 1.7 GiB).
- qwen3.8 is now 63 of 65 layers on the GPU (reranker in VRAM): 21 tok/s at 5.5k depth; `-c 40960` puts all 65 on the GPU, 26.7 tok/s.

## Phase 4 cutover (2026-10-11)

| Client | Result |
|---|---|
| Open WebUI (jupiter, k3s) chat | OpenAI connection `http://mars.internal:11434/v1`; chats on qwen3.8-27b and gemma4-26b, swaps work. Base URL without `/v1` lists models (`/models` works) but chat/embeddings 404 |
| Open WebUI RAG | query embeddings 30–45 ms; rerank 200 but 14–17 s per query: 3 concurrent requests × ~60 chunks × ~1,400 tokens ≈ 280k tokens. Tune Top K / Top K Reranker / chunk size (deferred) |
| Open WebUI document upload | ~150 chunks embedded; 71 s with `-t 4`, ~25 s expected with `-t 8` (48 chunks in 7.8 s) |
| docling-proxy, standard pipeline + `picture_description_api` → granite-vision | success, 27 s, table + picture description |
| docling-proxy, VLM pipeline → granite-docling (`response_format: doctags`) | success, 4 s; trailing `<\|end_of_text\|>` is handled (re-checked 2026-10-13 with a synthetic PDF: 0 occurrences in md/doctags/json) |
| opencode (`ollama/qwen3.6-35b-a3b`) | works; config updated to new ids and real context limits |
| Page Assist | skipped (not used); CORS verified above |

Incidents: config hot reload (`-watch-config`) restarted models under load: a port-bind race left snowflake down, and a second reload killed it mid-request (Open WebUI embedding 502s). Hot reload removed; apply edits with `docker compose restart llama-swap`.
