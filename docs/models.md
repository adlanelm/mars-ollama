# Models: audit of the current Ollama set and the target picks

Status: **implemented and measured through Phase 3** (2026-10-11); the live flags are in `llama-swap/config.yaml`. Research from HF model cards, llama.cpp docs and the Ollama library. Measurements: [bench/llama-swap-results.md](../bench/llama-swap-results.md). Items marked **[verify]** are still unconfirmed.

Related docs:
- [model-downloads.md](model-downloads.md): how the files below are downloaded and pinned (`llm-pull`).
- [llama-swap-config.md](llama-swap-config.md): how the entries and flags below land in `config.yaml`.

---

## 1. Audit of current Ollama models (fact-checked)

| Ollama model | Arch, MoE? | Current quant / params | Verdict |
|---|---|---|---|
| `qwen3.8` (:latest/:64k/:128k) | `qwen35`, **dense** 27.3B (hybrid DeltaNet + 16 attention layers), vision, MTP heads | Q4_K_M, **17 GB**. Sampling temp 1 / top_p 0.95 / top_k 20 / presence 0 (matches the official thinking-mode values) | **Not optimal.** 17 GB doesn't fit in 14.8 GiB, and because the model is dense, every token runs partly on the CPU. 64k and 128k make it worse. See section 2 |
| `gemma4:26b` (:64k/:128k) | `gemma4`, **MoE** 25.2B total / 3.8B active (128 experts, 8+1 active), vision | **Q4_0, not the QAT version** (hash differs from `-qat`), 15 GB. Sampling 1.0 / 0.95 / 64 (matches) | **Suboptimal quant.** Use Google QAT-based **unsloth `gemma-4-26B-A4B-it-qat` UD-Q4_K_XL (14.2 GB)** + mmproj. unsloth reports 85.6% vs 70.2% top-1 agreement for naive Q4_0. The three context tags become **one 128k entry** |
| `qwen3.5` (:latest/:64k/:128k) | `qwen35moe`, **MoE** 35B / 3B active | Q4_K_M 23 GB, presence 1.5 (matches thinking-general) | **Remove**, replace with **Qwen3.6-35B-A3B**: same layout, better scores (SWE-V 73.4 vs 70.0, TB2 51.5 vs 40.5). No qwen3.5 aliases are kept. In llama.cpp, `--n-cpu-moe` puts **only experts** in RAM, which beats Ollama's layer split. This becomes the long-context (128k) model |
| `glm-4.7-flash` (:latest/:198k) | `glm4moelite`, **MoE** 30B-A3B | Q4_K_M 19 GB | Drop (owner's choice) |
| `bge-m3` | bert / XLM-R, 568M | F16 | **Keep.** Reuse the exact Ollama blob (it's a GGUF) so Qdrant vectors stay valid |
| `snowflake-arctic-embed2` (+`:latest_8192`) | bert, 568M | F16, `latest_8192` only sets num_ctx | **Keep**, reuse the blob. Resolved: llama.cpp rejects the Ollama blob as is (charsmap stored as a base64 char array); `llm-pull import-ollama` fixes it automatically ([model-downloads.md](model-downloads.md)). Cosine parity PASS |
| `linux6200/bge-reranker-v2-m3` | bert, 568M, **shown as "completion"** | F16 | **Ollama has no rerank API, so this can't have been reranking.** Check how Open WebUI's reranker is set up today. Replace with `gpustack/bge-reranker-v2-m3-GGUF` and llama-server `--reranking` (`/v1/rerank`). The pre-migration Open WebUI setting is not recoverable (Ollama data removed, no screenshot); current setup: [bench/verification-2026-10-13.md](../bench/verification-2026-10-13.md) |
| `ibm/granite-docling:258m` | llama + clip, 164M+94M | BF16 | Use the official `ibm-granite/granite-docling-258M-GGUF`. Resolved: multimodal works (needs `--special`) |
| `ibm/granite3.3-vision:2b` | granite + clip | Q8_0, ctx 16k, temp 0 | Use the official `ibm-granite/granite-vision-3.3-2b-GGUF` Q8_0. Resolved: multimodal works |

### Global settings audit

| Ollama setting | Finding |
|---|---|
| `num_thread 13` | Pointless for models fully on GPU. For MoE offload it is likely worse than `-t 8` (physical cores). Benchmark it |
| `MAX_LOADED_MODELS=2` | Two 15 GB models can't share 14.8 GiB. Replaced by llama-swap groups: big models swap exclusively, small models stay loaded on the CPU |
| `draft_num_predict` (MTP) | llama.cpp has MTP (`--spec-type draft-mtp`), but there are **Vulkan bug reports (garbage output)** and it costs another 1-1.4 GB of VRAM. Tested 2026-10-13 on Vulkan: works for Gemma 4 (+34% tg at 5.6k depth, +40–75% short answers), not bit-identical at temperature 0, and `--fit` doesn't budget the draft. Still **off**; proposal in [bench/verification-2026-10-13.md](../bench/verification-2026-10-13.md). ROCm (`v263-rocm-b11541`) was benchmarked and loses on 2 of 3 models, so Vulkan stays |
| Flash attention, q8_0 KV | **Kept.** Both are supported on Vulkan; the RX 7800 XT is on the llama.cpp FA scoreboard |

---

## 2. Target models: quants and parameters

### Inputs (HF file listings and config.json, fetched 2026-10-11)

- **VRAM:** 15.98 GiB total. The desktop uses about 1.13 GiB when idle, leaving **about 14.85 GiB usable**. llama.cpp's `--fit` (on by default) keeps a 1 GiB margin per device.
- **RAM:** 30 GiB. The host uses about 10 GiB with Ollama idle (desktop, docling-proxy, browser), and the container `mem_limit` is 28g. The CPU-resident embedders take part of that (measured RSS: bge-m3 1.3 GiB, snowflake 1.3 GiB, reranker 0.5 GiB ≈ 3.1 GiB). So **at most about 12-13 GiB is available for MoE experts in RAM** before zram and NVMe swap come into play.

**KV cost per token at q8_0** (1.0625 B per element):

| Model | Attention layout | KV per token | Examples |
|---|---|---|---|
| Qwen3.8-27B | 16 attention layers x 4 KV heads x 256 dim | **34 KiB** | 32k = 1.06 GiB, 64k = 2.1 GiB |
| Qwen3.6-35B-A3B | 10 of 40 layers are attention, x 2 KV heads x 256 | **10.6 KiB** | 128k = 1.33 GiB |
| Gemma 4 26B | 25 sliding-window layers (window 1024, fixed ~0.15 GiB) + 5 global layers x 2 KV heads x 512 | **about 10.6 KiB** (measured: K and V are stored separately) | 128k = 1.33 GiB for the global layers; `fit-params` context total 1.64 GiB at 128k |

The per-token figure is layers x KV heads x head dim x 2 (K and V) x 1.0625 B, for example Qwen3.8: 16 x 4 x 256 x 2 x 1.0625 = 34,816 B = 34 KiB.

**Long context is cheap on all three**, because they are hybrid or sliding-window models. Weights are what cost VRAM.

### Final picks

| ID (aliases) | File (exact size) | Why this quant | Placement / key flags |
|---|---|---|---|
| **`qwen3.8-27b`** (`qwen3.8:latest`, `qwen3.8`, `qwen3.8:64k`, `qwen3.8:128k`; all old context tags map to this 48k entry) | `unsloth/Qwen3.8-27B-GGUF` **UD-Q3_K_XL** (13.15 GB = 12.24 GiB) + `mmproj-F16` (0.93 GB) | Dense, so it must be 100% on GPU. UD-IQ4_XS (13.27 GiB) is better quality, but with compute buffers it leaves only about 0.9 GiB, which is about 24k context with no vision on the GPU. Q4_K_S and above don't fit. UD-Q3_K_XL is the largest that leaves useful context | `-c 49152 -ctk q8_0 -ctv q8_0 --no-mmproj-offload -ub 256` (no `-ngl`: `--fit` decides; measured 2026-10-13 with the reranker and desktop in VRAM: **63 of 65 layers**, 21 tok/s at 5.5k depth; `-c 40960` fits all 65, 26.7 tok/s). **Running the vision encoder on the CPU frees 0.86 GiB**, which turns 32k into **48k with vision in a single entry**. Image encoding takes a few seconds on 8 cores |
| `muse-glimmer-30b` (trial 2026-10-22) | `unsloth/Muse-Glimmer-30B-GGUF` **UD-Q3_K_XL** (13.36 GB) + `mmproj-Muse-Glimmer-30B-Q8_0` (2.05 GB) | Dense ~28B, same tier as qwen3.8. Only 13 of 52 layers are full attention (2 KV heads), the rest a 2k sliding window, so the KV cache is small: 128k fits with 53/53 layers on the GPU and about 2 GiB left. UD-Q4_K_XL (15.88 GB) does not fit | `-c 131072 --no-mmproj-offload -ub 256`, temp 1 / top_p 0.95 / top_k 64. Measured 33.7 tok/s (bench/ab-2026-10-22.md). The template defaults to `Reasoning strength: high` |
| **`qwen3.6-35b-a3b`** (`qwen3.6`) | `unsloth/Qwen3.6-35B-A3B-GGUF` **UD-Q4_K_XL** (22.36 GB = 20.8 GiB) + `mmproj-F16` (0.90 GB) | MoE: about 92% of weights are experts (about 0.47 GiB per layer). Measured: `--fit` puts the experts of blk 20-39 (20 of 40 layers) on the CPU. UD-Q5_K_XL (24.8 GiB) would need about 13 GiB of RAM, past the RAM budget. MXFP4_MOE has no hardware benefit on RDNA3 | `-c 131072 -ctk q8_0 -ctv q8_0 -ub 2048 -b 2048 -t 8 -tb 8`. **Don't pin `-ngl` or `--n-cpu-moe`:** `--fit` picks the expert split from the VRAM free at load time. That matters because Unity and Blender also use the GPU. Log the chosen split |
| `ornith-1.5-35b-a3b` (trial 2026-10-22) | `ornith-ai/Ornith-1.5-35B-A3B-GGUF` **Q4_K_M** (21.71 GB) + `mmproj-Ornith-1.5-35B-BF16` (0.90 GB) | Qwen3.5-MoE fine-tune, same size class as qwen3.6 (no UD quants published) | Same flags as qwen3.6, temp 0.6 / top_p 0.95 / top_k 20. Measured 38.4 tok/s, about 1/3 of qwen3.6's thinking tokens (bench/ab-2026-10-22.md) |
| **`gemma4-26b`** (`gemma4:26b`, `gemma4:26b-64k`, `gemma4:26b-128k`) | `unsloth/gemma-4-26B-A4B-it-qat-GGUF` **UD-Q4_K_XL** (14.25 GB = 13.27 GiB) + `mmproj-BF16` (1.19 GB) | The only QAT-based quant, and the best 4-bit quality. With mmproj and 128k KV it is about 1.5-2 GiB over budget. Measured: `--fit` puts the experts of blk 24-29 (6 of 30 layers) on the CPU, which is cheap for a 3.8B-active MoE. **One entry replaces three context tags** | `-c 131072 -ctk q8_0 -ctv q8_0 -t 8 -tb 8 -ub 2048 -b 2048`, with `--fit` deciding the expert split. Keep `--swa-full` off, because the default SWA cache plus context checkpoints is what keeps 128k cheap |
| `granite-vision` (`ibm/granite3.3-vision:2b`) | `ibm-granite/granite-vision-3.3-2b-GGUF` **Q8_0** (2.69 GB) + `mmproj-model-f16` (0.89 GB) | Q8 is near-lossless and faster than BF16 (5.07 GB). Same quant as Ollama today | GPU, `-ngl 99 -np 4 --kv-unified -c 32768`, temp 0 (one image is about 5.2k tokens, so the 4 slots share a unified 32k pool). Member of the chat group (swaps the chat model out during docling jobs) |
| `granite-vision-4.1` (new, no Ollama name) | `ibm-granite/granite-vision-4.1-4b-GGUF` **Q8_0** (3.61 GB) + `mmproj-model-f16` (1.16 GB) | Official GGUF, needs llama.cpp b9534 or newer. Built for chart, table and KVP extraction; reads chart labels and values where 3.3 only says "a bar chart" | GPU, the same flags as `granite-vision`. Chat group. docling-proxy's default picture-description preset ([../bench/docling-2026-10-16.md](../bench/docling-2026-10-16.md)) |
| `granite-docling` (`ibm/granite-docling:258m`) | `ibm-granite/granite-docling-258M-GGUF` **BF16** (0.33 GB) + `mmproj-model-f16` (0.19 GB) | BF16 is the only official file, and it's tiny | GPU, `-ngl 99 -np 4 --kv-unified --special -c 32768`, temp 0 (`--special`: DocTags tags are special tokens). Chat group. The VLM pipeline renders every page, so the GPU is about 10x faster than the CPU |
| `bge-m3` (`bge-m3:latest`) | **copy of the Ollama blob** `sha256-daec91ff...` (F16, 1.16 GB) | Identical weights, so the Qdrant vectors stay valid. Any re-quant would force a re-index | **CPU** `-dev none -ngl 0 --embeddings -c 8192 -b 8192 -ub 8192 -t 4`. `ub` must be at least the input length. Parity vs Ollama: min cosine 0.999969, PASS |
| `snowflake-arctic-embed2` (`...:latest`, `...:latest_8192`) | **copy of the Ollama blob** `sha256-8c625c95...` (F16) | Same reason. The blob needs a charsmap fixup (applied by `import-ollama`); `files[].sha256` is the sha of the fixed file | CPU, same flags. Parity vs Ollama: min cosine 0.999952, PASS |
| `bge-reranker-v2-m3` (`linux6200/bge-reranker-v2-m3:latest`) | `gpustack/bge-reranker-v2-m3-GGUF` **Q8_0** (0.64 GB) | Q8 is about 1.8x less memory traffic than FP16 (1.16 GB), with negligible ranking change | **GPU** `-ngl 99 -fa on -c 8192 -b 8192 -ub 2048 -t 4 --reranking`, about 370 MiB. 10 chunks x ~350 tok: 0.2-0.65 s on the dGPU vs 3.7-4.9 s on the CPU and 5.8 s on the 610M. That is why qwen3.8 uses `-ub 256` |
| `colibri-big` | colibri model dir | n/a | n/a (see the colibri doc) |

**Disk:** about 60 GB in total (qwen3.8 14.1, qwen3.6 23.3, gemma 15.4, granite 4.1, reranker 0.6, embedders 2.4), compared with 78 GB for Ollama today. Add 14.3 GB if you take the IQ4_XS upgrade below.

### Measured speed (llama-swap v263-vulkan-b11541; image now v264-vulkan-b11580, see bench/verification-2026-10-13.md)

Generation, Phase 0 prompt (512 tokens). Source: [bench/llama-swap-results.md](../bench/llama-swap-results.md).

| Model | Ollama (Phase 0) | llama-swap |
|---|---|---|
| qwen3.8 27B dense | 6.6 tok/s (Q4_K_M, 22% on CPU, 4k ctx) | **32.4 tok/s** (UD-Q3_K_XL, 100% GPU, 48k ctx), about 5x |
| gemma4 26B-A4B | 33.0 tok/s (Q4_0, 4k ctx) | **43-47 tok/s** (QAT UD-Q4_K_XL, 128k ctx), about 1.4x |
| qwen3.6 35B-A3B | qwen3.5 not measured (Ollama hung) | **31.5 tok/s** (UD-Q4_K_XL, 128k ctx) |

Full-context fit check (no OOM, NVMe swap 0 B):

| Model | Prompt | Prompt speed | Generation at depth | Memory |
|---|---|---|---|---|
| qwen3.8-27b | 48,368 tok | 367 tok/s (ub 256) | 26.4 tok/s | VRAM 15.2 GB, all 65 layers on GPU |
| qwen3.6-35b-a3b | 126,026 tok | 734 tok/s | 24.6 tok/s | experts of 20 layers in host RAM, 11.4 GB RAM available |
| gemma4-26b | 120,033 tok | 801 tok/s | 29.7 tok/s | experts of 6 of 30 layers in host RAM, 17.6 GB RAM available |

### Optional upgrade: Qwen3.8 UD-IQ4_XS

Applies if the monitor is moved to the iGPU (+1.13 GiB of VRAM).

| | Value |
|---|---|
| File | **UD-IQ4_XS** (14.25 GB) instead of UD-Q3_K_XL |
| Benefit | Better quality at the same context |
| Flags | Same as above, `-c 49152` |
| Trade-off | Unity and Blender need `DRI_PRIME=1` to render on the dGPU |

### Parameters for all llama-server entries (as `macros` in llama-swap)

| Flag | Value | Reason |
|---|---|---|
| `-fa on` | always | Needed for quantized V cache; supported on Vulkan |
| `-ctk q8_0 -ctv q8_0` | chat models | Halves KV with negligible quality loss (same as the Ollama setting) |
| `--fit on --fit-target 1024` | default | Adapts to the desktop, Unity or Blender VRAM use at load time. No `-ngl` on the big chat models |
| `-np 2 --kv-unified` | chat models | Lets Open WebUI run a background task (title or tags) alongside a chat. With an explicit `-np 2`, llama.cpp otherwise gives each slot c/np (48k becomes 2x24k); `--kv-unified` makes the slots share one pool of the full size |
| `-lm dio` | chat models | Direct I/O, replaces `--no-mmap` (removed in b11541; `-lm/--load-mode {auto,none,mmap,mlock,mmap+mlock,dio}`). With mmap, page-cache eviction after a model swap made qwen3.6 re-read CPU-side experts from NVMe during inference: 3.8-14 tok/s prompt, 16-19 tok/s generation instead of 31.5 |
| `-ctxcp 8` | Qwen and Gemma | Context checkpoints let hybrid and SWA models reuse prompts across turns. The default of 32 per slot can use a lot of RAM. Measured per checkpoint: qwen3.6 62.8 MiB, qwen3.8 149.6 MiB, gemma4 106.3 MiB (worst case 8 × 2 slots: 1.0 / 2.3 / 1.7 GiB) |
| `-cram 2048` | all | The default 8 GiB host prompt cache competes with MoE experts for RAM |
| `-t 8 -tb 8` | MoE models | Physical cores. Measured: 6, 8 and 12 threads are within noise |
| `-ub -b` | 256 qwen3.8 / 2048 MoE (`-b 2048`) | MoE, measured prompt speed: 2048 = 1040 tok/s, 1024 = 750, 512 = 470 (generation unchanged). qwen3.8: 256 keeps all 65 layers on the GPU with the reranker in VRAM, -8% prompt speed (367 vs 398 tok/s at 512) |
| `--no-mmproj-offload` | qwen3.8 only | Frees VRAM. No measurable effect on the MoE models once the mmap issue was fixed |
| `--jinja`, `--reasoning-format auto` | defaults | Thinking output goes into `reasoning_content`, which Open WebUI shows as a thinking block |
| MTP (`--spec-type draft-mtp`) | **Gemma 4: on** (2026-10-13), others off | No VRAM to spare for Qwen3.8. Gemma 4's 252 MB draft works on Vulkan (+20–34% tg at depth with `--fit-target 2048`), but `--fit` ignores the draft's VRAM; see [bench/verification-2026-10-13.md](../bench/verification-2026-10-13.md) |

### Sampling defaults

Taken from the official model cards. Clients can override per request.

| Model | Server default (thinking) | Override for |
|---|---|---|
| Qwen3.8-27B | temp 1.0, top_p 0.95, top_k 20, min_p 0, presence 0, repeat 1.0 (matches the Ollama config) | Non-thinking: 0.7 / 0.8 / 20, presence 1.5 |
| Qwen3.6-35B-A3B | temp 1.0, top_p 0.95, top_k 20, min_p 0, presence 1.5 | **Coding: temp 0.6, presence 0.** Set this in the Continue or agent config. Non-thinking: 0.7 / 0.8, presence 1.5 |
| Gemma 4 26B | temp 1.0, top_p 0.95, top_k 64 (matches) | n/a |
| granite-vision / docling | temp 0 | n/a |

### Open WebUI task model

Set *Task model* = "current model". Otherwise title and tag generation request a different model and swap out the chat model on every message.

### Game-dev session caveat

Unity and Blender take another 1-3 GiB of VRAM. Qwen3.8 then can't keep 48k on the GPU; `--fit` spills layers to RAM, which is slow. While the editors are open, use **Qwen3.6**, where `--fit` moves experts instead (graceful), or Claude Code.

### Aliases

Aliases keep old Ollama model names working for docling params, scripts and Open WebUI presets. qwen3.5 and glm-4.7-flash get no aliases, so update any presets that still use them.

---

## 3. Who does what: Qwen3.8-27B (dense) vs Qwen3.6-35B-A3B (MoE)

- **Context:** Qwen3.8 gets **48k with vision** in VRAM, while Qwen3.6 gets 128k.
- **Why keep Qwen3.8:** a dense 27B uses all 27B parameters on every token, while the MoE uses only 3B. Qwen's own numbers have the 3.8 27B well ahead on hard coding and agentic tasks (SWE-bench Pro 61.7; Terminal-Bench 2.1 73.0 vs 63.4 for the 3.6 27B).

| Use `qwen3.8-27b` (48k, vision) | Use `qwen3.6-35b-a3b` (128k) |
|---|---|
| Focused, hard problems where quality matters more than context: one C# system or shader, a tricky bug in 1-3 files, Blender Python scripts, math for isometric projection or physics | Long-context work: agent loops (Continue/OpenCode-style) over many files, long chats, RAG answers over many chunks, summarising long docs or logs |
| **Vision:** critiquing renders and screenshots of the PS1 look, reading reference images, UI mockups (Qwen3.8 is the strongest local VLM) | Fast general Q&A. With only 3B active parameters it generates faster, even with experts in RAM |
| Code review of a diff | First-pass drafts that Qwen3.8 or Claude Code refine |

- **If you mostly run agentic coding with large context,** Qwen3.6 becomes the daily driver and Qwen3.8 becomes the "specialist" you swap to. Each swap takes about 10-20 s from page cache.
- **Benchmark both on your own prompts** (Phase 3 measured speed and fit only, not answer quality). If Qwen3.8 isn't clearly better at 32-64k on your tasks, drop it and save 14 GB.

---

## 4. How to change a model's quant

1. **Add the new file** and pin it (see [model-downloads.md](model-downloads.md)):
   ```bash
   bin/llm-pull add unsloth/Qwen3.8-27B-GGUF UD-IQ4_XS --mmproj mmproj-F16.gguf --id qwen3.8-27b
   ```
   The command prints a ready llama-swap snippet and updates `llama-swap/models.lock.yaml`.
2. **Edit `llama-swap/config.yaml`:** point `-m /models/...` at the new file and adjust `-c` if needed ([llama-swap-config.md](llama-swap-config.md)).
3. **Restart:**
   ```bash
   docker compose restart llama-swap
   curl -s localhost:11434/running | jq .
   ```
4. Run the fit check from the ROADMAP (full-context prompt, no OOM) before relying on it. After the change, `llm-pull gc` removes the old file.

---

## Sources

- https://huggingface.co/Qwen/Qwen3.8-27B
- https://huggingface.co/unsloth/Qwen3.8-27B-GGUF
- https://huggingface.co/Qwen/Qwen3.6-35B-A3B
- https://huggingface.co/unsloth/Qwen3.6-35B-A3B-GGUF
- https://huggingface.co/google/gemma-4-26B-A4B-it
- https://huggingface.co/unsloth/gemma-4-26B-A4B-it-qat-GGUF
- https://unsloth.ai/docs/models/gemma-4/qat
- https://ollama.com/library/gemma4/tags
- https://huggingface.co/ibm-granite/granite-vision-3.3-2b-GGUF
- https://huggingface.co/ibm-granite/granite-vision-4.1-4b-GGUF
- https://huggingface.co/ibm-granite/granite-docling-258M-GGUF
- https://huggingface.co/gpustack/bge-reranker-v2-m3-GGUF
- https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md
- https://github.com/ggml-org/llama.cpp/pull/22673
- https://github.com/ollama/ollama/issues/4510
