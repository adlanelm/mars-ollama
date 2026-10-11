# Colibri: slow big-model tier

Status: **build done, download in progress at hand-off (verify with `~/llm/colibri/verify.sh`), GPU test pending**. Config is prepared but not applied. Updated 2026-10-11. Items marked **[verify]** could not be confirmed from primary sources during research.

See also: [architecture.md](architecture.md), [gpu-switching.md](gpu-switching.md), Phase 7 in [../ROADMAP.md](../ROADMAP.md).

## What it is

[colibri](https://github.com/JustVugg/colibri) (Apache-2.0) streams MoE experts from NVMe, so it can run models far larger than VRAM plus RAM. It runs at about 1-5 tok/s (the README's reported range), and cold starts are much slower. It is a `colibri-big` member of the exclusive `chat` group in llama-swap: requesting it unloads the chat model, and the CPU aux models stay loaded.

## Use cases

| Good for | Not for |
|---|---|
| Overnight or batch generation: lore, item text, Signalis-style in-world documents, dialogue drafts, localisation drafts | Chat |
| An offline second-opinion review of a whole subsystem or design doc | Autocomplete |
| A consistency pass over the lore corpus | Agents |
| Optionally, asset tagging via `/v1/systemone` | Anything that needs Unity or Blender open |

## Model choice

Pick **one**, don't keep both. Disk is fine: 324 GB free now, about 345 GB after the migration cleanup, and ComfyUI models take about 60-80 GB.

| Model | Size | Spare disk after | Note |
|---|---|---|---|
| **Qwen3.8-Flash-Next** (suggested) | 186 GB | about 80-100 GB | Same family as the other models, tool calling supported |
| DeepSeek V4 Flash | 167 GB | about 100-120 GB | |

Download to `~/llm/colibri/models/`.

## RAM gates

- colibri needs **24 GB minimum, 32 GB recommended**. This machine has 30 GiB.
- Run it **only with Unity, Blender and ComfyUI closed**.
- The aux models stay loaded: about 3.1 GiB RAM (bge-m3 1.3, snowflake 1.3, reranker 0.5; the reranker weights live in VRAM) plus ~370 MiB VRAM. Headroom is tight. Check `free -h` and `swapon --show` during the first run.

## Integration

### Option A (preferred): bind-mount into llama-swap

colibri is built from source (v2.0.0, `make qwen38 VK=1`) inside a throwaway container of the llama-swap image, so nothing is installed on the host. The output tree is bind-mounted into llama-swap; no docker socket is needed. `coli` is a Python launcher (llama-swap's image ships python3.14) that spawns the C engine `c/qwen38`.

Applied (2026-10-11): the `colibri-big` entry and its `chat` group membership in [../llama-swap/config.yaml](../llama-swap/config.yaml), and two volumes on the `llama-swap` service in [../docker-compose.yaml](../docker-compose.yaml): `$HOME/llm/colibri/bin:/colibri:ro,Z` and `$HOME/llm/colibri/models:/colibri-models:Z` (read-write: colibri writes `.coli_kv`, `.coli_usage`, `.coli_ssd` there). `healthCheckTimeout` stayed at 600.

Paths in the container: launcher `/colibri/c/coli`, engine `/colibri/c/qwen38`, shaders `/colibri/c/shaders/` (found beside the binary), model `/colibri-models/qwen38-flash-next`.

```yaml
models:
  colibri-big:
    ttl: 600
    cmd: |
      python3 /colibri/c/coli serve --model /colibri-models/qwen38-flash-next
      --host 127.0.0.1 --port ${PORT} --model-id colibri-big
      --ram 18 --cap 32 --ctx 8192 --kv-slots 1 --max-queue 2 --queue-timeout 3600
    cmdStop: python3 /colibri/c/coli stop --port ${PORT}
    checkEndpoint: /health
    env: [COLI_VULKAN=1, COLI_VK_DEV=0, COLI_NO_OMP_TUNE=1, RAM_GB=18, CAP=32, RSS_GUARD_GB=22, PYTHONDONTWRITEBYTECODE=1]
```

`coli serve` flags (from `--help`, v2.0.0): `--model`, `--ram`, `--auto-tier`, `--no-tune-profile`, `--ctx`, `--gpu` (auto, none or device list), `--vram`, `--policy {quality,balanced,experimental-fast}`, `--repin`, `--cap`, `--ngen`, `--topp`, `--topk`, `--effort`, `--think/--no-think`, `--temp`, `--host`, `--port`, `--model-id`, `--api-key`, `--cors-origin`, `--max-queue`, `--queue-timeout`, `--kv-slots`, `--allowed-host`, plus cluster flags.

Facts confirmed in the source:
- `/health` is public (no `COLI_API_KEY` needed); with a key set it only hides internals. Binding to 127.0.0.1 needs no key.
- `coli serve` does **not** run `coli plan` by itself. The plan is applied only with `--auto-tier`. `coli plan` is a separate, CPU-only, read-only command.
- The server handles SIGTERM and stops its engine child. `coli stop --port N` is a backup.
- The pidfile goes to the temp dir (`/tmp/coli-serve-<port>.pid`), not the read-only mount.

### RAM settings

30 GiB host, container `mem_limit: 28g`, aux models resident (about 3.1 GiB): `--ram 18` / `RAM_GB=18`, `CAP=32` (README range for this model is 32-96 experts per layer, so 32 is the low end), `RSS_GUARD_GB=22`, `--ctx 8192`, one KV slot. Raise `--ram` only after watching `free -h` and `swapon --show` (swap must stay 0 B). Page cache for streamed experts counts against the cgroup.

### Files colibri writes into the model dir

`.coli_kv` (KV cache, warm reopen), `.coli_usage` (usage history for pinning) and `.coli_ssd`. The `/colibri-models` mount is therefore **read-write**. For a read-only mount set `KVSAVE=0` and `USAGE_SAVE=0`.

### Model

`Qwen/Qwen3.8-Flash-Next-FP8` at revision `236dfdf285828023ca3bcd3f37366c58a3469b13`, 185,563,783,577 bytes in 144 files, in `~/llm/colibri/models/qwen38-flash-next`. Ungated, Qwen Community License, no conversion needed. The optional int4 sidecar (+68 GB, about 4 tok/s in README's numbers) is **not** created.

Resume or verify the download:

```bash
uvx --from "huggingface_hub[hf_xet]" hf download Qwen/Qwen3.8-Flash-Next-FP8 \
  --revision 236dfdf285828023ca3bcd3f37366c58a3469b13 --local-dir ~/llm/colibri/models/qwen38-flash-next
~/llm/colibri/verify.sh   # sizes of all files, sha256 of 3 shards + non-shard files
```

### Plan (CPU only)

Run after the download, in a throwaway container, with no GPU device passed:

```bash
docker run --rm --entrypoint python3 -v ~/llm/colibri/bin:/colibri:ro -v ~/llm/colibri/models:/colibri-models:ro \
  ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 -B /colibri/c/coli plan --model /colibri-models/qwen38-flash-next --ram 18 --ctx 8192 --gpu none
```

Output: *not yet recorded* (download unfinished).

### Option B (fallback): docker run

```yaml
colibri-big:
  cmd: docker run --rm --name colibri ...
  cmdStop: docker stop colibri
```

This needs `/var/run/docker.sock` mounted into llama-swap. **Security warning:** access to the docker socket is root-equivalent host access. Accept that consciously, and prefer option A.

## Build steps (done, 2026-10-11)

Containerised; nothing installed on the host. The script is [`colibri/build.sh`](../colibri/build.sh) (it replaced `~/llm/colibri/build.sh`); it also applies the local patches in [`colibri/patches/`](../colibri/patches/README.md):

```bash
docker run --rm --entrypoint bash -v $PWD/colibri:/src:ro,Z -v ~/llm/colibri/bin:/out:Z \
  ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 -c 'mkdir -p /work; bash /src/build.sh'
# build.sh: apt-get install git build-essential libvulkan-dev glslc libomp-dev
#           git clone --branch v2.0.0 https://github.com/JustVugg/colibri, git apply colibri/patches/*.patch
#           cd c && make qwen38 VK=1; copy the tree (without .git) to /out, chown 1000:1000
```

- The llama-swap image is Ubuntu 26.04.1, so the glibc matches exactly. The release tarball was not used.
- Tag v2.0.0 is an annotated tag; it resolves to commit `bf2442915d6e3dd4cdfd2eb9c2a3d2aa44a25850`.
- The engine is built with `-march=native`: it runs on this Ryzen 7 7700 only (fine for this host; rebuild if the CPU changes).
- Needs only libm, libgomp and libc at link time; Vulkan is loaded with dlopen (libvulkan.so.1 is in the image).
- `ldd /colibri/c/qwen38` and `coli --help` verified inside the llama-swap image. No GPU was used.
- Only the `qwen38` engine was built. For another model family, build its target (`make <engine> VK=1`) the same way.

## GPU test (2026-10-11)

Applied: compose volumes and the `colibri-big` entry (chat group) in `llama-swap/config.yaml` (see Integration). `healthCheckTimeout` stayed at 600: the server is healthy 7 s after start (resident weights 11 GB RSS), experts load lazily.

Placement (from the log): Vulkan on the RX 7800 XT, dense chain 48/48 layers on the device (3.7 GiB), VRAM expert tier 9.6 GiB (1,897 experts), RAM cache `--ram 18 --cap 25`. The engine banner says "| CPU" regardless; the `[VK]` lines are what counts.

| Run | Request | Result |
|---|---|---|
| 1, cold cache | 82-token prompt, 300 tokens, thinking on (default) | 610 s, ~0.5 tok/s. 68k experts from disk, 42k RAM hits, 38% of expert work on the GPU. All 300 tokens were reasoning, no answer |
| 2, warmer | same, 250 tokens, `chat_template_kwargs.enable_thinking=false` | 365 s, ~0.68 tok/s. Ignored: still all reasoning |
| 3, warm | 120 tokens max, top-level `"enable_thinking": false`, streamed | prefill 16 s, decode ~80 tok in 71 s (~1.1 tok/s), total 88 s, a real answer; 53% of expert work on the GPU |

During the runs: NVMe reads about 1.25 GB/s, RAM available 11–14 GiB, NVMe swap 0 B, aux models still served.

Findings:
- **Thinking:** Qwen3.8 in colibri defaults to `reasoning_effort: xhigh`. For batch text send `"enable_thinking": false` or `"reasoning_effort": "none"` at the top level of the request; `chat_template_kwargs` is not read.
- **Logging:** upstream prints a summary only when a request ends (TTFT, tok/s, cache stats). The local patch adds `[progress]` lines (start, first token, every `COLI_PROGRESS_SECS`, finish). Follow with `bin/colibri-logs`.
- Not done yet: the overnight batch (task 5). iobench / tune: see the next section.

## Tuning (2026-10-11)

Result: warm decode **1.42 → 3.68 tok/s** (2.6×), prefill 15.6 → 10.4 s, with the same greedy output. Two changes: `OMP_NUM_THREADS=8` and `--cap 44`. All runs used the same synthetic prompt (a derelict-station maintenance log), `temperature 0`, `"enable_thinking": false` and 120 output tokens. Each config ran twice; run 2 is warm and reuses the prompt prefix. The `.coli_usage` history was reset to the same copy before each config. Experiments ran in a throwaway container (`docker run ... coli serve`, same image, devices, volumes and `--memory 28g`) while colibri-big was unloaded in llama-swap. The final row is through llama-swap.

### Disk: iobench

`coli iobench` does not exist in v2.0.0. It is a separate C tool, `c/iobench.c` (`make -C c iobench`), built here in a throwaway container: `iobench <file> <MB> <reads> <threads> <direct>`. The engine reads one expert as a 3.3 MB gate+up pair plus a 1.6 MB down matrix, using buffered `pread`. `DIRECT`/`PIPE` are read by the GLM engine only. Kingston Fury Renegade 2 TB, btrfs (the shards' data extents are not compressed):

| Block | 1 thread | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| 3.3 MB O_DIRECT | 4.0 GB/s | 6.8 | 7.4 | 7.4 | 7.6 |
| 3.3 MB buffered | 2.7 GB/s | 4.9 | 7.1 | 8.5* | 8.1* |
| 1.6 MB O_DIRECT | 2.6 GB/s | 4.7 | 6.1 | 6.8 | 6.7 |

\* Partly page-cache hits (the random reads overlap within one 1.6 GB shard).

The engine actually pulled 1.3–2.5 GB/s, so the SSD is not the limit.

### coli tune

`coli tune --model ... --ram 18 --ctx 8192 --tokens 64 --repeats 2 --no-think`, with `COLI_VULKAN=1 COLI_VK_DEV=0`.
- For qwen38 it sweeps only `OMP_NUM_THREADS` (physical cores vs half) and a smaller RAM cache (75% / 50% of the planned cap). It never tries a bigger cache, threads above the core count, or the VRAM tier knobs. With `--cap` given, the cache sweep is skipped. With `COLI_NO_OMP_TUNE` set, the thread sweep is skipped.
- Measured: baseline (cap 44, 8 threads) **3.90 tok/s**, hit 36%; 4 threads 3.85; cap 33 3.62; cap 22 disqualified (it changed the greedy tokens). **Baseline retained, no profile accepted.**
- Where it saves: `$XDG_CONFIG_HOME/colibri/tuning/<fingerprint>.json` (`~/.config/...` of the container user). The fingerprint covers the CPU, the model files and the engine binary. `coli serve` applies a saved profile **only with `--auto-tier`**. llama-swap's entry does not use `--auto-tier`, and the container's `~/.config` is not persistent, so the result is written into `config.yaml` as flags/env instead (there was nothing to accept anyway).
- `coli plan` with Vulkan visible (`COLI_VULKAN=1`): 18 GB = 1.5 dense (+4.4 on the device only) + 5.9 runtime + 10.6 warm experts → **cap 44**. The earlier cap 25 came from `--gpu none`, which charged the dense weights to RAM.

### Knobs for qwen38 + Vulkan (confirmed in source)

| Knob | Default | Where read | Effect / result here |
|---|---|---|---|
| `OMP_NUM_THREADS` | physical cores, **but** `COLI_NO_OMP_TUNE=1` skips that → 16 (SMT) | `omp_tune.h`, `coli` | CPU expert matmuls; **8 = 2.4× faster than 16** |
| `--cap` / `CAP` | plan | engine argv | RAM expert cache, slots per layer (4.69 MiB each × 48 layers) |
| `COLI_VK_TIER_GB` | free VRAM − reserve − dense | `vk_tier.c` | VRAM expert budget |
| `COLI_VK_TIER_RESERVE_GB` | 1.0 | `vk_tier.c` | VRAM left for scratch/driver; 0.5 tested, not adopted |
| `COLI_VK_TIER_RATE` | 16 | `vk_tier.c` | promotions to VRAM per token (0 skipped when warm) |
| `COLI_VK_TIER_BALANCE`, `_EXCLUSIVE`, `_WARM`, `_MTP` | on | `vk_tier.c`, `qwen38.c` | CPU/GPU balance, no RAM+VRAM duplicates, warm start from `.coli_usage`, MTP layer on GPU |
| `COLI_VK_TIER_STREAM`, `_ROWS`, `_SLOTS`; `COLI_VK_TIER_GEMM_ROWS` | on, rule, 64; 16 | `vk_tier.c` | cold experts uploaded per step; prompt steps only (≥16 rows) unless GEMM_ROWS is lowered |
| `COLI_VK_DENSE`, `COLI_VK_CHAIN`, `COLI_VK_DENSE_HOST` | on (discrete) | `backend_vulkan.c`, `decide_vk.h` | dense trunk on the GPU (3.7 GiB chain, host copy released) |
| `--kv-slots` / `KV_SLOTS` | 1 | `qwen38.c` | parallel conversations; >1 unloads the MTP head and turns the dense chain off |
| `Q38_MTP`, `Q38_MTP_DRAFTS`, `COLI_LOOKUP` | on, 2, on | `qwen38.c` | speculative decoding: 2.16 tokens/forward measured |
| `Q38_EXPERT_PREFETCH`, `Q38_EXPERT_PARALLEL_READS` | on | `qwen38_core.h` | fadvise prefetch, parallel expert reads |
| `Q38_EXPERT_INT4` | auto (sidecar if present) | `qwen38_core.h` | int4-g64 experts: 2.76 instead of 4.92 MB; changes quantization (not created, +68 GB) |
| `COLI_TIMERS` | 0 | `qwen38_core.h` | per-phase timing per request (used for the evidence below) |
| `RAM_GB`, `RSS_GUARD_GB` | — | `coli plan` / GLM engine | **not read by the qwen38 engine** |

### Before / after

Decode tok/s for run 1 / run 2. Prefill is from run 1 (82 prompt tokens). Device share = the share of routed experts computed on the GPU in run 2. Peaks were sampled every 2 s. NVMe swap stayed at 0 B in every run.

| Config | Decode tok/s | Prefill | Device share | RAM avail. min | Container peak | VRAM peak (of 16,368 MiB) | Verdict |
|---|---|---|---|---|---|---|---|
| A. old config (16 threads, cap 25) | 1.35 / 1.42 | 15.6 s | 53% | 14.4 GiB | 10.0 GiB | 16,308 MiB | baseline |
| B. + `OMP_NUM_THREADS=8` | 3.13 / 3.39 | 11.3 s | 53% | 14.8 GiB | 8.9 GiB | 16,316 MiB | **adopted** |
| C. B + cap 45 | 3.79 / 3.85 | 10.5 s | 52% | 10.0 GiB | 12.9 GiB | 16,198 MiB | **adopted (as cap 44)** |
| D. C + decode streaming (`GEMM_ROWS=1 STREAM_ROWS=1`) | 2.68 / 2.68 | 9.6 s | 99% | 9.7 GiB | 12.8 GiB | 16,252 MiB | slower |
| E. C + `COLI_VK_TIER_RESERVE_GB=0.5` | 3.73 / 4.02 | 10.5 s | 55% | 9.3 GiB | 13.1 GiB | 16,312 MiB | noise; 1 failed upload, smaller prompt chunks |
| F. C + `COLI_VK_DENSE=0` (all VRAM for experts) | 3.31 / 3.40 | 11.8 s | 62% | 6.5 GiB | 16.7 GiB | 15,207 MiB | slower |
| tune: cap 44 / 8 thr · 4 thr · cap 33 | 3.90 · 3.85 · 3.62 | ~10 s | — | — | — | — | cap 44 + 8 threads best |
| **Final, via llama-swap (cap 44, 8 threads)** | **3.55 / 3.68** | **10.4 s** | 52% | 10.2 GiB | 14.3 GiB | 16,321 MiB | in `config.yaml` |

Batch (C settings, distinct prompts, explicit `cache_slot` per request):

| Slots × concurrent | Per stream | Aggregate (wall, incl. prefill) | Single stream in that mode | Container peak |
|---|---|---|---|---|
| 1 × 1, sequential (C) | 3.85 | ≈3.1–3.3 tok/s (estimated: 10.5 s prefill + 31 s decode per request) | 3.85 (MTP on) | 12.9 GiB |
| 2 × 2 | 1.60, 1.85 | 3.21 tok/s (240 tok / 74.7 s) | 3.18 (MTP off) | 14.3 GiB |
| 4 × 4 | 0.86–1.06 | 3.43 tok/s (475 tok / 138.6 s) | 3.18 (MTP off) | 14.7 GiB |

More slots give at most ~5–11% more aggregate throughput. Prefills of new requests run one at a time and stall the others, and any `--kv-slots` > 1 unloads the MTP head (−17% for single requests). So the config stays at 1 slot. If you do use slots: the server hashes the first messages to pick a slot, so concurrent requests can land on the same one and wait. Send `"cache_slot": i` per worker.

### Why the GPU looks idle

Each token routes to 10 of 512 experts in each of 48 layers. The draft rows add more, so one forward uses about 1,450 experts across about 3 rows. Only about 1,850 of the 24,576 experts (7.5%) fit in the 9.5 GiB VRAM tier. They are the hottest ones, so they still cover about half of the routed work, and the GPU finishes that half in about 50 ms per forward. The other half has to be read from RAM or NVMe and computed on the CPU, which takes about 500 ms per forward. The GPU waits for that and sits idle about 90% of the time. Before the fix, 16 SMT threads made the CPU part about 10× slower (timers: 952 vs 82 ms of routed-expert compute per forward). Pushing everything onto the GPU (test D, 99% device share) is slower, because each cold expert then has to cross PCIe every step. Taking VRAM from the dense trunk (test F) moves trunk work onto the already busy CPU. The real levers are a faster CPU path and fewer bytes per expert. One example is the int4 sidecar (+68 GB on disk, 56% of the bytes per expert), but it changes the quantization and is not tested here.

### Chosen settings

`--cap 44`, `CAP=44`, `OMP_NUM_THREADS=8` (keep `COLI_NO_OMP_TUNE=1`), `--kv-slots 1`, VRAM reserve and streaming at their defaults. The previous config is saved as `llama-swap/config.yaml.bak-2026-10-11-pretune`. Untested headroom: cap 60 would use about 3.3 GiB more RAM (RAM available would drop to ~7 GiB).

## Lore batch and blind comparison (2026-10-11)

Phase 7 task 5. `bin/colibri-batch colibri/batch/lore-50.txt ~/llm/colibri/batch/lore-50 --system colibri/batch/system.txt`: 50 synthetic in-world documents (10 station subjects × log, memo, diary, item, terminal), thinking off, max 600 tokens, temperature 0.8. Then the same prompts on `qwen3.6-35b-a3b` for comparison.

| | colibri (Qwen3.8-Flash-Next) | Qwen3.6-35B-A3B |
|---|---|---|
| Items ok / errors / empty / cut at max | 50 / 0 / 0 / 0 | 50 / 0 / 0 / 0 |
| Wall time | 57 min | 5 min |
| Speed | 2.66 tok/s (33–240 s per item) | 29.5 tok/s (3–20 s per item) |
| Output tokens | 9,150 | 9,495 |

NVMe swap stayed 0 B; the batch ran unattended through llama-swap (colibri-big swapped in, chat model swapped out).

**Blind AI judge.** A subagent got only the prompt and two unlabelled answers per item (sides randomised per item, key kept separately) and scored each answer 1–5 on fit (format, length, in-world), atmosphere, craft and usefulness for the game, with a verdict per item. Unblinded per-item results: [bench/colibri-lore-50/judge-verdicts.json](../bench/colibri-lore-50/judge-verdicts.json).

| | colibri | Qwen3.6 |
|---|---|---|
| Wins (50 items) | **40** | 9 (+1 tie) |
| Fit | **4.16** | 3.58 |
| Atmosphere | **4.16** | 3.76 |
| Craft | **4.00** | 3.40 |
| Usefulness | **4.06** | 3.82 |

By document type: logs 10–0, memos 10–0, diaries 9–1 for colibri; terminals 6–4 and items 5–4 (+1 tie) are close.

- colibri wrote 3 of the judge's top 4 texts (choir-diary, archive-terminal, security-log) but also the single worst one: android-terminal looped and was cut off mid-line.
- Qwen3.6's weak spots: an all-caps reactor log with a typo, a medbay log dated in "Stardate" instead of station cycles.
- Both models often ignore the requested length (diaries and memos especially), use markdown bold in memos, and reuse the same names (Vance, Halloway, Kael, Aris) and phrases ("ozone and copper", "it knows"). Any batch needs a de-duplication and length pass.

**Use:** colibri for longer in-world prose with a voice (logs, memos, diaries, lore passes), overnight. Qwen3.6 for short item blurbs and terminal readouts, where it is nearly as good at a tenth of the time. The judge is an AI with its own taste; the user's own blind picks (`~/llm/colibri/batch/compare.html`, built by `bin/text-compare`) are the reference when available.

## NVMe wear

- Reads don't wear an SSD, only writes do. Expert streaming is read-heavy.
- The drive is a Kingston Fury Renegade 2 TB, rated about 2000 TBW **[verify the rating]**.
- The risk is swap writes if RAM runs out. Watch the NVMe swap partition (priority -1) with:
  ```bash
  swapon --show
  ```
  It should stay at 0 B.

## Batch-job pattern

A script requests model `colibri-big` overnight and writes one JSON output per item. llama-swap unloads the chat model for the job and restores nothing afterwards, so the next chat request just loads its model.

```bash
#!/usr/bin/env bash
set -euo pipefail
mkdir -p out
for f in prompts/*.txt; do
  id=$(basename "$f" .txt)
  jq -n --rawfile p "$f" '{model:"colibri-big", messages:[{role:"user", content:$p}]}' \
    | curl -s --max-time 3600 localhost:11434/v1/chat/completions \
        -H 'Content-Type: application/json' -d @- > "out/$id.json"
done
```

- Set `healthCheckTimeout` high enough for a cold start, and raise the client timeout for slow tokens.
- Log tok/s per item. Phase 7 acceptance test: 50 lore entries overnight.
- Implemented as `bin/colibri-batch` (resumable, thinking off by default, one `<id>.md` per item, `summary.md`). Blind A/B page for two runs: `bin/text-compare`. Results above.

## Acceptance (from the roadmap)

- Requesting `colibri-big` unloads the chat model automatically, and the batch completes.
- The NVMe swap stays at 0 B, and the aux models are still served.
- Rollback: remove the `colibri-big` entry and delete `~/llm/colibri`.
