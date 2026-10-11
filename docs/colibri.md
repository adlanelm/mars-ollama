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

Ready-to-merge pieces, **not applied**:
- [../llama-swap/colibri.snippet.yaml](../llama-swap/colibri.snippet.yaml): the `colibri-big` model entry (also add it to the `chat` group members, and raise the top-level `healthCheckTimeout`).
- [colibri-compose.patch.md](colibri-compose.patch.md): the two volume lines.

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

Applied: compose volumes and the `colibri-big` entry (chat group) in `llama-swap/config.yaml`; `llama-swap/colibri.snippet.yaml` and `docs/colibri-compose.patch.md` are kept as reference. `healthCheckTimeout` stayed at 600: the server is healthy 7 s after start (resident weights 11 GB RSS), experts load lazily.

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
- Not done yet: `coli iobench` / `coli tune`, and the overnight batch (task 5).

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

## Acceptance (from the roadmap)

- Requesting `colibri-big` unloads the chat model automatically, and the batch completes.
- The NVMe swap stays at 0 B, and the aux models are still served.
- Rollback: remove the `colibri-big` entry and delete `~/llm/colibri`.
