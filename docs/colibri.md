# Colibri: slow big-model tier

Status: **planned**, nothing implemented yet. Written 2026-10-11. Items marked **[verify]** could not be confirmed from primary sources during research.

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

Build colibri with `VK=1` in a container of the same base distro as the llama-swap image (so the binary links against compatible libraries), then bind-mount the binary and models into llama-swap. No docker socket is needed.

```yaml
# llama-swap config.yaml (member of group `chat`)
models:
  colibri-big:
    cmd: /colibri/c/coli serve --model /colibri-models/qwen38-flash-next --port ${PORT} --host 127.0.0.1
    cmdStop: /colibri/c/coli stop
    env:
      - COLI_VULKAN=1
      - COLI_VK_DEV=<7800 XT index>
```

- `--port` is confirmed in colibri's `docs/SETTINGS.md`.
- The model path above is the plan's example. Point it at whichever model you chose.
- Find the Vulkan index of the RX 7800 XT with `llama-server --list-devices` and keep it away from the Radeon 610M.
- Compose volumes: `~/llm/colibri` and the models dir mounted into llama-swap (paths in [architecture.md](architecture.md)).

### Option B (fallback): docker run

```yaml
colibri-big:
  cmd: docker run --rm --name colibri ...
  cmdStop: docker stop colibri
```

This needs `/var/run/docker.sock` mounted into llama-swap. **Security warning:** access to the docker socket is root-equivalent host access. Accept that consciously, and prefer option A.

## Build steps

1. Start a container from the same base distro as the llama-swap image.
2. Clone `https://github.com/JustVugg/colibri` and build with `VK=1`. Check the README for exact commands.
3. Copy the output to `~/llm/colibri/` on the host.
4. Download the chosen model to `~/llm/colibri/models/`.

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
