# mars-ollama docs

This repo runs the local AI stack: the LLM server (Ollama on `:11434`, **llama-swap + llama.cpp** on `:11435` until cutover) plus docling-proxy. These docs describe the migration and the game-asset workflow built on top of it.

**Status: Phases 0-3 done (2026-10-11).** llama-swap runs side by side with Ollama on `:11435`; the cutover to `:11434` (Phase 4) is pending. Execution order and progress are tracked in [../ROADMAP.md](../ROADMAP.md). Measurements: [../bench/llama-swap-results.md](../bench/llama-swap-results.md).

| Doc | What's in it |
|---|---|
| [architecture.md](architecture.md) | Current vs target stack, request flow, groups and swapping, VRAM/RAM budgets, ports, design decisions |
| [models.md](models.md) | Audit of the old Ollama models (MoE fact-check), target quants and parameters with sizing math, sampling, which model to use for what |
| [model-downloads.md](model-downloads.md) | `llm-pull` (replaces `ollama pull`): manifest, pinning, verification, Ollama import with charsmap fixup; `-hf` quick experiments |
| [llama-swap-config.md](llama-swap-config.md) | The live `config.yaml` and compose service explained: flags, groups, pinned image, mounts |
| [clients.md](clients.md) | Reconfiguring Open WebUI (chat, RAG embeddings, reranker), docling and Page Assist; alias table |
| [colibri.md](colibri.md) | Slow big-model tier: use cases, integration, disk and RAM gates |
| [gpu-switching.md](gpu-switching.md) | `gpu-mode`: sharing one 16 GB GPU between LLMs, ComfyUI, docling and editors |
| [game-asset-workflow.md](game-asset-workflow.md) | AI-assisted asset pipeline for the PS1/Signalis-style isometric Unity game |
| [trellis.md](trellis.md) | TRELLIS.2 image → textured .glb on ROCm (gfx1101): build, pins, how to run, measurements, licences |
| [krita.md](krita.md) | Krita (Flatpak) + krita-ai-diffusion v1.53.0 as a custom-server client of the local ComfyUI: install, connect, styles, limits, troubleshooting |
| [licenses.md](licenses.md) | Licence registry for everything that can put content into the game (verdicts, conditions, revisions), already-produced assets, prioritised replacement plan, provenance tagging |

## How do I…

| Task | After migration |
|---|---|
| See available models | `curl -s localhost:11435/v1/models \| jq '.data[].id'` |
| See what's loaded | `curl -s localhost:11435/running` (`:11434` after cutover) |
| Download a new model (the `ollama pull` replacement) | `bin/llm-pull add <org>/<repo>-GGUF <QUANT>`, paste the printed snippet into `llama-swap/config.yaml`, then `docker compose restart llama-swap` ([model-downloads.md](model-downloads.md)) |
| Try a model quickly | Add a config entry with `llama-server -hf <repo>:<quant>` ([model-downloads.md](model-downloads.md#tier-2)) |
| Check for updated GGUFs | `bin/llm-pull outdated` |
| Switch chat models | Just request the other model id. llama-swap unloads the current one |
| Free the GPU for ComfyUI | `bin/gpu-mode comfy` (unloads everything and re-warms the aux models); go back with `bin/gpu-mode llm` |
| Roll back to Ollama | `docker compose stop llama-swap && docker compose --profile legacy up -d ollama`, then revert client settings ([../ROADMAP.md](../ROADMAP.md#phase-4--cutover-on-11434)) |

## Keeping things up to date
Run `bin/check-updates`: one table of pinned vs latest for models, git pins (ComfyUI and its custom nodes, trellis, colibri), the llama-swap image, torch in `comfyui-rocm` and the Krita plugin and the Blender MCP pin (in psx-iso). It only reports (exit 4 if anything is OUTDATED, 2 on network failure).
- To update a pin: edit it in the file named in the table's first column, rebuild the image, then run `trellis/licence-check.py` and the smoke test `trellis/run_workflow.py --image concept.png`.
- Models: `bin/llm-pull update <id>` for GGUFs; edit `comfyui/models.txt` for ComfyUI files, then `bin/comfy-pull`.
- A ComfyUI update builds a new `comfyui-rocm:<ver>` tag and rebuilds and recreates only `comfyui`; the old tag is kept while TRELLIS uses it (`trellis/Dockerfile` `COMFY_BASE`). TRELLIS moves to a new base only when you choose to: bump `COMFY_BASE`, rebuild, licence-check, smoke test. Remove an old base tag only after nothing uses it.
- Pins stay where they are; nothing is bumped automatically.
- After a torch/ROCm update, re-run the idle-CPU check in [rocm-idle-spin.md](rocm-idle-spin.md) (known ROCm 7.x runtime spin, one core at 100% while ComfyUI is up).

## Conventions
- **[verify]** marks a claim that could not be confirmed from a primary source during research (2026-10-10/11). Resolve it in the roadmap phase where it appears.
- Paths: models live in `~/llm/models` (mounted read-only), config in `./llama-swap/`, helper scripts in `./bin/`.
