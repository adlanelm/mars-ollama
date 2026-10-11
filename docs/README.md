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
| [krita.md](krita.md) | Krita (Flatpak) + krita-ai-diffusion v1.53.0 as a custom-server client of the local ComfyUI: install, connect, styles, limits, troubleshooting |

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

## Conventions
- **[verify]** marks a claim that could not be confirmed from a primary source during research (2026-10-10/11). Resolve it in the roadmap phase where it appears.
- Paths: models live in `~/llm/models` (mounted read-only), config in `./llama-swap/`, helper scripts in `./bin/`.
