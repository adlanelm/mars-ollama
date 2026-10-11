# Roadmap: Ollama → llama-swap + llama.cpp, then the game asset pipeline

Status: **Phases 0-3 done (2026-10-11)**; llama-swap runs side by side with Ollama on `:11435`. Phase 4 onward is pending. Results: [bench/llama-swap-results.md](bench/llama-swap-results.md).

Each phase lists its goal, prerequisites, tasks, acceptance criteria and rollback. Items marked **[verify]** could not be confirmed from primary sources during research. Resolve them during the phase where they appear.

Background docs: [docs/README.md](docs/README.md).

| Phase | Goal | Changes the running stack? | Status |
|---|---|---|---|
| 0 | Baselines and prerequisites | No | **Done** 2026-10-11 (qwen3.5 baseline not measured: Ollama hung) |
| 1 | `llm-pull` + model manifest; download the target models | No (disk only) | **Done** 2026-10-11 |
| 2 | llama-swap running side by side on `:11435` | Adds a container | **Done** 2026-10-11 |
| 3 | Validation and tuning | No | **Done** 2026-10-11 |
| 4 | Cutover on `:11434` + client reconfiguration | **Yes** | Done 2026-10-11 (Page Assist skipped: not used) |
| 5 | Cleanup of Ollama data | Deletes data | Pending |
| 6 | `gpu-mode` + ComfyUI | Adds a container | Pending (`bin/gpu-mode` exists, no `comfyui` service yet) |
| 7 | Colibri big-model tier | Adds a model entry | Pending |
| 8 | Game asset pipeline | Separate projects | Pending |

---

## Phase 0 — Baselines and prerequisites

**Goal:** have numbers to compare against, and the tools installed.

**Tasks**
1. **Record idle VRAM** (the desktop is on the dGPU):
   ```bash
   rocm-smi --showmeminfo vram | grep 'GPU\[0\]'
   ```
   Measured 2026-10-11: about 1.2 GB used when idle.
2. **Baseline Ollama speed** with a fixed prompt. Save the results to `bench/ollama-baseline.md`:
   ```bash
   PROMPT='Write a C# Unity MonoBehaviour that snaps a transform to an isometric grid. Explain briefly.'
   for m in qwen3.8:latest gemma4:26b qwen3.5:latest; do
     curl -s localhost:11434/api/generate -d "{\"model\":\"$m\",\"prompt\":\"$PROMPT\",\"stream\":false,\"options\":{\"num_predict\":512}}" \
       | jq '{model, eval_count, tps: (.eval_count / (.eval_duration/1e9)), prompt_tps: (.prompt_eval_count/(.prompt_eval_duration/1e9))}'
     docker exec ollama ollama ps
   done
   ```
3. **Baseline embeddings.** Use 5 fixed texts, synthetic and with no personal data, and save the vectors to `bench/embed-baseline-{bge-m3,snowflake}.json`:
   ```bash
   curl -s localhost:11434/api/embed -d '{"model":"bge-m3:latest","input":["text one","text two","text three","text four","text five"]}' > bench/embed-baseline-bge-m3.json
   curl -s localhost:11434/api/embed -d '{"model":"snowflake-arctic-embed2:latest","input":[...same...]}' > bench/embed-baseline-snowflake.json
   ```
4. **Record current client settings** by screenshotting the Open WebUI admin pages before changing anything:
   - Admin → Connections
   - Admin → Documents: embedding engine and model, reranker engine, model and URL, Docling settings and `DOCLING_PARAMS`
   - Admin → Interface: task model
   - Page Assist provider settings
   - Note in particular **how the reranker is configured today**. Ollama has no rerank API, so `bge-reranker-v2-m3` cannot have been served as a reranker through Ollama.
5. **Install uv** (it provides an isolated Python 3.12 for the HF CLI):
   ```bash
   sudo pacman -S uv
   uvx --from "huggingface_hub[hf_xet]" hf version
   ```
6. **Optional: move the monitor cable to the motherboard** so the Radeon 610M drives the desktop. This frees about 1.2 GB of dGPU VRAM and allows the Qwen3.8 UD-IQ4_XS upgrade ([docs/models.md](docs/models.md)). The trade-off is that Unity and Blender need `DRI_PRIME=1` to render on the dGPU.

**Acceptance criteria:** `bench/` contains the baseline numbers and embedding files, and screenshots of the client settings exist.

**Done (2026-10-11):** baselines in `bench/ollama-baseline.txt` and `bench/embed-baseline-*.json`. The qwen3.5 baseline was **not measured**: Ollama hung ("Stopping..."). Numbers: [bench/llama-swap-results.md](bench/llama-swap-results.md). Whether the screenshot item was done is not recorded **[verify]**.

---

## Phase 1 — `llm-pull` and model manifest

**Goal:** a reproducible, pinned replacement for `ollama pull`. Spec: [docs/model-downloads.md](docs/model-downloads.md).

**Tasks**
1. Create `llama-swap/models.lock.yaml` (schema in the spec) and `bin/llm-pull` (bash + `uvx hf` + `jq`).
2. **Download the target set:**
   ```bash
   bin/llm-pull add unsloth/Qwen3.8-27B-GGUF          UD-Q3_K_XL  --mmproj mmproj-F16.gguf       --id qwen3.8-27b
   bin/llm-pull add unsloth/Qwen3.6-35B-A3B-GGUF      UD-Q4_K_XL  --mmproj mmproj-F16.gguf       --id qwen3.6-35b-a3b
   bin/llm-pull add unsloth/gemma-4-26B-A4B-it-qat-GGUF UD-Q4_K_XL --mmproj mmproj-BF16.gguf     --id gemma4-26b
   bin/llm-pull add ibm-granite/granite-vision-3.3-2b-GGUF Q8_0   --mmproj mmproj-model-f16.gguf --id granite-vision
   bin/llm-pull add ibm-granite/granite-docling-258M-GGUF  BF16   --mmproj mmproj-model-f16.gguf --id granite-docling
   bin/llm-pull add gpustack/bge-reranker-v2-m3-GGUF       Q8_0                                  --id bge-reranker-v2-m3
   bin/llm-pull import-ollama bge-m3:latest                   --id bge-m3
   bin/llm-pull import-ollama snowflake-arctic-embed2:latest  --id snowflake-arctic-embed2
   bin/llm-pull du
   ```
   The total is about 60 GB under `~/llm/models`, and `/home` has 324 GB free.
3. Commit `models.lock.yaml` and `bin/llm-pull`.

**Acceptance criteria:**
- `bin/llm-pull sync` is a no-op; everything is present and every sha256 matches.
- `bin/llm-pull du` shows about 60 GB.

**Rollback:** `rm -r ~/llm/models` (nothing else is touched).

**Resolved**
- `hf download --revision` with Xet works, and `.lfs.oid` is present in the tree API.
- The Ollama bert blobs load in llama.cpp: bge-m3 as is, snowflake after a charsmap fixup that `llm-pull import-ollama` applies automatically ([docs/model-downloads.md](docs/model-downloads.md)).

**Done (2026-10-11):** all 8 entries pinned in `llama-swap/models.lock.yaml`, about 60 GB. Details: [bench/llama-swap-results.md](bench/llama-swap-results.md).

---

## Phase 2 — llama-swap side by side on `:11435`

**Goal:** the new stack running next to Ollama, without touching clients. Config: [docs/llama-swap-config.md](docs/llama-swap-config.md).

**Tasks**
1. Create `llama-swap/config.yaml` (live file; explained in the doc).
2. **Add the `llama-swap` service** to `docker-compose.yaml` with **host port `11435:8080` for now**. Use no `ollama` network alias yet, so docling-proxy still reaches the real Ollama.
3. **Pin the image to a version tag:** `ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541`. Resolved: it bundles `llama-server` (llama.cpp build 11541). The entrypoint is overridden to `-config /config/config.yaml -listen :11434` (hot reload removed after it caused 502s; apply edits with `docker compose restart llama-swap`) and the `llama-swap/` directory is mounted at `/config`.
4. **Check the Vulkan device order:**
   ```bash
   docker compose run --rm --entrypoint llama-server llama-swap --list-devices
   ```
   Resolved: `GGML_VK_VISIBLE_DEVICES=0` (Vulkan0 = RX 7800 XT, Vulkan1 = 610M). `group_add` uses the numeric GIDs 983 and 987.
5. Run `docker compose up -d llama-swap`, then check `curl -s localhost:11435/v1/models | jq '.data[].id'`.

**Acceptance criteria:**
- All model IDs and aliases are listed.
- The aux models (bge-m3, snowflake, reranker) are preloaded: `curl localhost:11435/running`.
- Ollama on `:11434` is untouched.

**Rollback:** `docker compose rm -sf llama-swap`.

**Done (2026-10-11):** running on `:11435` with the pinned image. Details: [bench/llama-swap-results.md](bench/llama-swap-results.md). Note: a ROCm image `v263-rocm-b11541` exists; not benchmarked.

---

## Phase 3 — Validation and tuning

**Goal:** prove functional parity and pick the final parameters before any client moves.

**Tasks.** Use `:11435` throughout.
1. **Chat smoke test.** Do this for each chat model, including one tool call and one image request for qwen3.8, qwen3.6, gemma4 and granite-vision:
   ```bash
   curl -s localhost:11435/v1/chat/completions -H 'Content-Type: application/json' -d '{
     "model":"qwen3.8-27b","messages":[{"role":"user","content":"Say hi"}],"max_tokens":64}' | jq .
   ```
2. **Embedding parity.** Embed the same 5 texts through `/v1/embeddings` for `bge-m3` and `snowflake-arctic-embed2`. Compute the cosine similarity against the Phase 0 vectors. The **pass threshold is at least 0.999 for every text**. A small Python script with `uvx --with numpy` is enough.
   - If a model fails, its Open WebUI knowledge bases must be **re-indexed** after cutover.
3. **Rerank format:**
   ```bash
   curl -s localhost:11435/v1/rerank -d '{"model":"bge-reranker-v2-m3","query":"isometric camera","documents":["orthographic camera at 30 degrees","banana bread recipe"],"top_n":2}' | jq .
   ```
   The response must contain `results[].index` and `results[].relevance_score`, which is what Open WebUI's external reranker reads. Resolved: it matches. Scores are raw logits, so keep Open WebUI's relevance threshold at 0 (or in logit units).
4. **Granite:** granite-docling converts one page image to DocTags, and granite-vision describes a test image. Resolved: multimodal works for both (docling needs `--special`; granite-vision needs about 5.2k tokens per image). If either regresses, use the docling-serve local-engine fallback ([docs/clients.md](docs/clients.md)).
5. **Fit check.** Send a full-context prompt to each model (qwen3.8 48k, qwen3.6 128k, gemma4 128k).
   - There must be no OOM. qwen3.8 must log *all* layers offloaded to the GPU.
   - Record the expert split `--fit` chooses for qwen3.6 and gemma4: `docker logs llama-swap | grep -i -E 'fit|offload|n_cpu_moe'`.
6. **Speed and tuning:**
   ```bash
   docker exec llama-swap llama-bench -m /models/unsloth/Qwen3.6-35B-A3B-GGUF/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf \
     -fa 1 -ctk q8_0 -ctv q8_0 -ub 512,1024,2048 -t 8,13 -ncmoe <fit value> -p 2048 -n 256
   ```
   Pin the winning `-ub` and `-t` in `config.yaml`. Compare with the Phase 0 numbers.
   - Measured generation: qwen3.8 32.4 tok/s (Ollama 6.6), gemma4 43-47 tok/s (Ollama 33.0), qwen3.6 31.5 tok/s (qwen3.5 baseline not measured: Ollama hung). Pinned: MoE `-t 8 -tb 8 -ub 2048 -b 2048`, qwen3.8 `-ub 256`.
7. **Resources:** with qwen3.6 at 128k plus the aux models, `free -h` shows at least 3 GiB available, `swapon --show` shows the NVMe partition at 0 B, and `rocm-smi` stays at or below about 15.5 GB.
8. **Rerank latency** for 10 chunks × about 350 tokens: CPU 3.7-4.9 s, 610M iGPU 5.8 s, dGPU 0.2-0.65 s. Over the 3 s limit on the CPU, so the reranker runs on the GPU (`-ngl 99`, about 370 MiB) and qwen3.8 uses `-ub 256` ([docs/models.md](docs/models.md)).
9. **Optional: MTP.** Not done: skipped because of Vulkan bug reports. Still **[verify]**. Keep it only if outputs are correct and it is faster.

**Acceptance criteria:**
- Every check above passes, or has a documented fallback.
- `config.yaml` contains the pinned tuning values.
- Results are recorded in `bench/llama-swap-results.md`.

**Rollback:** not needed, since nothing has changed for clients.

**Done (2026-10-11):** all checks passed except the optional ones: MTP and the ROCm-vs-Vulkan comparison were not done, and `-ctxcp` RAM cost per checkpoint was not isolated. The qwen3.5 baseline was not measured (Ollama hung). Results and the findings that changed the plan (groups under `routing.router.settings.groups`, `--no-mmap` replaced by `-lm dio`, `/api/models/unload` also unloading aux, directory mount, numeric GIDs): [bench/llama-swap-results.md](bench/llama-swap-results.md).

---

## Phase 4 — Cutover on `:11434`

**Goal:** llama-swap replaces Ollama in place, and all clients work.

**Tasks**
1. **Edit `docker-compose.yaml`:**
   - Move the `ollama` service under `profiles: ["legacy"]`.
   - Change the `llama-swap` ports to `11434:8080`.
   - Add the network alias `ollama` on the `ollama` network (the llama-swap service has none yet).
2. **Apply:**
   ```bash
   docker compose stop ollama
   docker compose up -d llama-swap
   curl -s localhost:11434/v1/models | jq '.data[].id'
   ```
3. **Reconfigure the clients** per [docs/clients.md](docs/clients.md):
   - **Open WebUI:** OpenAI connection, embeddings, external reranker, and task model = current model.
   - **Docling:** `DOCLING_PARAMS` and picture-description/VLM API URLs pointed at `/v1/chat/completions`. **[verify]** at cutover how docling handles granite-docling's trailing `<|end_of_text|>` after `</doctag>`.
   - **Page Assist:** an OpenAI-compatible provider.
4. Commit the compose and config changes.

**Acceptance criteria**
- **Chat:** Open WebUI chats with qwen3.8-27b, qwen3.6-35b-a3b and gemma4-26b. Thinking shows as a collapsible block.
- **Background tasks:** Open WebUI title and tag generation does not swap models. `curl localhost:11434/running` shows a single chat model.
- **RAG:** a RAG query on an existing knowledge base returns cited chunks, and reranking is active.
- **Docling:** converting a PDF with images produces picture descriptions.
- **Page Assist:** chat works. *(Skipped 2026-10-11: not used; CORS verified, setup kept in docs/clients.md.)*

**Done (2026-10-11):** llama-swap serves `:11434` (host and in-network alias `ollama:11434`); the Ollama container was removed (`profiles: ["legacy"]`, data kept). Open WebUI chat/RAG/uploads, docling (both pipelines) and opencode verified; Page Assist pending. Results and incidents: [bench/llama-swap-results.md](bench/llama-swap-results.md#phase-4-cutover-2026-10-11). RAG rerank tuning deferred.

**Rollback** (about 1 minute):
```bash
docker compose stop llama-swap
docker compose --profile legacy up -d ollama
```
Then revert the client settings from the Phase 0 screenshots.

---

## Phase 5 — Cleanup

**Goal:** reclaim disk space.

**Tasks**
1. **Right after Phase 3 passes**, remove the models that are being dropped (about 42 GB). Do this while the legacy Ollama can still be started:
   ```bash
   docker compose --profile legacy up -d ollama
   docker exec ollama ollama rm qwen3.5:latest qwen3.5:64k qwen3.5:128k glm-4.7-flash:latest glm-4.7-flash:198k
   docker compose stop ollama
   ```
2. **After 2 weeks without needing rollback:**
   - Delete `~/llm/ollama/data` (78 GB).
   - Remove the `legacy` service block.
   - Run `docker image rm ollama/ollama:rocm`.

**Acceptance criteria:** `df -h /home` shows about 345 GB free.

---

## Phase 6 — `gpu-mode` and ComfyUI

**Status: Done (2026-10-11).** Results: [bench/comfyui-results.md](bench/comfyui-results.md). Krea 2 Turbo measured about 40 s per 1024² image (acceptance target about 30 s missed); FLUX.2 Klein 4B 7.5 s, Z-Image Turbo about 27 s.

**Goal:** a one-command switch between LLM work and image generation. Spec: [docs/gpu-switching.md](docs/gpu-switching.md).

**Tasks**
1. **Add a `comfyui` service** (ROCm, `torch 2.14.1+rocm7.2`, models in `~/llm/comfyui/models`). Use `profiles: ["comfy"]` so it doesn't start automatically. The wheel includes the aotriton files; do not use `--use-flash-attention`; ComfyUI issue #15585 describes the slowdown on this exact GPU.
2. **Write `bin/gpu-mode`** with the subcommands `llm`, `comfy`, `status`.
3. Document VRAM numbers per mode in `bench/`.

**Acceptance criteria:**
- `gpu-mode comfy`, then `gpu-mode llm`, then `gpu-mode status` shows exactly one large VRAM consumer each time.
- A Krea 2 Turbo 1024² image renders in about 30 s or less.

---

## Phase 7 — Colibri big-model tier

**Status (2026-10-11):** tasks 1–4 done and integrated (`colibri-big` in llama-swap, chat group). Measured 0.5 tok/s cold, 1.1 tok/s warm, NVMe swap 0 B, aux models kept serving; results in [docs/colibri.md](docs/colibri.md#gpu-test-2026-10-11). Live `[progress]` logging via a local patch ([colibri/patches](colibri/patches/README.md)). Task 5 (overnight 50-entry batch) pending.

**Goal:** a slow, unattended tier for models far larger than VRAM plus RAM. Spec: [docs/colibri.md](docs/colibri.md).

**Tasks**
1. Build colibri with `VK=1` in a container of the same base distro as the llama-swap image. Output goes to `~/llm/colibri/`.
2. Download **one** model, Qwen3.8-Flash-Next (186 GB, suggested) or DeepSeek V4 Flash (167 GB), to `~/llm/colibri/models/`.
3. Bind-mount the binary and models into llama-swap and add the `colibri-big` entry to the `chat` group.
4. Find the Vulkan index of the RX 7800 XT and set `COLI_VK_DEV`.
5. Run an overnight batch test: 50 lore entries, with tok/s logged.

**Acceptance criteria:**
- Requesting `colibri-big` unloads the chat model automatically, and the batch completes.
- The NVMe swap stays at 0 B, and the aux models are still served.

**Rollback:** remove the `colibri-big` entry and delete `~/llm/colibri`.

---

## Phase 8 — Game asset pipeline

This phase lives in the game project repos. Full detail: [docs/game-asset-workflow.md](docs/game-asset-workflow.md).

1. ComfyUI models and LoRAs, then a test character turnaround.
2. Krita + krita-ai-diffusion connected to the local ComfyUI.
3. TRELLIS.2 (ROCm port, gfx1101): take one prop from concept to mesh.
4. Blender MCP plus `ps1_prep.py` (decimate, UV, bake, export at 128/256px).
5. Unity URP project: PSX shader, `PsxSettings`, AssetPostprocessor, unity-mcp, git-lfs.
6. **Vertical slice:** 1 room tileset, 3 props, 1 humanoid, 1 creature, an SFX set and a music loop.

---

## [verify] checklist

Resolved (Phases 0-3, see [bench/llama-swap-results.md](bench/llama-swap-results.md)):
- [x] `llama-server` is bundled in the pinned image `v263-vulkan-b11541`; the v263 config keys work (groups live under `routing.router.settings.groups`)
- [x] `GGML_VK_VISIBLE_DEVICES` index of the RX 7800 XT: **0**
- [x] Embedder parity with Ollama: **PASS** (bge-m3 min cosine 0.999969, snowflake 0.999952; snowflake needs the charsmap fixup)
- [x] `/v1/rerank` response shape matches Open WebUI's external reranker (scores are raw logits)
- [x] granite-docling and granite-vision multimodal work in llama.cpp (docling needs `--special`)
- [x] `LLAMA_CACHE` is the env var for the `-hf` download cache
- [x] `hf download --revision` with Xet works, and `.lfs.oid` is present
- [x] Page Assist CORS (`Access-Control-Allow-Origin: *`)

Still open:
- [ ] MTP on Vulkan (Gemma 4 draft): not tested
- [ ] `-ctxcp` checkpoint RAM cost for hybrid models: not isolated
- [ ] How the reranker was actually configured in Open WebUI before migration
- [ ] ROCm vs Vulkan: image `v263-rocm-b11541` exists, not benchmarked
- [ ] docling's handling of granite-docling's trailing `<|end_of_text|>` after `</doctag>` (Phase 4)
