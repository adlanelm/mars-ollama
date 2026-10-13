# Verification of the open llama-swap items (2026-10-13)

Resolves the open **[verify]** items of the LLM hosting migration (Phases 0–7). Raw numbers: `verification-2026-10-13.jsonl`.
Scripts: `verify-bench.sh` (throwaway llama-server container, production flags of one model, load time, VRAM,
3 runs with a fixed prompt), `verify-mtp.sh` (Gemma 4 with and without MTP over `verify-mtp-prompts.json`),
`verify-ctxcp.sh` (growing multi-turn conversation on slot 0, RSS and checkpoint log lines).

Method: the production llama-swap stayed untouched (aux models loaded, reranker on the GPU, desktop about 1.74 GiB VRAM).
Before each test the chat models were unloaded from llama-swap and ComfyUI was freed; only one test container ran at a time.
The fixed prompt is `verify-prompt.txt` (the start of `docs/models.md`, about 5.5k tokens) through `/v1/chat/completions`,
256 tokens, temperature 0, `ignore_eos`, `cache_prompt: false`. pp is the mean of runs 2–3 (run 1 warms up), tg the mean of 3 runs.
VRAM is `rocm-smi` total used on GPU0, including the desktop and the reranker.

## 1. ROCm vs Vulkan (`v263-rocm-b11541` vs `v263-vulkan-b11541`, same llama.cpp build 11541)

| Model | Backend | Load | VRAM loaded / peak | pp tok/s | tg tok/s |
|---|---|---|---|---|---|
| gemma4-26b | Vulkan | 9–15 s | 15.3 / 15.4 GiB | **2112–2123** | **47.3–50.2** |
| gemma4-26b | ROCm (`--fit-target 2400`) | 17 s | 15.5 / 15.7 GiB | 1955 | 41.4 |
| qwen3.6-35b-a3b | Vulkan | 13–15 s | 15.2 / 15.3 GiB | 1565 | 38.5–41.0 |
| qwen3.6-35b-a3b | ROCm (`--fit-target 2400`) | 27–29 s | 15.3 / 15.4 GiB | 1469–1777 (unstable) | **42.8–44.3** |
| qwen3.8-27b | Vulkan | 6 s | 15.0 / 15.1 GiB | 444 | **20.9** |
| qwen3.8-27b | ROCm (`--fit-target 2400`) | 7 s | 15.0 / 15.3 GiB | 473 | 16.7 |

**Verdict: stay on Vulkan.** ROCm wins only qwen3.6 generation (+8%) and loses gemma4 (−17% tg) and qwen3.8 (−20% tg).
It also loads 1.2–2x slower, the image is 22.9 GB instead of 0.9 GB, and:

- **ROCm reports the wrong free VRAM.** `--list-devices` shows `16002 MiB free` with the desktop and reranker holding 1.7 GiB; Vulkan
  shows `14631 MiB free` (correct). `--fit` on ROCm would overcommit by about 1.4 GiB, which is the VRAM overflow that crashed the
  desktop before. The ROCm runs above needed a manual `--fit-target 2400` (1024 + 1371 MiB). A fixed margin cannot adapt to Unity or Blender.
- With Vulkan, host-side MoE weights don't count in RSS. With ROCm they do (qwen3.6: 12.8 GiB RSS), so compare host RAM with `free`, not RSS.

**llama-swap v264** (2026-10-12; `bin/check-updates` reports OUTDATED): its changes are the swap-matrix `auto:orphans` set, an `/audiocpp`
endpoint and capability detection for decision models. None of them affects the backends or speed. Image tags `v264-{vulkan,rocm}-b11561` and
`-b11580` exist. No ROCm re-test is needed for it. It is an optional bump, and the fit check should be re-run after it.

## 2. MTP on Vulkan (Gemma 4 26B-A4B, `mtp-gemma-4-26B-A4B-it.gguf`, 252 MB Q4_0)

Draft file from `unsloth/gemma-4-26B-A4B-it-qat-GGUF` at the pinned revision `7b92b5b2`, sha256 `7272d975…768a`, downloaded for the test to
`~/llm/llama-cache/mtp-test/` (not in `models.lock.yaml`). Flags: `--spec-type draft-mtp --spec-draft-model … --spec-draft-n-max 4`.

| Test | Without MTP | With MTP | Acceptance |
|---|---|---|---|
| 4 short prompts × 400 tok (code, math, list, Python), `--fit-target 1400` | 59.5–60.2 tok/s | **83.5–104.9 tok/s** (+40–75%) | 60–83% |
| 5.6k-token summary, `--fit-target 1400` | 48.3 | **64.8** (+34%) | 63% (182/291) |
| 5.6k-token summary, `--fit-target 2048` | 44.3 | **59.4** (+34%); +20% vs production (49.5) | 62% |

- **Works, no garbage output** on Vulkan with this build (the old Vulkan bug reports don't reproduce).
- **Correctness:** output stays coherent and the answers are the same (17 × 23 + 144 / 12 = 403 in both), but it is **not bit-identical** at
  temperature 0. 1 of 4 short prompts matched exactly; the others diverge at near-tie wording ("against" / "with"). That is batched-verification
  numerics, not a wrong draft: changing only the expert split (`--fit-target` 1024 vs 1400, no MTP) also changes the greedy text, and the MTP run at
  1400 matched the non-MTP run at 1024 byte for byte.
- **VRAM is not budgeted by `--fit`:** the log says `failed to measure the memory of the extra model, fitting without it`. The extra VRAM
  varied from +0.23 to +1.0 GiB between runs. At `--fit-target 1536`, two runs reproduced an anomaly: VRAM fell from 15.5 to 14.6 GiB
  after the first request and prompt speed collapsed to about 670 tok/s. That points to driver eviction at the VRAM edge. At `--fit-target 2048` it was clean
  (peak 15.2 GiB, pp 1947, tg 59.4).
- Not applied to production. Proposal in "Recommendations".

## 3. `-ctxcp` checkpoint RAM cost (Vulkan, production flags, 12-turn conversation up to about 11.8k tokens)

Checkpoint size (from the llama-server log, `created context checkpoint N of 8 (… size = X MiB)`). It is fixed per model and doesn't grow with context:

| Model | Per checkpoint | RSS after 7 checkpoints | Worst case (8 × 2 slots) |
|---|---|---|---|
| qwen3.6-35b-a3b (DeltaNet hybrid) | **62.8 MiB** | 690 MiB vs 287 MiB with `-ctxcp 0` (+403 MiB ≈ 7 × 62.8 − overhead) | 1.0 GiB |
| qwen3.8-27b (DeltaNet hybrid, dense) | **149.6 MiB** | 1365 MiB (load 282) | 2.3 GiB |
| gemma4-26b (SWA, 4096-token window state) | **106.3 MiB** | 910 MiB (load 264) | 1.7 GiB |

`-ctxcp 8` stays: worst case 1.0 GiB beside qwen3.6's experts in RAM is inside the budget. Prompt reuse when appending turns works with
`-ctxcp 0` too. Checkpoints matter when the client rewrites history (for example when it drops reasoning), which forces a rollback.

## 4. granite-docling's trailing `<|end_of_text|>` (docling-serve 1.32.0 in docling-proxy)

Synthetic one-page PDF (reportlab: heading, paragraph, 3×4 table; no real data) → `POST :8000/v1/convert/file`, `pipeline=vlm`,
`vlm_pipeline_model_api` → `http://ollama:11434/v1/chat/completions`, model `granite-docling`, `response_format: doctags`.
- Raw llama-server output (direct call, temperature 0) ends with `</doctag><|end_of_text|>`.
- docling output: `status: success`, 18.8 s including the model load. Markdown has a clean heading, paragraph and pipe table (all cells correct). `doctags_content`
  ends with `</doctag>`, and `<|end_of_text|>` occurs **0 times** in md, doctags or json. docling's granite-docling spec has
  `stop_strings: ["</doctag>", "<|end_of_text|>"]`. **Resolved: handled, output is clean.**
- The `vlm_pipeline_model_api` field is deprecated (warning) but still works in 1.32.0.

## 5. Open WebUI's pre-migration reranker config

**Not recoverable.** Ollama's data and logs were removed in Phase 5. Open WebUI's settings need an admin token or DB access on jupiter
(k3s), which this host doesn't have (no kubectl, ssh host key not trusted). No Phase 0 screenshot is recorded.
Current state from the llama-swap access log: Open WebUI (192.168.1.87, `python-requests`) calls `/v1/rerank`, 21 calls in 72 h,
median 1.9 s, max 2.8 s (Phase 4 measured 14–17 s per query; the cause of the drop, tuning or lighter queries, is not visible from here).
Check in the UI: Admin → Settings → Documents → Reranking engine "External", URL `http://mars.internal:11434/v1/rerank`, model
`bge-reranker-v2-m3`, **Relevance threshold 0** (scores are raw logits), Top K Reranker ≤ 10.

## 6. Other locally checkable items

| Item | Result |
|---|---|
| Flags fit in 16 GB | Yes. Loaded 15.0–15.3 GiB, peak ≤ 15.45 GiB on Vulkan with the reranker and desktop, no OOM |
| **qwen3.8 "all 65 layers on GPU"** | **No longer true.** With the reranker (370 MiB) and desktop (1.74 GiB) in VRAM, `--fit` (1 GiB margin) places **63 of 65** layers (log: `63 layers, 13222 MiB used, 1099 MiB free`): 20.9–21.5 tok/s tg at 5.5k depth. `--fit-target 768` gives 64 layers, 23.9 tok/s. `-c 40960` gives **65 layers, 26.7 tok/s (+25%)**, peak 15.1 GiB. `-ub 128` doesn't help (63 layers) |
| Gemma 4 KV "half if K=V stored once" | No. `fit-params` context memory is 998 MiB at 64k and 1678 MiB at 128k, so **10.6 KiB/token** (K and V stored separately), 128k ≈ 1.64 GiB including SWA |
| Gemma 4 `--fit` split | Now experts of about blk 18–29 on the CPU (`fit-params`) (was 24–29 in Phase 3, before the reranker moved to the GPU) |
| Reranker scores | Correct order (isometric doc 2.98, orthographic −3.10, unrelated −11.0); raw logits; 10 chunks / 5.1k tokens in 0.31 s |

Needs the user: the Phase 0 settings screenshots, the Qwen3.8 UD-IQ4_XS upgrade (needs the monitor moved to the iGPU), the Phase 8
vision-critic licence and fit, and the Page Assist UI.

## Recommendations (not applied)

1. **Vulkan stays.** Optional: `v264-vulkan-b11580` (no speed-relevant changes; re-run `verify-bench.sh` afterwards).
2. **Gemma 4 MTP:** pin the draft with `llm-pull` (`MTP` file, revision `7b92b5b2`), then add
   `--spec-type draft-mtp --spec-draft-model /models/unsloth/gemma-4-26B-A4B-it-qat-GGUF/mtp-gemma-4-26B-A4B-it.gguf --spec-draft-n-max 4 --fit-target 2048`
   to `gemma4-26b`. That gives +20% tg at 5.6k depth and up to +75% on short answers, peak 15.2 GiB. Watch VRAM for a few days, because `--fit` doesn't count the draft.
3. **qwen3.8:** choose between 48k at 21 tok/s (now) and `-c 40960` at 26.7 tok/s with all layers on the GPU. `--fit-target 768` (48k, 64 layers,
   23.9 tok/s) is the middle ground with about 0.9 GiB less safety margin.

## Addendum: llama-swap v264-vulkan-b11580 (llama.cpp b11580), 2026-10-13

Same fixed prompt and flags (`verify-bench.sh`), qwen3.6-35b-a3b: load 12.1 s, VRAM 15.17 / 15.24 GiB, pp 1493-1591, **tg 42.3-42.5 tok/s**
(v263: 38.5-41.0). Smoke test after the switch: all chat models answer; gemma4 with MTP 83.6 tok/s (86% accepted); qwen3.8 23.9 tok/s
(short prompt); bge-m3 and the reranker OK. The colibri binary (built on v263) still runs in the new image (all libraries resolve).
