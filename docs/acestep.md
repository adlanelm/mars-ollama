# ACE-Step 1.5 on the RX 7800 XT (text → music)

**Status 2026-10-12: works.** First run on gfx1101: an 80 s instrumental takes 12–16 s (DiT only) or 30–38 s (with the 1.7B LM), plus 7 s / 24 s model load, at a whole-card VRAM peak of 13.8 GiB / 14.7 GiB (desktop included). Used for the vertical slice's station music loop (`psx-iso` `Assets/_Project/Audio/Music/station_ambient_loop.wav`, notes in `psx-iso/art-src/Audio/README.md`). Research and licence background: [gamekit docs/characters-audio-research.md](../../gamekit/docs/characters-audio-research.md) section 4.

It runs as a one-shot container (`acestep-rocm:1.5-ca1e85f`, no compose service). It is **not** covered by gpu-guard / gpu_arbiter ([gpu-switching.md](gpu-switching.md)): `acestep/run.sh` refuses to start while a chat model is loaded or a ComfyUI container is up, and kills the job if VRAM goes over 15 GiB.

## Files

| File | What |
|---|---|
| [`acestep/Dockerfile`](../acestep/Dockerfile) | ubuntu 24.04 + venv + PyTorch **2.11.0+rocm7.2** (torch/torchvision/torchaudio pinned), ACE-Step clone pinned to `ca1e85fe`, upstream `requirements-rocm-linux.txt` (no torchcodec, no nano-vllm/flash-attn) plus the non-CUDA rest of `pyproject.toml`, `pip install --no-deps -e .`. `pip freeze` in `/opt/acestep-pip-freeze.txt`. Image 17 GB, build ~6 min |
| [`acestep/generate.py`](../acestep/generate.py) | Our batch script (entrypoint): one generation per seed, writes `<name>_s<seed>.wav` (48 kHz stereo) + `.json` (request, all params used, LM metadata, timings, torch peak) |
| [`acestep/run.sh`](../acestep/run.sh) | GPU pre-checks, `docker run`, 1 s VRAM sampling into `<name>.peaks.log`, watchdog (`VRAM_KILL_MIB`, default 15360) |
| [`acestep/fetch-weights.sh`](../acestep/fetch-weights.sh) | Pinned download of the main bundle into `~/llm/acestep/checkpoints` (9.4 GB, not gated, no token) |

Why torch 2.11 and not 2.14 like `comfyui-rocm`: torchaudio's last ROCm 7.2 wheel is 2.11.0 (torchaudio is in maintenance mode upstream) and `acestep/audio_utils.py` imports it at module level. torchao 0.16.0 logs "Skipping import of cpp extensions due to incompatible torch version"; INT8 weight-only quantization still applies (log line `DiT quantized with: int8_weight_only`).

## Run

```bash
cd ~/devel/mars-ollama
docker build -t acestep-rocm:1.5-ca1e85f acestep/            # once
acestep/fetch-weights.sh                                     # once, ~9.4 GB
acestep/run.sh --caption "dark ambient drone soundscape, beatless, ..." --duration 80 --bpm 50 \
  --keyscale "D minor" --timesignature 4 --seeds 8808,9909,1010 --name nolm --no-lm
# -> ~/llm/acestep/out/nolm_s9909.{wav,json}, nolm.peaks.log
```

Options of `generate.py`: `--lyrics` (default `[Instrumental]`; anything else turns vocals on, `\n` is a line break; a syllable vocalise such as `[Verse]\nooh... aah...` gives wordless vocals, intelligibility not verified by ear) and `--vocal-language` (default `unknown`), added 2026-10-13 (rebuild the image after editing `generate.py`: it is copied in), `--no-lm` (DiT only), `--lm acestep-5Hz-lm-0.6B` (lower tier; the 0.6B LM is a separate repo, not fetched), `--no-quant`, `--steps` (8), `--shift` (3.0, recommended for turbo), `--dit`.

Settings are upstream's **12–16 GB tier** ("tier5" in `acestep/gpu_config.py`): turbo DiT on the GPU with INT8 weight-only quantization (`torch.compile` on), VAE and text encoder offloaded to the CPU between uses, 1.7B LM on the PyTorch backend (`ACESTEP_LM_BACKEND=pt`; nano-vllm needs flash-attn). Environment from upstream's `start_api_server_rocm.sh` and `docs/en/INSTALL.md`: `HSA_OVERRIDE_GFX_VERSION=11.0.1` (gfx1101 is listed there for the RX 7800 XT), `MIOPEN_FIND_MODE=FAST`. Upstream's ROCm default dtype is **float32** (`ACESTEP_ROCM_DTYPE`, chosen for Strix Halo stability); we kept it.

Quirks:
- `/ckpt` must be writable: on init upstream copies its own `acestep/models/turbo/*.py` over the HF copies in `acestep-v15-turbo/` (`_sync_model_code_files`). So the checkpoint dir's two `.py` files match the pinned code, not the HF revision.
- Negative phrases in the caption ("no drums") are not reliable; the LM-planned runs had a regular transient pulse almost every time. For beatless drones, `--no-lm` was clearly better (see results).
- The VAE decode picks its chunk size from free VRAM, so the whole-card peak tracks what is free; torch's own peak was 11.3–12.9 GiB.

## Measured (2026-10-12, 80 s requests, 8 steps, batch 1)

| Mode | Load | Per track | torch peak | Whole-card VRAM peak (desktop baseline 0.75–0.88 GiB) |
|---|---|---|---|---|
| DiT + LM 1.7B (3 batches, 10 tracks) | 22.6–24.7 s | 30.1–38.8 s (LM 22 s, DiT 4.3 s diffusion, VAE decode 6.8 s, offload 2.3 s) | 12.56–12.91 GiB | 14,658–14,746 MiB |
| DiT only (`--no-lm`, 3 tracks) | 7.0 s | 12.1–15.8 s | 11.25–11.56 GiB | 13,779 MiB |

No OOM, no hang, no MIOpen stall. The output ends with 1–9 s of digital silence (the model stops before the requested duration) and is peak-normalised to -1 dBFS by upstream (`enable_normalization`).

Headroom is thin with the LM: 14.7 GiB leaves ~1.3 GiB, less than Unity or Blender take. **With the editors open, use `--no-lm`**, or `--lm acestep-5Hz-lm-0.6B` (not tested), or `ACESTEP_ROCM_DTYPE=bfloat16` (not tested on this card).

## Licences (verified 2026-10-12 at the pinned revisions)

| Component | Licence | Note |
|---|---|---|
| ACE-Step 1.5 code `ace-step/ACE-Step-1.5@ca1e85fe` | MIT (`LICENSE`, "Copyright (c) 2026 ACEStep"; `pyproject.toml` `license = MIT`) | |
| Weights `ACE-Step/Ace-Step1.5@19671f40` (turbo DiT, VAE, 5Hz LM 1.7B) | MIT (card `license: mit`, not gated). Card: outputs may be used commercially; training data "licensed", "royalty-free / no-copyright" and synthetic (MIDI-to-audio): the authors' claim | sha256: DiT `3f6e0797…`, LM `f161689d…`, VAE `da17edb6…` |
| Bundled `Qwen3-Embedding-0.6B` (text encoder) | Apache-2.0 upstream (`Qwen/Qwen3-Embedding-0.6B@97b0c614`); the bundle has no separate licence file | sha256 `0437e45c…` |
| LM base (5Hz LM 1.7B is fine-tuned from Qwen3-1.7B) | Apache-2.0 (`Qwen/Qwen3-1.7B@70d244cc`) | |
| VAE | `AutoencoderOobleck` architecture (diffusers, Apache-2.0). The weights are ACE-Step's own: 48 kHz, downsampling 2·4·4·6·10 = 1920, unlike Stable Audio Open's VAE (44.1 kHz, 2048), so they are not the Stability-licensed weights **[verify in the tech report]** | |
| Python deps in the image | Permissive per `pip-licenses`; `torchao` reports UNKNOWN but is BSD-3-Clause (pytorch/ao). No GPL/AGPL/NC packages. Nothing in the image ships | |
| Generated audio | Commercial use allowed by the card. **EU copyright status of purely AI-generated audio is open** (no protection may mean others can copy it) **[verify with a lawyer before release]**; check for similarity to known tracks | tag `acestep-mit` |

Registry row: [licenses.md](licenses.md) section 5.

## Not done / next

- `bin/check-updates` does not know the ACE-Step / torch 2.11 pins yet.
- Base / SFT DiT (CFG, 50 steps) and the XL DiT were not tried; nor the 0.6B LM or bf16.
- No listening test is possible here; quality was judged by spectrogram and loudness analysis only.
