#!/usr/bin/env bash
# Fetch the ACE-Step 1.5 main bundle (turbo DiT, VAE, Qwen3-Embedding-0.6B text encoder, 5Hz LM 1.7B; ~9.4 GB)
# at a pinned revision into ~/llm/acestep/checkpoints. No token needed (not gated). Host python via uv.
set -euo pipefail
REV=19671f406d603126926c1b7e2adc169acbcade22   # ACE-Step/Ace-Step1.5, 2026-02-03, card license: mit
DEST=${1:-$HOME/llm/acestep/checkpoints}
mkdir -p "$DEST"
HF_HUB_DISABLE_TELEMETRY=1 uv run -q --no-project --with huggingface_hub python -I -c "
import sys; from huggingface_hub import snapshot_download
print(snapshot_download('ACE-Step/Ace-Step1.5', revision='$REV', local_dir=sys.argv[1]))" "$DEST"
