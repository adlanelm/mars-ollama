#!/usr/bin/env bash
# Fetch the SkinTokens checkpoints (TokenRig GRPO + FSQ-CVAE, ~1.6 GB) and the Qwen3-0.6B config/tokenizer
# (no weights; TokenRig builds the backbone from the config) at pinned revisions into ~/llm/skintokens.
# Same files as upstream download.py --model. The datasets (download.py --data) are NOT fetched.
set -euo pipefail
DEST=${1:-$HOME/llm/skintokens}
mkdir -p "$DEST"
HF_HUB_DISABLE_TELEMETRY=1 uv run -q --no-project --with huggingface_hub python -I -c "
import sys; from huggingface_hub import hf_hub_download, snapshot_download
d = sys.argv[1]
for f in ['experiments/skin_vae_2_10_32768/last.ckpt', 'experiments/articulation_xl_quantization_256_token_4/grpo_1400.ckpt', 'README.md']:
    print(hf_hub_download('VAST-AI/SkinTokens', f, revision='79736cad0fd84de384d5eede659b4ebd24effe33', local_dir=d))
print(snapshot_download('Qwen/Qwen3-0.6B', revision='c1899de289a04d12100db370d81485cdf75e47ca',
                        local_dir=d + '/models/Qwen3-0.6B', ignore_patterns=['*.bin', '*.safetensors']))" "$DEST"
