# ACE-Step 1.5 (text-to-music, MIT code + weights) on ROCm for the RX 7800 XT (gfx1101).
# Same base pattern as comfyui/Dockerfile: ubuntu 24.04 + venv + PyTorch ROCm 7.2 wheels (pinned).
# Weights are NOT in the image: they live in ~/llm/acestep/checkpoints (mounted at /ckpt), fetched by fetch-weights.sh.
# See docs/acestep.md.
FROM ubuntu:24.04
ARG ACESTEP_COMMIT=ca1e85fe9430179831e6bc6be790c332190a3866
# torchaudio stops at 2.11.0 on the rocm7.2 index (deprecated upstream) and acestep imports it, so torch 2.11.0 to match
ARG TORCH=2.11.0 TORCHVISION=0.26.0 TORCHAUDIO=2.11.0
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv python3-dev git ca-certificates libsndfile1 libgomp1 build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN userdel -r ubuntu 2>/dev/null; useradd -m -u 1000 -s /bin/bash ace && mkdir -p /opt && chown ace /opt
USER ace
ENV VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
RUN python3 -m venv $VIRTUAL_ENV \
 && pip install torch==${TORCH} torchvision==${TORCHVISION} torchaudio==${TORCHAUDIO} --index-url https://download.pytorch.org/whl/rocm7.2 \
 && pip freeze | grep -E '^(torch|torchvision|torchaudio|pytorch-triton-rocm)==' > /opt/torch-constraints.txt
# Pinned clone. requirements-rocm-linux.txt is upstream's ROCm list (no torchcodec, no nano-vllm/flash-attn);
# the extra packages are the remaining non-CUDA deps from pyproject.toml. The constraints file keeps the ROCm torch.
RUN git clone https://github.com/ace-step/ACE-Step-1.5 /opt/acestep \
 && git -C /opt/acestep checkout ${ACESTEP_COMMIT} \
 && pip install -c /opt/torch-constraints.txt -r /opt/acestep/requirements-rocm-linux.txt \
      diskcache python-multipart typer-slim "pytorch-wavelets>=1.3.0" "pywavelets>=1.9.0" "setuptools<72" "torchao>=0.16.0,<0.17.0" \
 && pip install --no-deps -e /opt/acestep \
 && pip freeze > /opt/acestep-pip-freeze.txt
WORKDIR /opt/acestep
# ROCm settings from upstream start_api_server_rocm.sh / docs/en/INSTALL.md (gfx1101 -> 11.0.1).
# NOT expandable_segments (broken on this box, see comfyui/Dockerfile).
ENV HSA_OVERRIDE_GFX_VERSION=11.0.1 ACESTEP_LM_BACKEND=pt MIOPEN_FIND_MODE=FAST TOKENIZERS_PARALLELISM=false \
    ACESTEP_CHECKPOINTS_DIR=/ckpt ACESTEP_PROJECT_ROOT=/opt/acestep HF_HUB_OFFLINE=1 HF_HUB_DISABLE_TELEMETRY=1 \
    GRADIO_ANALYTICS_ENABLED=False TORCHINDUCTOR_CACHE_DIR=/cache/inductor TRITON_CACHE_DIR=/cache/triton \
    MIOPEN_USER_DB_PATH=/cache/miopen
COPY --chown=ace generate.py /opt/generate.py
ENTRYPOINT ["python", "/opt/generate.py"]
