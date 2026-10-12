# SkinTokens / TokenRig (auto-rigging: skeleton + skin weights, MIT code + weights) on ROCm, RX 7800 XT (gfx1101).
# FEASIBILITY TEST ONLY (2026-10-12), see docs/skintokens.md. Code = PR #14 head (ROCm/CPU support, unmerged),
# flash-attn replaced by PyTorch SDPA by that PR. Python 3.11 because bpy (Blender as a module) has no cp312 wheel
# before 5.1, and 5.1+ is cp313 only. Weights are NOT in the image: ~/llm/skintokens/{experiments,models} are mounted.
FROM ghcr.io/astral-sh/uv:0.12.24 AS uv
FROM ubuntu:24.04
ARG SKINTOKENS_COMMIT=b576ed8d0ee5d68d45474e9f9546a9597fa8ce4b
ARG TORCH=2.14.1 TORCHVISION=0.29.1 BPY=4.5.14
ENV DEBIAN_FRONTEND=noninteractive UV_NO_CACHE=1 UV_PYTHON_INSTALL_DIR=/opt/python
# libatomic1: torch ROCm wheel; libx*/libsm/libgl: bpy imports them even headless
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates libatomic1 libgl1 libegl1 libglib2.0-0 libgomp1 libxrender1 libxi6 libxxf86vm1 libxfixes3 libxkbcommon0 libsm6 libice6 libx11-6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /usr/local/bin/uv
RUN userdel -r ubuntu 2>/dev/null; useradd -m -u 1000 -s /bin/bash rig && mkdir -p /opt && chown rig /opt
USER rig
ENV VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
RUN uv venv --python 3.11 $VIRTUAL_ENV \
 && uv pip install torch==${TORCH} torchvision==${TORCHVISION} --index-url https://download.pytorch.org/whl/rocm7.2 \
 && uv pip freeze | grep -E '^(torch|torchvision|pytorch-triton-rocm)==' > /opt/torch-constraints.txt
RUN git clone https://github.com/VAST-AI-Research/SkinTokens /opt/skintokens \
 && git -C /opt/skintokens fetch origin pull/14/head \
 && git -C /opt/skintokens checkout ${SKINTOKENS_COMMIT} \
 && uv pip install -c /opt/torch-constraints.txt -r /opt/skintokens/requirements.txt "bpy==${BPY}" \
 && uv pip freeze > /opt/skintokens-pip-freeze.txt \
 && ! uv pip show flash-attn 2>/dev/null
WORKDIR /opt/skintokens
# gfx1101 is a native target of the torch ROCm wheels; HSA_OVERRIDE_GFX_VERSION=11.0.1 is a no-op here, kept for parity with acestep
ENV HSA_OVERRIDE_GFX_VERSION=11.0.1 HF_HUB_OFFLINE=1 HF_HUB_DISABLE_TELEMETRY=1 GRADIO_ANALYTICS_ENABLED=False \
    MIOPEN_FIND_MODE=FAST MIOPEN_USER_DB_PATH=/cache/miopen TRITON_CACHE_DIR=/cache/triton
ENTRYPOINT ["python", "demo.py"]
