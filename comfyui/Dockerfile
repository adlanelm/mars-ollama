# ComfyUI on ROCm for the RX 7800 XT (gfx1101). Everything (torch, ComfyUI, custom nodes) lives in this image.
# Custom nodes are baked in (pinned to the commits krita-ai-diffusion v1.53.0 expects); models/output/user are volumes.
FROM ubuntu:24.04
ARG COMFY_VERSION=v0.39.0
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv python3-dev git ca-certificates libgl1 libglib2.0-0 libgomp1 build-essential \
    && rm -rf /var/lib/apt/lists/*
# ubuntu:24.04 ships a uid 1000 user ("ubuntu"); replace it
RUN userdel -r ubuntu 2>/dev/null; useradd -m -u 1000 -s /bin/bash comfy && mkdir -p /opt && chown comfy /opt
USER comfy
ENV VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
RUN python3 -m venv $VIRTUAL_ENV \
 && pip install torch torchvision --index-url https://download.pytorch.org/whl/rocm7.2
RUN git clone --depth 1 --branch ${COMFY_VERSION} https://github.com/Comfy-Org/ComfyUI /opt/ComfyUI \
 && pip install -r /opt/ComfyUI/requirements.txt
WORKDIR /opt/ComfyUI/custom_nodes
RUN set -e; \
    clone() { git clone "$1" "$2" && git -C "$2" checkout "$3" && { [ ! -f "$2/requirements.txt" ] || pip install -r "$2/requirements.txt"; }; }; \
    clone https://github.com/city96/ComfyUI-GGUF ComfyUI-GGUF 01f8845bf30d89fff293c7bd50187bc59d9d53ea; \
    clone https://github.com/Acly/comfyui-tooling-nodes comfyui-tooling-nodes ca01116495cad1f2d8440641f26ced8fbdbbe8de; \
    clone https://github.com/Acly/comfyui-inpaint-nodes comfyui-inpaint-nodes 12937559e1aea4bb073e9e82f915d1dab92f248b; \
    clone https://github.com/Fannovel16/comfyui_controlnet_aux comfyui_controlnet_aux e8b689a513c3e6b63edc44066560ca5919c0576e; \
    clone https://github.com/cubiq/ComfyUI_IPAdapter_plus ComfyUI_IPAdapter_plus b188a6cb39b512a9c6da7235b880af42c78ccd0d
WORKDIR /opt/ComfyUI
# NOT expandable_segments: on this stack it pins ~12 GB outside torch's accounting and VAEDecode OOMs (bench/comfyui-results.md)
ENV TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1 PYTORCH_TUNABLEOP_ENABLED=0
EXPOSE 8188
# No --use-flash-attention (ComfyUI #15585); PyTorch SDPA uses the bundled aotriton kernels.
# --disable-pinned-memory --cache-ram 6: keep host RAM use bounded (30 GiB box shared with other services)
CMD ["python", "main.py", "--listen", "0.0.0.0", "--port", "8188", "--use-pytorch-cross-attention", "--disable-pinned-memory", "--cache-ram", "6"]
