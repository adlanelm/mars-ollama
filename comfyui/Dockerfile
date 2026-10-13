# ComfyUI on ROCm for the RX 7800 XT (gfx1101). Everything (torch, ComfyUI, custom nodes) lives in this image.
# Custom nodes are baked in (pinned to the commits krita-ai-diffusion v1.53.0 expects); models/output/user are volumes.
FROM ubuntu:24.04
ARG COMFY_VERSION=v0.40.0
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
    clone https://github.com/city96/ComfyUI-GGUF ComfyUI-GGUF 6ea2651e7df66d7585f6ffee804b20e92fb38b8a; \
    clone https://github.com/Acly/comfyui-tooling-nodes comfyui-tooling-nodes 0abe742480d4d2e98cc7c6f432f9442463a7bca3; \
    clone https://github.com/Acly/comfyui-inpaint-nodes comfyui-inpaint-nodes 5b7681746fe19e563113d27366ba85236bc1ba5b; \
    clone https://github.com/Fannovel16/comfyui_controlnet_aux comfyui_controlnet_aux a587a4c1594524c1d2de5cd1a86cd868ff448e1c; \
    clone https://github.com/cubiq/ComfyUI_IPAdapter_plus ComfyUI_IPAdapter_plus a0f451a5113cf9becb0847b92884cb10cbdec0ef
WORKDIR /opt/ComfyUI
# NOT expandable_segments: on this stack it pins ~12 GB outside torch's accounting and VAEDecode OOMs (bench/comfyui-results.md)
ENV TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1 PYTORCH_TUNABLEOP_ENABLED=0
EXPOSE 8188
# No --use-flash-attention (ComfyUI #15585); PyTorch SDPA uses the bundled aotriton kernels.
# --disable-pinned-memory --cache-ram 6: keep host RAM use bounded (30 GiB box shared with other services)
CMD ["python", "main.py", "--listen", "0.0.0.0", "--port", "8188", "--use-pytorch-cross-attention", "--disable-pinned-memory", "--cache-ram", "6"]
