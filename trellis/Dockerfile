# TRELLIS.2 (image -> textured 3D mesh) for the RX 7800 XT (gfx1101), as ComfyUI custom nodes.
# Derived from comfyui-rocm:0.39.2 (torch 2.14.1+rocm7.2, ComfyUI v0.39.2); that image stays untouched.
# Install: our own install.sh (every git clone pinned, gfx1101 targets); the recipe follows the one validated on RDNA3
# by toastmanAu/trellis-2-rocm-comfyui @2c93cb94.
# Spec and numbers: docs/trellis.md. Weights are NOT baked in: models/Trellis2 is a volume (~/llm/trellis/models).
# Licence-clean (docs/licenses.md): nvdiffrast, nvdiffrec renderutils and MeshLib (all non-commercial) are not built or
# installed; licence-stubs/ satisfies their module-level imports and raises on use. Colour is baked on vertices.
FROM comfyui-rocm:0.39.2
USER root
ARG ROCM_APT=7.2
# HIP toolchain to compile the CUDA extensions (CuMesh, FlexGEMM, o-voxel) with hipcc.
# Must match torch's bundled HIP 7.2 (torch.version.hip 7.2.53211). The Mesa GL/EGL packages were for nvdiffrast's
# GL rasterizer; they are MIT-licensed and kept so this layer stays cached (open3d/cv2 also load libGL).
RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
 && mkdir -p /etc/apt/keyrings \
 && curl -fsSL https://repo.radeon.com/rocm/rocm.gpg.key | gpg --dearmor -o /etc/apt/keyrings/rocm.gpg \
 && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/rocm.gpg] https://repo.radeon.com/rocm/apt/${ROCM_APT} noble main" > /etc/apt/sources.list.d/rocm.list \
 && printf 'Package: *\nPin: release o=repo.radeon.com\nPin-Priority: 600\n' > /etc/apt/preferences.d/rocm-pin-600 \
 && apt-get update && apt-get install -y --no-install-recommends \
      hipcc hip-dev hip-runtime-amd rocm-llvm rocm-device-libs hsa-rocr-dev rocm-core \
      rocprim-dev hipcub-dev rocthrust-dev \
      cmake ninja-build lsof iproute2 libsparsehash-dev libeigen3-dev \
      libgl1-mesa-dev libegl1-mesa-dev libgles2-mesa-dev libgbm-dev libegl-mesa0 libgl1-mesa-dri \
      libxrender1 libxi6 libxkbcommon0 libsm6 \
 # torch's ATen/hip headers include hipblas/hipblaslt/hipsolver/hipsparse headers; the libs themselves ship
 # inside the torch wheel, so take only the include/ trees (avoids GBs of rocBLAS/hipBLASLt kernels).
 && mkdir /tmp/debs && cd /tmp/debs \
 && apt-get download hipblas-dev hipblas-common-dev hipblaslt-dev hipsolver-dev hipsparse-dev hiprand-dev rocrand-dev rocblas-dev rocsolver-dev rocsparse-dev \
 && for d in *.deb; do dpkg -x "$d" x; done && cp -rn x/opt/rocm-*/include/* /opt/rocm/include/ \
 && cd / && rm -rf /tmp/debs /var/lib/apt/lists/* \
 && test -x /opt/rocm/bin/hipcc && test -f /opt/rocm/include/hipblas/hipblas.h
USER comfy
ENV ROCM_HOME=/opt/rocm HIP_HOME=/opt/rocm CUDA_HOME=/opt/rocm FORCE_CUDA=1 \
    PYTORCH_ROCM_ARCH=gfx1101 HCC_AMDGPU_TARGET=gfx1101 AMDGPU_TARGETS=gfx1101 GPU_ARCHS=gfx1101 MAX_JOBS=8
ARG TOAST_COMMIT=2c93cb946a08
# Installs into /opt/ComfyUI with python3 (= /opt/venv via PATH).
COPY --chown=comfy install.sh /opt/trellis-install.sh
RUN bash /opt/trellis-install.sh \
 && rm -rf /opt/ComfyUI/models/facebook ~/.cache/pip \
 # the node pack vendors prebuilt nvdiffrast/nvdiffrec_render wheels and an installer that would pip-install them
 && rm -rf /opt/ComfyUI/custom_nodes/ComfyUI-Trellis2-GGUF/wheels /opt/ComfyUI/custom_nodes/ComfyUI-Trellis2-GGUF/install.py
# Node-pack bugfix from the toast guide (PostProcess Mesh remove_infinite_vertices NameError); 05 is BF16-only, skipped.
RUN cd /opt/ComfyUI/custom_nodes/ComfyUI-Trellis2-GGUF \
 && curl -fsSL https://raw.githubusercontent.com/toastmanAu/trellis-2-rocm-comfyui/${TOAST_COMMIT}/patches/06-postprocess-infinite-vertices.patch | git apply \
 # rembg was installed --no-deps by the script; add its runtime deps (u2net background removal, Apache-2.0)
 && pip install onnxruntime==1.31.0 pymatting==1.1.16 jsonschema \
 # platformdirs: needed by pooch (rembg); it used to arrive via controlnet_aux's yapf/fvcore, dropped in a587a4c1
 && pip install platformdirs==4.13.0 \
 # Aero-Ex pins transformers==5.2.0, which rejects the base's huggingface_hub 2.2 / tokenizers 0.23.3:
 # go back to the base image's 5.19.0 (has DINOv3ViTModel).
 && pip install --no-deps transformers==5.19.0 \
 && python -c "import transformers; from transformers import DINOv3ViTModel" \
 && pip freeze > /opt/trellis-pip-freeze.txt
# open3d's pybind needs libusb (kept as a late layer so the compiled layers above stay cached)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libusb-1.0-0 && rm -rf /var/lib/apt/lists/*
USER comfy
# rembg >= 2.0.6x defaults to bria-rmbg (BRIA RMBG-2.0, CC BY-NC 4.0): force a permissive model instead.
# birefnet-general (BiRefNet, MIT code + weights; ONNX ~973 MB). Not downloaded at run time: trellis/fetch-weights.py pre-fetches it
# (pinned sha256) into /cache/u2net, which rembg uses as is.
# isnet-general-use is not used: DIS licenses its code, not the weights (docs/licenses.md).
RUN cd /opt/ComfyUI/custom_nodes/ComfyUI-Trellis2-GGUF \
 && python - <<'PY'
p = "nodes.py"; s = open(p).read()
old = "            from rembg import remove\n            image = remove(image)"
assert s.count(old) == 1, "rembg call not found"
s = s.replace(old, "            from rembg import remove, new_session\n"
                   "            image = remove(image, session=new_session(os.environ.get('REMBG_MODEL', 'birefnet-general')))")
open(p, "w").write(s)
PY
# Licence stubs on sys.path via a .pth file; licence-check.py proves no NC package or file is left (build fails otherwise).
COPY --chown=comfy licence-stubs /opt/licence-stubs
COPY --chown=comfy licence-check.py /opt/licence-check.py
RUN echo /opt/licence-stubs > "$(python -c 'import site; print(site.getsitepackages()[0])')/licence_stubs.pth" \
 && python /opt/licence-check.py
ENV TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1 PYTORCH_TUNABLEOP_ENABLED=0 ATTN_BACKEND=sdpa \
    U2NET_HOME=/cache/u2net TRITON_CACHE_DIR=/cache/triton HF_HOME=/cache/hf REMBG_MODEL=birefnet-general
EXPOSE 8188
CMD ["python", "main.py", "--listen", "0.0.0.0", "--port", "8188", "--use-pytorch-cross-attention", "--disable-pinned-memory", "--cache-ram", "6"]
