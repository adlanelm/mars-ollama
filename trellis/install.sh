#!/usr/bin/env bash
# Installs the TRELLIS.2 ComfyUI node pack (Aero-Ex/ComfyUI-Trellis2-GGUF) into the comfyui-rocm image and compiles
# its GPU extensions (CuMesh, FlexGEMM, o-voxel) for ROCm/gfx1101 with hipcc. Run at image build time by
# trellis/Dockerfile, as the comfy user, with the venv's python3 on PATH.
#
# Every clone is pinned to a commit (table in docs/trellis.md, section Pins). No model weights are fetched here:
# they live in a volume (docs/trellis.md, section Run). nvdiffrast, nvdiffrec and MeshLib are deliberately not
# installed because of their non-commercial licences; licence-stubs/ covers their imports (docs/licenses.md P0-b).
# This script is our own replacement for the unlicensed third-party script used before (docs/licenses.md P1-b).
set -euo pipefail

COMFYUI_DIR="${COMFYUI_DIR:-/opt/ComfyUI}"
NODE_DIR="${COMFYUI_DIR}/custom_nodes/ComfyUI-Trellis2-GGUF"
BUILD_DIR="/tmp/trellis2-rocm-build"   # path ends up in pip freeze ("cumesh @ file://..."); keep it stable
PY="${PYTHON:-python3}"
PIP=("$PY" -m pip install --no-cache-dir --no-warn-script-location --timeout=1000 --retries 20)

AERO_COMMIT=b3b9e951874c5fb5c936566c93b908299be08b2c       # Aero-Ex/ComfyUI-Trellis2-GGUF
CUMESH_COMMIT=d10e54c30ddd03d11472c1431693f985501c7966     # visualbruno/CuMesh
FLEXGEMM_COMMIT=6dd94a859c26ee8246888502eada3dd8ad85532e   # JeffreyXiang/FlexGEMM
TRELLIS2_COMMIT=75fbf0183001ed9876c8dbb35de6b68552ee08bd   # microsoft/TRELLIS.2 (only o-voxel is built)
UTILS3D_COMMIT=83bfa5bda008803f6521ce9547f94023b8afeddd    # EasternJournalist/utils3d

export ROCM_HOME="${ROCM_HOME:-/opt/rocm}"
export HIP_HOME="$ROCM_HOME" CUDA_HOME="$ROCM_HOME"
export FORCE_CUDA=1 PYTORCH_ROCM_ARCH=gfx1101 HCC_AMDGPU_TARGET=gfx1101 AMDGPU_TARGETS=gfx1101
export GIT_LFS_SKIP_SMUDGE=1

log() { printf '\n== %s\n' "$*"; }

# clone_at URL DIR COMMIT: full clone, then detach at the pinned commit.
clone_at() {
    rm -rf "$2"
    git clone "$1" "$2"
    git -C "$2" checkout -q "$3"
}

# The extensions include torch 2.14 headers, which need C++20; their setup.py files ask for C++17.
force_cxx20() { find "$1" -name setup.py -exec sed -i 's/c++17/c++20/g' {} +; }

log "python / torch"
"$PY" --version
"$PY" -c 'import torch; assert torch.version.hip, "torch has no HIP support"; print("torch", torch.__version__, "hip", torch.version.hip)'
echo "arch=${PYTORCH_ROCM_ARCH} ROCM_HOME=${ROCM_HOME}"

SITE="$("$PY" -c 'import site; print(site.getsitepackages()[0])')"
# leftovers of interrupted pip runs, and any previous build of the three extensions
find "$SITE" -maxdepth 1 -type d -name '~*' -exec rm -rf {} + 2>/dev/null || true
rm -rf "$SITE"/o_voxel "$SITE"/o_voxel-0.0.1.dist-info \
       "$SITE"/cumesh "$SITE"/cumesh-0.0.1.dist-info "$SITE"/cumesh-1.0.dist-info \
       "$SITE"/flex_gemm "$SITE"/flex_gemm-0.0.1.dist-info

# ---------------------------------------------------------------------------------------------------------------
log "node pack Aero-Ex/ComfyUI-Trellis2-GGUF @${AERO_COMMIT:0:8}"
clone_at https://github.com/Aero-Ex/ComfyUI-Trellis2-GGUF "$NODE_DIR" "$AERO_COMMIT"
REQS="${NODE_DIR}/requirements.txt"
sed -i "s|utils3d.git$|utils3d.git@${UTILS3D_COMMIT}|" "$REQS"
sed -i '/^meshlib$/d' "$REQS"                       # MeshLib: non-commercial licence, stubbed instead
if grep -qiE '^meshlib' "$REQS"; then echo "meshlib still in $REQS" >&2; exit 1; fi
# One package at a time and without dependencies, so the base image's torch stack is left alone and one
# unavailable package does not stop the rest. The Dockerfile adds the runtime deps that are actually needed.
while IFS= read -r line || [ -n "$line" ]; do
    pkg="$(echo "$line" | xargs)"
    if [ -z "$pkg" ] || [ "${pkg:0:1}" = "#" ]; then continue; fi
    "${PIP[@]}" --no-deps "$pkg" || echo "WARNING: could not install '$pkg', continuing" >&2
done < "$REQS"
"${PIP[@]}" --no-deps --upgrade huggingface_hub

"${PIP[@]}" setuptools wheel ninja
mkdir -p "$BUILD_DIR"

# ---------------------------------------------------------------------------------------------------------------
log "CuMesh @${CUMESH_COMMIT:0:8}"
CUMESH="${BUILD_DIR}/CuMesh"
clone_at https://github.com/visualbruno/CuMesh.git "$CUMESH" "$CUMESH_COMMIT"
git -C "$CUMESH" submodule update --init --recursive

# HIP port, part 1: rocprim's radix-sort decomposer only accepts rocprim::tuple, not cuda::std::tuple.
# Rename first, then add the macro definition (so the definition's own cuda::std::tuple is not renamed).
# rocprim::tuple has explicit constructors, so the braced return in the decomposer is spelled out.
f="${CUMESH}/src/clean_up.cu"
if [ -f "$f" ]; then
    sed -i 's/::cuda::std::tuple/CUMESH_TUPLE/g' "$f"
    sed -i '/#include <cub\/cub.cuh>/a \
#ifdef __HIP_PLATFORM_AMD__\
#include <rocprim\/types\/tuple.hpp>\
#define CUMESH_TUPLE rocprim::tuple\
#else\
#define CUMESH_TUPLE ::cuda::std::tuple\
#endif' "$f"
    sed -i 's/return {key\.x, key\.y, key\.z};/return CUMESH_TUPLE<int\&, int\&, int\&>(key.x, key.y, key.z);/' "$f"
fi
# HIP port, part 2: hipcub's segmented reduce builds its identity value on the host, so Vec3f() must be
# callable from host code too.
f="${CUMESH}/src/dtypes.cuh"
if [ -f "$f" ]; then
    sed -i 's/__device__ __forceinline__ Vec3f();/__host__ __device__ __forceinline__ Vec3f();/' "$f"
    sed -i 's/^__device__ __forceinline__ Vec3f::Vec3f() {/__host__ __device__ __forceinline__ Vec3f::Vec3f() {/' "$f"
fi
# HIP port, part 3: hipcc does not know these nvcc-only flags.
f="${CUMESH}/setup.py"
if [ -f "$f" ]; then
    for flag in --extended-lambda --expt-relaxed-constexpr -U__CUDA_NO_HALF_OPERATORS__ \
                -U__CUDA_NO_HALF_CONVERSIONS__ -U__CUDA_NO_HALF2_OPERATORS__; do
        sed -i "/\"${flag}\",/d" "$f"
    done
fi
# The vendored cubvh expects Eigen under its third_party/ but has no submodule for it: use Ubuntu's Eigen 3.4.0.
cubvh="${CUMESH}/third_party/cubvh"
if [ -d "$cubvh" ] && [ ! -f "${cubvh}/third_party/eigen/Eigen/Dense" ]; then
    mkdir -p "${cubvh}/third_party"
    rm -rf "${cubvh}/third_party/eigen"
    cp -r /usr/include/eigen3 "${cubvh}/third_party/eigen"
fi
force_cxx20 "$CUMESH"
"${PIP[@]}" "$CUMESH" --no-build-isolation
# The packaged remeshing.py is replaced by the repo's copy at the same commit (as in the reference recipe).
if [ -f "${SITE}/cumesh/remeshing.py" ]; then cp "${SITE}/cumesh/remeshing.py" "${SITE}/cumesh/remeshing.py.bak"; fi
curl -fsSL -o "${SITE}/cumesh/remeshing.py" \
    "https://raw.githubusercontent.com/visualbruno/CuMesh/${CUMESH_COMMIT}/cumesh/remeshing.py"

# ---------------------------------------------------------------------------------------------------------------
log "FlexGEMM @${FLEXGEMM_COMMIT:0:8}"
FLEXGEMM="${BUILD_DIR}/FlexGEMM"
clone_at https://github.com/JeffreyXiang/FlexGEMM.git "$FLEXGEMM" "$FLEXGEMM_COMMIT"
git -C "$FLEXGEMM" submodule update --init --recursive
force_cxx20 "$FLEXGEMM"
"${PIP[@]}" "$FLEXGEMM" --no-build-isolation
# TF32 does not exist on AMD; Triton on ROCm rejects it. Keep it on for CUDA builds only.
f="${SITE}/flex_gemm/kernels/triton/spconv/config.py"
if [ -f "$f" ]; then
    sed -i '1s/^/import torch\n/' "$f"
    sed -i 's/^allow_tf32 = True$/# TF32: NVIDIA only; ROCm Triton supports ieee\/bf16x3\/bf16x6.\nallow_tf32 = not getattr(torch.version, "hip", None)/' "$f"
    echo "FlexGEMM: TF32 off on HIP"
fi
rm -rf ~/.triton/cache

# ---------------------------------------------------------------------------------------------------------------
log "o-voxel from TRELLIS.2 @${TRELLIS2_COMMIT:0:8}"
TRELLIS2="${BUILD_DIR}/TRELLIS.2"
OVOXEL="${TRELLIS2}/o-voxel"
clone_at https://github.com/microsoft/TRELLIS.2.git "$TRELLIS2" "$TRELLIS2_COMMIT"
# Eigen is a submodule hosted on gitlab.com; when that is unreachable, fall back to Ubuntu's Eigen 3.4.0.
git -C "$TRELLIS2" submodule update --init --recursive || cp -rT /usr/include/eigen3 "${OVOXEL}/third_party/eigen"
if [ ! -f "${OVOXEL}/third_party/eigen/Eigen/Dense" ]; then
    git -C "$TRELLIS2" submodule update --init --recursive
fi
if [ -d "$OVOXEL" ]; then
    # o-voxel declares CuMesh as a git dependency; the HIP build above already provides it.
    sed -i '/cumesh.*git+/d' "${OVOXEL}/pyproject.toml"
    force_cxx20 "$OVOXEL"
    "${PIP[@]}" "$OVOXEL" --no-build-isolation
    # The node pack ships a newer flexible_dual_grid.py (adds tiled_flexible_dual_grid_to_mesh).
    if [ -f "${NODE_DIR}/patch/flexible_dual_grid.py" ] && [ -d "${SITE}/o_voxel/convert" ]; then
        cp "${NODE_DIR}/patch/flexible_dual_grid.py" "${SITE}/o_voxel/convert/flexible_dual_grid.py"
        echo "o_voxel: flexible_dual_grid.py replaced by the node pack's version"
    fi
else
    echo "WARNING: ${OVOXEL} not found" >&2
fi

# ---------------------------------------------------------------------------------------------------------------
log "remaining packages"
"${PIP[@]}" --no-deps --upgrade pooch
"$PY" -c 'import numpy; print("numpy", numpy.__version__)'

rm -rf "$BUILD_DIR"
log "TRELLIS.2 install done"
