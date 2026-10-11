#!/usr/bin/env bash
# Rewrite egore's install-trellis2-gguf-rocm.sh (@0afa00c9) in place so every clone is pinned, the GPU target is
# gfx1101, and no weights are downloaded at build time. Each edit must match exactly once, or the build fails.
set -euo pipefail
python3 - "$1" <<'EOF'
import re, sys
p = sys.argv[1]; s = open(p).read()

AERO     = "b3b9e951874c5fb5c936566c93b908299be08b2c"  # Aero-Ex/ComfyUI-Trellis2-GGUF (toast guide's pin)
CUMESH   = "d10e54c30ddd03d11472c1431693f985501c7966"  # visualbruno/CuMesh
FLEXGEMM = "6dd94a859c26ee8246888502eada3dd8ad85532e"  # JeffreyXiang/FlexGEMM
TRELLIS2 = "75fbf0183001ed9876c8dbb35de6b68552ee08bd"  # microsoft/TRELLIS.2 (o-voxel)
NVDREC   = "b296927cc7fd01c2ac1087c8065c4d7248f72da4"  # JeffreyXiang/nvdiffrec branch renderutils
UTILS3D  = "83bfa5bda008803f6521ce9547f94023b8afeddd"  # EasternJournalist/utils3d

def sub(old, new, count=1):
    global s
    n = s.count(old)
    if n != count:
        sys.exit(f"pin-install: expected {count}x {old!r}, found {n}")
    s = s.replace(old, new)

s = s.replace("gfx1102", "gfx1101")
# Weights are volumes, fetched separately with a pinned HF revision (docs/trellis.md), not at build time.
s, n = re.subn(r"# ---- Model download \(DINOv3\) ----.*?(?=# ---- Site-packages path ----)", "", s, flags=re.S)
assert n == 1, "DINOv3 block not found"

sub('git clone https://github.com/Aero-Ex/ComfyUI-Trellis2-GGUF "$TRELLIS_GGUF"',
    f'git clone https://github.com/Aero-Ex/ComfyUI-Trellis2-GGUF "$TRELLIS_GGUF" && git -C "$TRELLIS_GGUF" checkout -q {AERO}\n'
    f'sed -i "s|utils3d.git$|utils3d.git@{UTILS3D}|" "${{TRELLIS_GGUF}}/requirements.txt"')
sub('git clone --recursive https://github.com/visualbruno/CuMesh.git "${TMPBUILD}/CuMesh"',
    f'git clone https://github.com/visualbruno/CuMesh.git "${{TMPBUILD}}/CuMesh" && git -C "${{TMPBUILD}}/CuMesh" checkout -q {CUMESH} '
    f'&& git -C "${{TMPBUILD}}/CuMesh" submodule update --init --recursive')
# cubvh's Eigen: Ubuntu's libeigen3-dev (3.4.0) instead of gitlab HEAD
sub('git clone --depth 1 https://gitlab.com/libeigen/eigen.git "${CUBVH_EIGEN}"', 'cp -r /usr/include/eigen3 "${CUBVH_EIGEN}"')
sub("https://raw.githubusercontent.com/visualbruno/CuMesh/main/cumesh/remeshing.py",
    f"https://raw.githubusercontent.com/visualbruno/CuMesh/{CUMESH}/cumesh/remeshing.py")
sub('git clone --recursive https://github.com/JeffreyXiang/FlexGEMM.git "${TMPBUILD}/FlexGEMM"',
    f'git clone https://github.com/JeffreyXiang/FlexGEMM.git "${{TMPBUILD}}/FlexGEMM" && git -C "${{TMPBUILD}}/FlexGEMM" checkout -q {FLEXGEMM} '
    f'&& git -C "${{TMPBUILD}}/FlexGEMM" submodule update --init --recursive')
sub('git clone --depth 1 --recursive https://github.com/microsoft/TRELLIS.2.git "$TRELLIS2_SRC"',
    f'git clone https://github.com/microsoft/TRELLIS.2.git "$TRELLIS2_SRC" && git -C "$TRELLIS2_SRC" checkout -q {TRELLIS2} '
    f'&& {{ git -C "$TRELLIS2_SRC" submodule update --init --recursive || cp -rT /usr/include/eigen3 "$TRELLIS2_SRC/o-voxel/third_party/eigen"; }}')
sub('git clone -b renderutils https://github.com/JeffreyXiang/nvdiffrec.git "${TMPBUILD}/nvdiffrec"',
    f'git clone -b renderutils https://github.com/JeffreyXiang/nvdiffrec.git "${{TMPBUILD}}/nvdiffrec" && git -C "${{TMPBUILD}}/nvdiffrec" checkout -q {NVDREC}')
# torch 2.14 headers require C++20; FlexGEMM, o-voxel (and others) hardcode -std=c++17. Bump before each build.
s, n = re.subn(r'^(\s*)(\$PYTHON_EXE -m pip install "(\$\{?[A-Za-z0-9_]+\}?[^"]*)" --no-build-isolation)',
               r'\1find "\3" -name setup.py -exec sed -i "s/c++17/c++20/g" {} +\n\1\2', s, flags=re.M)
assert n == 5, f"expected 5 extension builds, found {n}"
# nvdiffrast / nvdiffrec build failures are only warnings upstream; make them fatal so a broken image never builds.
s, n = re.subn(r'(--no-build-isolation \$PIPargs) \|\| \\\n\s*echo -e "\$\{warning\}WARNING: (nvdiffrast v0\.4\.0|nvdiffrec_render) build failed[^\n]*',
               r'\1', s)
assert n == 2, f"expected 2 soft-fail builds, found {n}"
# nvdiffrast is cloned by tag (v0.4.0 = 253ac4fc, v0.3.5 = 20c4135f); tags are immutable enough here.
open(p, "w").write(s)
print("pin-install: ok")
EOF
