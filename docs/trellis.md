# TRELLIS.2 on the RX 7800 XT (image → vertex-coloured .glb)

**Status 2026-10-12: works, licence-clean.** One concept image becomes a vertex-coloured `.glb` (about 48k triangles) in about 63 s warm (153 s cold in the measurement run, which still downloaded BiRefNet on first use; it is now pre-fetched by `fetch-weights.py`) at the 512 pipeline, with a 4.1 GiB VRAM peak. It runs in a separate ComfyUI image (`comfyui-trellis2-rocm:0.39.2-b3b9e95-clean`, compose service `comfyui-trellis`, profile `trellis`, port `127.0.0.1:8189`). The Krita image moved to `comfyui-rocm:0.40.0` on 2026-10-13; TRELLIS stays on the `comfyui-rocm:0.39.2` base, which is kept.

The image contains **no non-commercial code**: nvdiffrast, nvdiffrec and MeshLib are neither built nor installed ([licenses.md](licenses.md) P0-a/P0-b). So there is no UV texture bake and no MeshLib hole fill. Colour is sampled per vertex from the voxel attribute grid, and the PS1 texture is baked later in Blender (P0-c). The first version (2026-10-11, image `…-b3b9e95` without `-clean`) used nvdiffrast for a 2048² PBR bake. Its outputs `prop-terminal_0000{1,2}_` are prototype-only. Its numbers are kept below for comparison.

## What was tried

| Option | Result |
|---|---|
| `toastmanAu/trellis-2-rocm-comfyui` @`2c93cb94` (MIT, 2026-09-15, 7★) | **Used as the recipe.** It is an install guide plus 6 patches, not code. It wraps egore's install script @`0afa00c9` and Aero-Ex's node pack @`b3b9e951`, and was tested on gfx1100 with ROCm 7.2.2. Its gfx1101 support is a one-line arch change. Patch 06 is applied; 01–04 are not needed in Docker; 05 only affects BF16. |
| `steelxt/comfyui-trellis2-gguf-rocm` @`aa6614a` (no licence file, 2026-09-27, 0★) | **Not used directly.** It is a fork of egore with a Dockerfile, but it targets gfx1200/RDNA4 (`HSA_OVERRIDE_GFX_VERSION=12.0.0`), uses `rocm/pytorch:latest`, unpinned clones, builds flash-attention from source with nightly Triton, sets `expandable_segments:True` (known broken on this box) and patches at runtime from the entrypoint. Its useful lessons (GL rasterizer segfaults, headless EGL) match what we hit. |
| Upstream `microsoft/TRELLIS.2` | Not needed. Only its `o-voxel` extension is built (@`75fbf018`). |

### How the CUDA-only parts are replaced on ROCm

| CUDA dependency | ROCm replacement in this image |
|---|---|
| flash-attn / xformers | PyTorch SDPA (`backend: sdpa`, `ATTN_BACKEND=sdpa`) using aotriton kernels |
| spconv | FlexGEMM (Triton sparse conv; TF32 disabled on HIP), built from source |
| CuMesh (cub, `cuda::std::tuple`) | hipified build; `rocprim::tuple`, hipcub |
| nvdiffrast, nvdiffrec renderutils | **not installed** (non-commercial licence). `licence-stubs/` provides import-time stubs that raise `LicenceRemovedError` on use. The first image had a hipified EGL GL-rasterizer build |
| MeshLib (hole fill, simplify) | **not installed** (non-commercial licence); stubbed the same way. Simplification uses CuMesh |
| o-voxel / flexible dual grid | built with hipcc from TRELLIS.2 |
| kaolin | not used by this node pack |

All extensions are compiled for **gfx1101** (`PYTORCH_ROCM_ARCH=gfx1101`), so no `HSA_OVERRIDE_GFX_VERSION` is needed.

## Build (what the Dockerfile does)

`trellis/Dockerfile` builds on `comfyui-rocm:0.39.2` (torch 2.14.1+rocm7.2, HIP 7.2.53211, ComfyUI v0.39.2):

1. Installs the ROCm 7.2 HIP toolchain from repo.radeon.com: hipcc, hip-dev, rocm-llvm, rocprim/hipcub/rocthrust. Only the **headers** of hipblas/hipblaslt/hipsolver/hipsparse/rocblas are taken, via `dpkg -x`, because the libraries already ship in the torch wheel. It also installs Mesa GL/EGL (MIT; kept for layer caching, and open3d/cv2 load libGL), Eigen 3.4 and sparsehash.
2. Runs `trellis/install.sh`, our own install script (written for P1-b; it replaces egore's unlicensed `install-trellis2-gguf-rocm.sh`, which used to be downloaded at build time and rewritten by `pin-install.sh`). It does the same steps:
   - clones Aero-Ex's node pack and installs its `requirements.txt` one package at a time with `--no-deps`, with utils3d pinned and `meshlib` removed
   - builds CuMesh (hipified: `rocprim::tuple`, host-callable `Vec3f()`, nvcc-only flags removed, Ubuntu Eigen for cubvh), FlexGEMM (TF32 off on HIP) and o-voxel (node pack's `flexible_dual_grid.py` copied in) with hipcc for **gfx1101**
   - bumps `-std=c++17` to `-std=c++20` in their `setup.py` files (torch 2.14 headers require C++20)
   - pins every clone (table below), downloads no weights, and never clones, builds or installs nvdiffrast, nvdiffrec or MeshLib
   The node pack's vendored nvdiffrast wheels and its `install.py` are deleted too.
3. Fixes the Python side:
   - Aero-Ex pins `transformers==5.2.0`, which conflicts with the base image's huggingface_hub 2.2 and tokenizers 0.23.3, so the image goes back to the base's **5.19.0**.
   - Adds the rembg runtime deps onnxruntime 1.31.0 and pymatting 1.1.16.
   - Adds `libusb-1.0-0` for open3d.
4. Patches the preprocess node: rembg 2.0.85's default `remove()` model is **bria-rmbg (RMBG-2.0, CC BY-NC 4.0)**. The node now uses `new_session($REMBG_MODEL)`, default **`birefnet-general`** (BiRefNet, MIT code and weights, ~973 MB ONNX), pre-fetched by `fetch-weights.py` (pinned sha256) into the `/cache` mount rather than downloaded at run time. `isnet-general-use` is not used: DIS licenses its code, not its weights.
5. Puts `licence-stubs/` on `sys.path` (a `.pth` file), because `nodes.py` and o_voxel import nvdiffrast and MeshLib at module load. Then it runs `licence-check.py`, which fails the build if any NC package or file is in the image or a stub doesn't raise. To re-check a built image: `docker run --rm --entrypoint python comfyui-trellis2-rocm:0.39.2-b3b9e95-clean /opt/licence-check.py`.
6. Launches plain `main.py`. The old `trellis_main.py` only existed to create an nvdiffrast GL context early (toast issue #5).

The base is pinned separately (`ARG COMFY_BASE` in `trellis/Dockerfile`), so a ComfyUI update does not rebuild TRELLIS; bump `COMFY_BASE` only when deliberately updating TRELLIS, and keep that `comfyui-rocm` tag until then.

Build time is about 8 min (the CuMesh compile takes about 2 min). The clean image is 25.3 GB (the first image was 25.7 GB). `pip freeze` is saved in the image at `/opt/trellis-pip-freeze.txt`.

### Pins

| Component | Pin |
|---|---|
| egore/comfyui-trellis2-gguf-rocm (install script) | `0afa00c9`: reference only, not fetched (`trellis/install.sh` replaces it) |
| toastmanAu/trellis-2-rocm-comfyui (patch 06) | `2c93cb946a08` |
| Aero-Ex/ComfyUI-Trellis2-GGUF | `b3b9e951874c5fb5c936566c93b908299be08b2c` |
| visualbruno/CuMesh | `d10e54c30ddd03d11472c1431693f985501c7966` (+ cubvh with Ubuntu Eigen 3.4.0) |
| JeffreyXiang/FlexGEMM | `6dd94a859c26ee8246888502eada3dd8ad85532e` |
| microsoft/TRELLIS.2 (o-voxel) | `75fbf0183001ed9876c8dbb35de6b68552ee08bd`. GitLab was overloaded during the build, so the Eigen submodule fell back to Ubuntu Eigen 3.4.0 (the fallback is in `install.sh`) |
| EasternJournalist/utils3d | `83bfa5bda008803f6521ce9547f94023b8afeddd` |
| HF Aero-Ex/Trellis2-GGUF | `edf81aef93af0823ac8721da3c9cd3c52d539f87` |
| HF Aero-Ex/Dinov3 | `33aa61361f167fadf08fa88f312717ba5f482c42` |
| ROCm apt | `7.2` (7.2.0, matches torch's HIP) |

The remaining Python packages from Aero-Ex's `requirements.txt` (gguf, pymeshlab, open3d, sdnq, …) are resolved at build time. Their versions are recorded in the freeze file.

## Run

```bash
cd ~/devel/mars-ollama
bin/gpu-mode llm --stop                       # stop the Krita ComfyUI, free VRAM (one GPU user at a time)
docker compose build comfyui-trellis          # once (~8 min)
# weights, once (~11 GB at Q8_0; Q4_K_M ~6 GB) at pinned revisions into ~/llm/trellis/models/Trellis2, plus BiRefNet (sha256-pinned) into ~/llm/trellis/cache
docker run --rm --user 1000:1000 -e HF_HOME=/models/.hf -v ~/llm/trellis/models:/models -v ~/llm/trellis/cache:/cache \
  -v $PWD/trellis:/w:ro --entrypoint python comfyui-rocm:0.39.2 /w/fetch-weights.py Q8_0
cp concept.png ~/llm/trellis/in/
docker compose --profile trellis up -d comfyui-trellis      # UI/API on http://localhost:8189
trellis/run_workflow.py --image concept.png [--seed N] [--prefix trellis2/NAME]   # host python3, stdlib only; prints stage timings; --prefix names the .glb
trellis/sample-peaks.sh comfyui-trellis > peaks.log &       # optional: VRAM / RAM / NVMe-swap peaks
docker compose --profile trellis stop comfyui-trellis
# optional stats + 4-view unlit preview PNG next to the .glb (CPU only, no GPU needed, can run any time):
docker run --rm --user 1000:1000 -v ~/llm/trellis/out:/out -v $PWD/trellis:/w:ro --entrypoint python \
  comfyui-trellis2-rocm:0.39.2-b3b9e95-clean /w/inspect_glb.py /out/trellis2/<name>.glb
```

The workflow is `trellis/workflows/trellis2_512_api.json` (ComfyUI API format):

```
LoadImageWithTransparency → PreProcessImage (rembg birefnet) → MeshWithVoxelGenerator → PostProcessAndUnWrapAndRasterizer (bake_on_vertices) → ExportMesh
```

- Generator settings: Q8_0, `sdpa`, `low_vram`, `pipeline_type 512`, 12/12/12 steps, seed 1234.
- Post-processing settings: remesh at dual-contouring 512, CuMesh simplify to `target_face_num` 50k, `fill_holes: false`, `bake_on_vertices: true` (the node skips UV unwrap and texture bake; `texture_size` is ignored).
- Don't switch on `fill_holes`, `simplify_method: Meshlib` or `bake_on_vertices: false`: they call the stubs and the prompt fails with `LicenceRemovedError`, by design. Fill holes in Blender instead (`Select Non-Manifold` → `Fill`).

In the UI, the node pack's example workflows also load; re-pick `backend: sdpa` in the loader.

Volumes:
- `~/llm/trellis/models/Trellis2` (weights)
- `in/` and `out/` (ComfyUI input/output; the .glb lands in `out/trellis2/`)
- `user/`
- `cache/` (rembg models in `u2net/`, Triton JIT cache)

## Measured, clean image (2026-10-12, same concept `prop-terminal_05_1234.png`)

| | cold (1st run after start, seed 1234) | warm (seed 42) |
|---|---|---|
| load + rembg + generate | 141.7 s (measured when BiRefNet still downloaded on first use, ~973 MB) | 54.1 s |
| remesh + simplify + vertex colours | 10.9 s | 8.9 s |
| **total** | **152.7 s** | **63.0 s** |

- **Peak VRAM** (absolute): 4.15 GiB cold, 4.04 GiB warm. **Container RAM peak** 11.5 GB. Host available RAM never below 13.0 GiB; **NVMe swap 0 B**.
- **Result**: `prop-terminal_00003_.glb` (seed 1234): 24,843 vertices / 48,027 triangles, about 1 MB, `COLOR_0` RGBA per vertex, no UVs or textures, not watertight. Seed 42 (`_00005_`): 26,267 / 47,974.
- **Quality** (`_00003_.preview.png` next to the old `_00001_.preview.png`, same seed): same geometry and palette. The CRT, green screen, keyboard, vents and disk slot all read clearly. The warning stickers come out as blotchy colour patches, because colour resolution is now one colour per vertex. At the 128/256 px PS1 texture that Blender bakes from this mesh, the difference doesn't show.
- No `LicenceRemovedError` in the container log, so no stub was reached.
- **Smoke test after the rebuild from our own `trellis/install.sh`** (2026-10-12, seed 7, container force-recreated, first run after start): load + rembg + generate 117.2 s, remesh + UV + bake 10.1 s, total 127.3 s (workflow run 129 s wall). `prop-terminal_00006_.glb`: 27,175 vertices / 47,197 triangles, `vertex_colors=yes`, no UVs, not watertight. 0 `LicenceRemovedError` or `Traceback` lines in the container log. (Slower than the 63 s warm run above; this was the first run in a freshly recreated container, so the cause was not isolated.)

## Measured, first image with nvdiffrast bake (2026-10-11, prop-terminal concept `krea-sweep/s6-1024/05_1234.png`)

| | cold (1st run after start) | warm (2nd run, seed 42) |
|---|---|---|
| load + rembg + generate (sparse structure → shape 512 → texture 512 → decode) | 112.9 s | 52.0 s |
| remesh + UV unwrap + texture bake | 19.1 s | 14.1 s |
| export glb | 1.3 s | 1.3 s |
| **total** | **133.3 s** | **67.4 s** |

- **Cold sub-stages** (from `docker logs -t`):
  - rembg plus first-use imports: ~30 s, including the one-off 179 MB isnet download
  - sparse structure with DINOv3 cond: ~17 s
  - shape 512: ~18 s
  - texture 512: ~38 s
  - decoders: ~6 s
- **Warm sub-stages**: shape ~18 s, texture ~12 s. The cold texture stage includes the first-time Triton JIT of the FlexGEMM kernels, which is cached in `cache/triton`.
- **Peak VRAM** (absolute `mem_info_vram_used`, ~0.65 GiB idle baseline): 4.27 GiB cold, 4.66 GiB warm. Lots of headroom, so 1024 or `1024_cascade` should fit.
- **Container RAM peak** (cgroup `memory.peak`, includes page cache of the mmapped GGUFs): 9.9 GB cold, 13.5 GB after both runs.
- **Host**: available RAM never went below 15.8 GiB, and **NVMe swap stayed at 0 B**.
- **Raw generator mesh**: 1.48 M vertices / 3.02 M faces. After the remesh it was 2.29 M faces, then simplified to the target.
- **Result** (`~/llm/trellis/out/trellis2/prop-terminal_00001_.glb`, 10.2 MB):
  - 124,421 vertices, 187,705 triangles, one geometry, not watertight (140 holes reported before fill)
  - PBR material: 2048² baseColor (RGBA) + 2048² metallicRoughness, Xatlas UVs
  - Seed 42 (`_00002_.glb`): 124,879 vertices / 195,768 triangles
- **Quality** (`prop-terminal_00001_.preview.png`, 4 views, unlit): matches the concept closely. CRT bezel and green screen, keyboard, front vents, disk slot and the warning stickers are baked in. The model invented the back and sides plausibly (deep CRT housing, rear vents).

## Licences

| Item | Licence | Note |
|---|---|---|
| TRELLIS.2 code + `microsoft/TRELLIS.2-4B` weights | MIT | |
| `Aero-Ex/Trellis2-GGUF` (quantised TRELLIS.2 weights) | none declared on HF | derived from MIT weights; treat as MIT, but the repo states nothing |
| DINOv3 ViT-L (`Aero-Ex/Dinov3`, an ungated re-upload of `facebook/dinov3-vitl16-pretrain-lvd1689m`) | **Meta DINOv3 License** (custom, gated by Meta on HF) | not OSI. Read Meta's terms before commercial use [verify]. TRELLIS.2 needs it as the image encoder |
| Aero-Ex node pack, CuMesh, FlexGEMM | MIT | |
| egore install script | **no licence file** | no longer used: replaced by our own `trellis/install.sh` (P1-b) |
| steelxt fork | no licence file | not used |
| toastmanAu guide/patches | MIT | |
| nvdiffrast (v0.3.5 + v0.4.0), nvdiffrec renderutils | NVIDIA Source Code License: **non-commercial only** | **removed** from the clean image (stubs only); vertex colours + Blender bake instead |
| MeshLib | non-commercial & education licence | **removed** (stubs only); CuMesh simplify, Blender hole fill |
| rembg `birefnet-general` (BiRefNet) | MIT (code + weights) | default here |
| rembg `isnet-general-use` | Apache-2.0 code; weights not explicitly licensed | not used (still in `cache/u2net` from the first image; harmless) |
| **BRIA RMBG-2.0** (`pipeline.json` default; rembg ≥2.0.6x `remove()` default) | **CC BY-NC 4.0** | **avoided**: patched out; a copy auto-downloaded during the first test was deleted |

No region restrictions were found. Hunyuan3D is not used (its licence excludes the EU).

## Limitations

- **open3d imports fail**: its pybind needs dash/flask/…, which were not installed under `--no-deps`. Only "Laplacian Smoothing (Open3d)" and some PostProcess Mesh options use it.
- The node pack logs `[SPARSE] Attention backend: flash_attn` and two harmless "monkeypatch" warnings. Dense attention uses SDPA and the run completes; whether sparse attention silently falls back was not investigated.
- No UV texture bake in the node (licence). If higher-resolution textures are ever needed outside PS1, see [licenses.md](licenses.md) P0-d (pure-PyTorch UV rasterizer).
- Other nodes in the pack that use nvdiffrast or MeshLib (PBR preview renderer, MeshLib simplify/fill-holes nodes, texture re-bake) fail with `LicenceRemovedError`.
- Only the 512 pipeline with Q8_0 was tested. `1024`, `1024_cascade`, MeshRefiner and multiview are untested.
- `run_workflow.py` reports three stages because ComfyUI only executes output nodes. Use `docker logs -t` for sub-stages.
- `gpu-mode` does not know about the `trellis` profile; stop the service by hand.

## Next steps (PS1 pipeline)

- Feed the `.glb` to `~/unity_projects/psx-iso/Tools/blender/ps1_prep.py` to fill holes, decimate (300–3k tris), Smart UV and bake the vertex colours (selected-to-active) at 128/256 px ([licenses.md](licenses.md) P0-c).
- Try the cryopod (transparent glass) and crate concepts, `1024_cascade` for detail, and Q4_K_M to halve the weight disk use.
- Add a `trellis` mode to `bin/gpu-mode`.
- Fallbacks, if this stack breaks after an upgrade: Pixal3D (MIT, low-VRAM; the same Aero-Ex node pack supports it via `modelname: Pixal3D-GGUF`), or TRELLIS.2 on a rented CUDA GPU / the HF Space.
