# TRELLIS.2 on the RX 7800 XT (image → textured .glb)

**Status 2026-10-11: works.** One concept image becomes a textured, UV-mapped `.glb` in about 67 s warm (133 s cold) at the 512 pipeline, with a 4.7 GiB VRAM peak. It runs in a separate ComfyUI image (`comfyui-trellis2-rocm:0.39.0-b3b9e95`, compose service `comfyui-trellis`, profile `trellis`, port `127.0.0.1:8189`). The Krita image `comfyui-rocm:0.39.0` is unchanged.

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
| nvdiffrast CUDA rasterizer (PTX) | removed. The OpenGL rasterizer from v0.3.5 is rebuilt as an EGL surfaceless plugin with CPU-bounce copies (no HIP-GL interop on Mesa). `interpolate`/`texture`/`antialias` come from v0.4.0, hipified |
| nvdiffrec renderutils | hipified (`-lcuda -lnvrtc` dropped) |
| o-voxel / flexible dual grid | built with hipcc from TRELLIS.2 |
| kaolin | not used by this node pack |

All extensions are compiled for **gfx1101** (`PYTORCH_ROCM_ARCH=gfx1101`), so no `HSA_OVERRIDE_GFX_VERSION` is needed.

## Build (what the Dockerfile does)

`trellis/Dockerfile` builds on `comfyui-rocm:0.39.0` (torch 2.14.1+rocm7.2, HIP 7.2.53211, ComfyUI v0.39.0):

1. Installs the ROCm 7.2 HIP toolchain from repo.radeon.com: hipcc, hip-dev, rocm-llvm, rocprim/hipcub/rocthrust. Only the **headers** of hipblas/hipblaslt/hipsolver/hipsparse/rocblas are taken, via `dpkg -x`, because the libraries already ship in the torch wheel. It also installs Mesa EGL/radeonsi, Eigen 3.4 and sparsehash.
2. Downloads egore's `install-trellis2-gguf-rocm.sh` @`0afa00c9` and rewrites it with `trellis/pin-install.sh`. Every edit must match exactly once, or the build fails. The rewrite:
   - targets gfx1101
   - skips weight downloads
   - pins every clone (table below)
   - replaces `-std=c++17` with `-std=c++20` (torch 2.14 headers require C++20)
   - turns the nvdiffrast/nvdiffrec "build failed" warnings into errors
3. Fixes the Python side:
   - Aero-Ex pins `transformers==5.2.0`, which conflicts with the base image's huggingface_hub 2.2 and tokenizers 0.23.3, so the image goes back to the base's **5.19.0**.
   - Adds `meshlib-core`, plus the rembg runtime deps onnxruntime 1.31.0 and pymatting 1.1.16.
   - Adds `libusb-1.0-0` for open3d.
4. Patches the preprocess node: rembg 2.0.85's default `remove()` model is **bria-rmbg (RMBG-2.0, CC BY-NC 4.0)**. The node now uses `new_session($REMBG_MODEL)`, default `isnet-general-use` (Apache-2.0).
5. Changes the launcher to `trellis_main.py`, which creates one nvdiffrast GL context **before** ComfyUI starts. Without it, the texture-bake step segfaults in `RasterizeGLContext()` inside the prompt worker (toast issue #5; a standalone script works). Measured here: the first run crashed exactly there, and runs with the launcher pass.

Build time is about 8 min (the CuMesh compile takes about 2 min). The image is 25.7 GB, about 6 GB on top of the shared `comfyui-rocm` layers. `pip freeze` is saved in the image at `/opt/trellis-pip-freeze.txt`.

### Pins

| Component | Pin |
|---|---|
| egore/comfyui-trellis2-gguf-rocm (install script) | `0afa00c9` |
| toastmanAu/trellis-2-rocm-comfyui (patch 06) | `2c93cb946a08` |
| Aero-Ex/ComfyUI-Trellis2-GGUF | `b3b9e951874c5fb5c936566c93b908299be08b2c` |
| visualbruno/CuMesh | `d10e54c30ddd03d11472c1431693f985501c7966` (+ cubvh with Ubuntu Eigen 3.4.0) |
| JeffreyXiang/FlexGEMM | `6dd94a859c26ee8246888502eada3dd8ad85532e` |
| microsoft/TRELLIS.2 (o-voxel) | `75fbf0183001ed9876c8dbb35de6b68552ee08bd`. GitLab was overloaded during the build, so the Eigen submodule fell back to Ubuntu Eigen 3.4.0 (the fallback is in `pin-install.sh`) |
| NVlabs/nvdiffrast | tags `v0.4.0` (`253ac4fc`) and `v0.3.5` (`20c4135f`) |
| JeffreyXiang/nvdiffrec (renderutils) | `b296927cc7fd01c2ac1087c8065c4d7248f72da4` |
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
# weights, once (~11 GB at Q8_0; Q4_K_M ~6 GB), pinned revisions, into ~/llm/trellis/models/Trellis2
docker run --rm --user 1000:1000 -e HF_HOME=/models/.hf -v ~/llm/trellis/models:/models -v $PWD/trellis:/w:ro \
  --entrypoint python comfyui-rocm:0.39.0 /w/fetch-weights.py Q8_0
cp concept.png ~/llm/trellis/in/
docker compose --profile trellis up -d comfyui-trellis      # UI/API on http://localhost:8189
trellis/run_workflow.py --image concept.png [--seed N]      # host python3, stdlib only; prints stage timings
trellis/sample-peaks.sh comfyui-trellis > peaks.log &       # optional: VRAM / RAM / NVMe-swap peaks
docker compose --profile trellis stop comfyui-trellis
# optional stats + 4-view preview PNG next to the .glb:
docker run --rm --device /dev/kfd --device /dev/dri --group-add 983 --group-add 987 --user 1000:1000 \
  -v ~/llm/trellis/out:/out -v $PWD/trellis:/w:ro --entrypoint python \
  comfyui-trellis2-rocm:0.39.0-b3b9e95 /w/inspect_glb.py /out/trellis2/<name>.glb
```

The workflow is `trellis/workflows/trellis2_512_api.json` (ComfyUI API format):

```
LoadImageWithTransparency → PreProcessImage (rembg isnet) → MeshWithVoxelGenerator → PostProcessAndUnWrapAndRasterizer → ExportMesh
```

- Generator settings: Q8_0, `sdpa`, `low_vram`, `pipeline_type 512`, 12/12/12 steps, seed 1234.
- Post-processing settings: remesh at dual-contouring 512, `target_face_num` 200k, Xatlas UV, 2048² texture.

In the UI, the node pack's example workflows also load; re-pick `backend: sdpa` in the loader.

Volumes:
- `~/llm/trellis/models/Trellis2` (weights)
- `in/` and `out/` (ComfyUI input/output; the .glb lands in `out/trellis2/`)
- `user/`
- `cache/` (isnet model, Triton JIT cache)

## Measured (2026-10-11, prop-terminal concept `krea-sweep/s6-1024/05_1234.png`)

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
| egore install script | **no licence file** | fetched at build time, not vendored |
| steelxt fork | no licence file | not used |
| toastmanAu guide/patches | MIT | |
| nvdiffrast (v0.3.5 + v0.4.0), nvdiffrec renderutils | NVIDIA Source Code License: **non-commercial only** ("research or evaluation purposes only", checked in both LICENSE.txt files) | **flag**: the texture bake depends on them. It is fine for a hobby project. Using it for a commercial release needs a licence review; alternatives are a bake path without nvdiffrast (e.g. `bake_on_vertices`, or rebaking in Blender, which `ps1_prep.py` does anyway) |
| rembg `isnet-general-use` | Apache-2.0 | default here |
| **BRIA RMBG-2.0** (`pipeline.json` default; rembg ≥2.0.6x `remove()` default) | **CC BY-NC 4.0** | **avoided**: patched out; a copy auto-downloaded during the first test was deleted |

No region restrictions were found. Hunyuan3D is not used (its licence excludes the EU).

## Limitations

- **open3d imports fail**: its pybind needs dash/flask/…, which were not installed under `--no-deps`. Only "Laplacian Smoothing (Open3d)" and some PostProcess Mesh options use it.
- The node pack logs `[SPARSE] Attention backend: flash_attn` and two harmless "monkeypatch" warnings. Dense attention uses SDPA and the run completes; whether sparse attention silently falls back was not investigated.
- The GL rasterizer copies through host memory (CPU-bounce), so baking at 4096² or with multiview texturing will be slower than on CUDA.
- Only the 512 pipeline with Q8_0 was tested. `1024`, `1024_cascade`, MeshRefiner and multiview are untested.
- `run_workflow.py` reports three stages because ComfyUI only executes output nodes. Use `docker logs -t` for sub-stages.
- `gpu-mode` does not know about the `trellis` profile; stop the service by hand.

## Next steps (PS1 pipeline)

- Feed the `.glb` to `~/unity_projects/psx-iso/Tools/blender/ps1_prep.py` to decimate (300–3k tris), re-UV and bake at 128/256 px.
- The raw mesh is ~188k tris with a 2048² texture. Lowering `target_face_num` (e.g. 20k) and `texture_size` (1024) in the workflow would make the Blender step faster without losing anything at PS1 resolution.
- Try the cryopod (transparent glass) and crate concepts, `1024_cascade` for detail, and Q4_K_M to halve the weight disk use.
- Add a `trellis` mode to `bin/gpu-mode`.
- Fallbacks, if this stack breaks after an upgrade: Pixal3D (MIT, low-VRAM; the same Aero-Ex node pack supports it via `modelname: Pixal3D-GGUF`), or TRELLIS.2 on a rented CUDA GPU / the HF Space.
