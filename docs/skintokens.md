# SkinTokens / TokenRig on gfx1101 (feasibility test)

**Result 2026-10-12: works on Linux + RX 7800 XT with PR #14, first try, no patches of our own.** About 33 s per mesh (model load excluded), whole-card VRAM peak 11.0 GiB, output is a skinned `.glb` (skeleton + 4 weights per vertex). The crew humanoid and the high-poly crawler get clean, plausible rigs; the 2.5k-triangle crawler FBX gets a usable but blurrier one. This was a feasibility test only: nothing went into the game repo, and the outputs are kept to compare with the hand rigs. Background and the earlier research: [characters-audio-research.md](characters-audio-research.md) section 1.2.

## Recipe

| File | What |
|---|---|
| [`skintokens/Dockerfile`](../skintokens/Dockerfile) | ubuntu 24.04 + uv 0.12.24, **Python 3.11** venv (bpy has no cp312 wheel before 5.1, and 5.1+ is cp313 only), PyTorch **2.14.1+rocm7.2** (same as `comfyui-rocm`), SkinTokens at **PR #14 head `b576ed8d`** (Marckfe, 2026-10-11, "Support AMD GPUs (ROCm) and CPU-only inference", on top of main `273b691d`), `requirements.txt` + `bpy==4.5.14`, a build check that flash-attn is absent. `libatomic1` is needed by the torch wheel. Image `skintokens-rocm:pr14-b576ed8`, 18.7 GB, `pip freeze` in `/opt/skintokens-pip-freeze.txt` |
| [`skintokens/fetch-weights.sh`](../skintokens/fetch-weights.sh) | Pinned download into `~/llm/skintokens`: the two checkpoints (1.6 GB) and the Qwen3-0.6B config/tokenizer (no weights; TokenRig builds the backbone from the config). Not the datasets |
| [`skintokens/run.sh`](../skintokens/run.sh) | `run.sh IN_DIR OUT_DIR [demo.py args]`: GPU pre-checks, runs upstream `demo.py` on every mesh in IN_DIR, VRAM watchdog (15 GiB), `peaks.log` + `run.log` |
| [`skintokens/render_rig.py`](../skintokens/render_rig.py) | CPU Workbench contact sheet (host Blender, `LIBGL_ALWAYS_SOFTWARE=1`): bones over x-ray mesh, and mesh coloured by dominant bone; prints rig stats |

```bash
docker build -t skintokens-rocm:pr14-b576ed8 skintokens/
skintokens/fetch-weights.sh
skintokens/run.sh IN_DIR OUT_DIR --use_transfer          # --use_transfer keeps the source scale/material
LIBGL_ALWAYS_SOFTWARE=1 blender -b --factory-startup -P skintokens/render_rig.py -- OUT_DIR/x.glb x.rig.png
```

How PR #14 replaces the CUDA parts: no flash-attn installed, so `flash_attn_func` falls back to PyTorch SDPA (`src/model/attention_fallback.py`) and the Qwen3 backbone gets `attn_implementation="sdpa"`; ROCm defaults to `--precision mixed` (fp32 weights + bf16 autocast). The log line "use flash attention 2." is printed by the `except` branch in the vendored Michelangelo/skin-VAE blocks and does not mean flash-attn is in use. No `HSA_OVERRIDE_GFX_VERSION` is needed (gfx1101 is a native target of the torch wheels). PR #10 (Yuri Pourre, `f01bf76f`, "Add support to ROCm") does the same with a different attention module; it was not built.

The mesh loader and exporter is Blender-as-a-module (`bpy_server.py`, a local HTTP server on port 59876 inside the container).

## Measured (2026-10-12)

| Input | Source tris | Generation (`tqdm`) | Container wall (incl. load + bpy server) | Whole-card VRAM peak (baseline 0.53 GiB) | Result |
|---|---|---|---|---|---|
| upstream `examples/giraffe.glb` | | 38.7 s | 58 s | 10,936 MiB | OK (sanity check; PR #14 reports 50 s on an RX 9070 XT under Windows) |
| `Assets/_Project/Art/Creatures/Crawler/crawler.fbx` (copy, low poly) | 2,525 | 35.8 s | 120 s for the 3-mesh batch | 11,024 MiB | 43 bones, 1 root, depth 10 |
| `art-src/Characters/Crew/crew_00001_.glb` (TRELLIS source) | 48,931 | ~32 s | (same batch) | (same) | 46 bones |
| `art-src/Creatures/Crawler/crawler_00001_.glb` (TRELLIS source) | 48,341 | ~33 s | (same batch) | (same) | 46 bones |
| crew again with `TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1` | 48,931 | **18 s** | 38 s | **13,382 MiB** | 34 bones, same quality |

All outputs: 4 influences per vertex (mean 3.0–3.7), every vertex of the real mesh weighted (the 42 unweighted vertices per file belong to the extra helper mesh SkinTokens' export adds; upstream README: delete the `glTF_not_exported` node). Bone names are generic (`bone_0` …). Sampling is stochastic (beam search with top-k/top-p), so reruns give different bone counts (46 vs 34 on the crew).

The AOTriton flag nearly halves the time but costs 2.4 GiB more VRAM; it is left off in `run.sh` (add `-e TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1` to its `docker run` line to try it).

## Quality (from the CPU contact sheets, not by posing)

Contact sheets (scratch, not committed): `crawler.rig.png`, `crawler_00001_.rig.png`, `crew_00001_.rig.png`, `crew_00001_aot.rig.png`.

- **Crew (humanoid, A-pose):** textbook partition: head, neck, 3–4 spine/chest segments, clavicle, upper/lower arm, hand with finger chains, thigh, shin, foot, toe. Joint boundaries sit at elbows, knees and the waist. Looks directly usable as a starting point; it is not a Unity Humanoid naming/bone layout, so it would need renaming or retargeting.
- **Crawler, high-poly source:** clear creature rig: skull, a spine chain, separate rib-cage region, tail chain, four limbs with toes. Boundaries follow the geometry (claws and toes get their own bones).
- **Crawler, low-poly FBX:** the same overall layout, but the weight regions are larger and blend across the sparse triangles; some limb segments share colours with their neighbours. Usable as a proposal, hand fixing expected. Alternatively rig the high-poly source and transfer weights to the low-poly mesh (Data Transfer), as planned for Rigify.
- No pose/deformation test was done (that is the hand-rig comparison step).

Output files (scratch, deliberately outside the game repo): `/tmp/claude-1000/-home-adlan-devel-mars-ollama/85de99f5-77dd-4aef-91be-0a425194a70d/scratchpad/slice/skintokens/out/{crawler,crawler_00001_,crew_00001_}.glb`. Scratch is session-scoped; copy them somewhere persistent before the comparison if needed.

## Licences (verified 2026-10-12)

| Component | Licence | Note |
|---|---|---|
| SkinTokens code, main `273b691d` and PR #14 head `b576ed8d` | MIT (`LICENSE`, "Copyright (c) 2025 VAST-AI-Research"; the PR does not touch it) | PR is unmerged, by a third party (Marckfe); its diff is small (8 files) and MIT by contribution |
| Vendored code inside the repo | `src/model/michelangelo/` comes from NeuralCarver/Michelangelo, which is **GPL-3.0** upstream (GitHub API); the copy has no header. `src/model/skin_vae/transformers/tripo2_transformer.py` carries an Apache-2.0 header (HunyuanDiT authors + HF, i.e. the diffusers copy, not Tencent's model licence). 3DShape2VecSet: MIT | Local tool, nothing ships, so GPL is fine under our policy ([licenses.md](licenses.md#policy)); **do not redistribute this repo/image as MIT** **[verify the Michelangelo provenance]** |
| Weights `VAST-AI/SkinTokens@79736cad` (TokenRig GRPO, FSQ-CVAE) | MIT (card `license: mit`, not gated) | sha256 `f4e4706a…` (grpo_1400), `4843f49e…` (skin_vae). They are pickled Lightning checkpoints (`torch.load(weights_only=False)`), so loading runs code: only from the pinned revision, inside the container |
| `Qwen/Qwen3-0.6B@c1899de2` (config + tokenizer only) | Apache-2.0 | |
| bpy 4.5.14 (Blender as a Python module) | GPL-3.0 | local tool; output not covered |
| Other Python deps | permissive per `pip-licenses` (no AGPL/NC) | |
| Training data | README/card: ArticulationXL 2.0 (70 %, an Objaverse subset; Objaverse objects carry per-object licences incl. CC-BY-NC/ND), VRoid Hub (20 %; per-model VRoid terms, many prohibit commercial or AI use), ModelsResource (10 %; ripped game models, rights stay with the game publishers). The card says the training data "will be released separately"; no dataset licence statement found | **[verify]**: the weights' MIT label does not clear the training data. Same caveat as UniRig. For the game: treat SkinTokens output as a proposal that is hand-checked; the rig (joint positions + weights) is an output of our own mesh, not a copy of training data |

Verdict unchanged: **OK-cond** (local tool; training-data caveat).

## Assessment

Feasible and fast enough to use as a first pass: ~35 s per mesh on our card, well within VRAM, no ROCm patching beyond PR #14. Most useful for the creature (no metarig exists), and better on the high-poly source than on the decimated FBX. The weak points are generic bone names, stochastic bone counts and the training-data provenance. Next: pose-test the crawler rig against the hand rig (`psx-iso/Tools/blender/rig_crawler.py`), try `--use_skeleton` with the hand skeleton (skin-only mode, 29 s in PR #14), and watch whether upstream merges #10 or #14.
