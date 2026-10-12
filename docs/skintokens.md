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
- **Crawler, high-poly source:** clear creature rig: skull, a spine chain, separate rib-cage region, four limbs with toes (no tail chain: see the comparison below, the tail is weighted to the pelvis bone). Boundaries follow the geometry (claws and toes get their own bones).
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

## Comparison with hand rigs (2026-10-13)

Blender 5.2 headless on the CPU only (`blender -b --factory-startup`, Workbench with Mesa software GL, no GPU, no Docker, no Unity; nothing went into the game repo). The same extreme poses are put on our hand rigs and on the SkinTokens results from 2026-10-12, and the deformed meshes are measured and rendered. One SkinTokens run per input (sampling is stochastic, so another run would differ, e.g. 34 instead of 46 bones on the crew). Sheets and metrics: `~/llm/skintokens/results-2026-10-12/compare/` (`*.png`, `metrics/*.json`, `scripts/rc.py` + helpers; run `rc.py --char crew|crawler --variant H|S|S48|T|TS`, then `sheet.py`). The scratch copy of the scripts is session-scoped, the `compare/scripts` copy is the persistent one.

**Variants.** `H` = hand rig (`crew.fbx` 1,433 tris / 22 bones, `crawler.fbx` 2,442 tris / 22 bones; the FBX's Idle/Crawl/Attack actions are cleared). `S` = SkinTokens on the input it was given: crew `crew_00001_.glb` (48,851 tris, 46 bones), crawler `crawler.glb` (the 2.5k-tri low-poly FBX, 2,445 tris, 43 bones). `S48` = crawler `crawler_00001_.glb` (48,261 tris, 46 bones). `T` = transfer: SkinTokens weights of the 48k mesh onto our low-poly mesh with our skeleton (nearest face, barycentric interpolation, top 4 influences, normalised; my own implementation of what Data Transfer does). `TS` = `T` plus 3 rounds of neighbour smoothing of the weights.

**How the rigs were matched.** The 48k results are scaled and shifted into the hand-rig space (crew x1.746, crawler x1.372, bbox fit). SkinTokens bones were mapped to hand bones by nearest joint head and checked: crew Hips, Spine, Chest, UpperChest, Neck, Head, Shoulder, UpperArm, LowerArm, Hand (the 12 finger bones inherit Hand), UpperLeg, LowerLeg, Foot, Toes map one-to-one on both sides, so the crew transfer is clean. Crawler: Pelvis, Spine, Chest (2 bones), Neck, Head and the 3 leg bones per leg map; toe chains inherit Foot; **the hand rig's four Tail bones have no counterpart** (SkinTokens made no tail bones in either crawler run). Mapped joint heads agree within 1-3 cm at knees, ankles, wrists and toes, but hips/shoulders are 9-25 cm off (SkinTokens roots the limbs near the spine, we put them at the flank/shoulder). Poses use the same angles about the same world axes through the mapped bone's head (the `rig_pose_test.py` set for the crew, a crawler set of my own), so the shapes are comparable, not identical; Chest on the crawler splits the angle over its 2 bones. Tail curl is therefore a no-op on the SkinTokens crawler.

### Numbers

Bleed = share of limb vertices (truth: nearest hand-skeleton segment, shoulders counted as torso) carrying more than 5 % / 25 % weight on the bones of another limb. Stretch = longest triangle edge in any of the poses over its rest length, and the share of edges stretched more than 2x in the worst pose. All variants: 0 unweighted vertices, at most 4 influences, weights sum to 1; coincident (unwelded) vertices carry equal weights, so no cracks. SkinTokens' `.glb` is unwelded (3 vertices per triangle, 146k/145k/7.3k vertices), the counts below are triangles.

| Crew | tris | bones | mean infl. | bleed >5 % / >25 % | worst stretch (pose) | edges >2x | min volume (pose) |
|---|---|---|---|---|---|---|---|
| H hand | 1,433 | 22 | 2.25 | 0 / 0 % | 2.4x (leg_side_kick) | 0.09 % | 0.85 (squat) |
| S SkinTokens 48k | 48,851 | 46 | 2.99 | 0.3 / 0 % | 7.6x (arms_up) | 0.52 % | 0.95 (squat) |
| T transfer to 1,433 | 1,433 | 22 | 2.83 | 0.3 / 0 % | 3.8x (arms_forward_bent) | 0.28 % | 0.87 (squat) |
| TS smoothed | 1,433 | 22 | 3.52 | 1.7 / 0 % | 4.0x (arms_up) | 0.61 % | 0.83 (squat) |

| Crawler | tris | bones | mean infl. | bleed >5 % / >25 % | worst stretch (pose) | edges >2x | min volume (pose) |
|---|---|---|---|---|---|---|---|
| H hand | 2,442 | 22 | 2.87 | 0.5 / 0 % | 1.6x (legs_spread) | 0 % | 0.95 (spine_bend) |
| S SkinTokens low-poly | 2,445 | 43 | 3.73 | 9.4 / 4.0 % | 26.8x (legs_spread) | 2.9 % | 0.98 |
| S48 SkinTokens 48k | 48,261 | 46 | 3.35 | 2.5 / 1.0 % | 42.2x (legs_spread) | 0.94 % | 0.97 |
| T transfer to 2,442 | 2,442 | 22 | 2.37 | 3.2 / 1.1 % | 16.7x (legs_spread) | 1.5 % | 0.87 (legs_spread) |
| TS smoothed | 2,442 | 22 | 3.33 | 4.3 / 0.2 % | 10.1x (legs_spread) | 1.2 % | 0.85 (legs_spread) |

Where the numbers come from: crew `S` stretch sits on a few chest/armpit and crotch triangles (arms_up: chest bone and right arm bone mixed 0.3/0.3 at the sternum; stride: the two UpperLeg bones mixed 0.4/0.5 at the crotch). On the 48k mesh these are tiny triangles. Crawler `S`/`S48`/`T` stretch is on the **claw tips of the front feet**: tip vertices of the left front foot carry up to 0.28 weight on the right front foot's toe bones, so the claws are pulled toward the other foot (left-front vertices with >5 % other-limb weight: 20 % in `S`, 3.5 % in `S48`, 11 % in `T`, 0 % in `H`). The tail region is 84 % (`S`) / 98 % (`S48`) weighted to the pelvis bone, ie. the tail is rigid; the hand rig has Tail1-4 (hand: 10 % of tail vertices over 5 % bleed, `S` 69 %). Transfer distance from our low-poly surface to the 48k surface: crew mean 1.6 mm, p95 4.2 mm, max 13 mm; crawler mean 6 mm, p95 15 mm, max 42 mm (the decimated crawler differs from its source more than the crew does).

### Contact sheets (hand | SkinTokens | transfer, same pose and camera, flat grey Workbench)

- `crew_rest_weights.png`: dominant-bone colours. SkinTokens gives clean bands (head, neck, chest block, arm, forearm, hand, hips, thigh, shin, boot); the hand rig's torso is blotchier (Hips/Spine/Chest/UpperChest interleave) but has no L/R mixing either.
- `crew_poses_1.png`, `crew_poses_2.png`: arms_up, arms_forward_bent, stride, squat, spine_bend_twist, forearm_twist, arms_cross, leg_side_kick. On the 48k mesh SkinTokens looks as good as or better than the hand rig everywhere: clean knees and thighs in stride/squat, no tearing. Hand rig: fine in all, but the squat crotch folds into a hole (volume 0.85). Transfer: same silhouette as the hand rig, but arms_up and arms_forward_bent open a web of stretched triangles between chest and arm.
- `crew_detail.png` (close-ups): armpit with arms up (hand chunky but closed; SkinTokens smooth; transfer webbed), squat crotch (hand: gap, SkinTokens: closed, slightly lumpy knee patches, transfer: close to hand), stride hip (SkinTokens smooth, small crease at the front of the thigh), forearm twist 90 deg (no candy-wrapper in any; SkinTokens and transfer pinch the cuff a little: area p1 0.73-0.74 vs 0.90 for the hand rig).
- `crawler_rest_weights.png`: regions are comparable between hand and SkinTokens low-poly except the tail (pelvis colour in SkinTokens). The 48k mesh is a different, more detailed shape (ribs, toes) than the decimated low-poly.
- `crawler_poses_lifts.png`: each leg lifted (front legs swing forward and fold, rear legs back). Rear lifts are clean in all four (stretch at most 1.5x). Front lifts: hand rig clean, SkinTokens low-poly and transfer grow spikes at the front claws (up to 11x), SkinTokens 48k has the same claws as thin slivers.
- `crawler_poses_body.png`: legs_spread (top view), spine_bend, tail_curl, head_turn. Spine bend and head turn are fine in all (stretch at most 1.6x, 48k head turn reaches 5x on a few edges). Tail curl: only the hand rig curls the tail; every SkinTokens variant leaves it rigid. Legs spread: SkinTokens low-poly and transfer drag long flat wedges from the front claws, 48k shows whisker-thin toes.
- `crawler_detail.png`: close-ups of lift_FL (front leg) and legs_spread (front claws).

Caveats: equal angles about different pivots (see above); edge ratios are not comparable between a 48k and a 1.4k mesh in how visible they are; extreme poses, not animation clips; one SkinTokens sample per input; no Unity check (humanoid avatar mapping, import settings).

### Verdict

- **Crew: hand rig is better for what we ship (the 1,433-tri mesh); SkinTokens is not needed here ((c)).** On its own 48k mesh SkinTokens skins at least as well as our rig (cleaner knees and crotch, slightly worse at the armpit/chest seams), but the shipped low-poly mesh with SkinTokens weights (transfer) is worse than the hand weights: armpit web, more weight on the wrong torso bones, more stretch (3.8x vs 1.3x at arms forward), and smoothing makes it worse. It is also 46 bones (24 are fingers) with generic names; a Humanoid avatar would need renaming and the bone count changes between runs. One thing to reuse: the squat crotch of the hand rig (volume 0.85) is a known weak spot and the SkinTokens weights there are better; copy by hand if it ever matters.
- **Crawler: hand rig is clearly better ((c)), SkinTokens at most a skeleton proposal ((b) for a new creature).** On the 2.5k-tri input SkinTokens has 20 % of front-leg vertices pulled by the other leg, claw tips stretched up to 27x, a rigid tail, 43 bones (more than twice ours). On the 48k source it is cleaner (2.5 % bleed) but still has the claw problem (42x) and no tail. Transfer to our mesh is better than SkinTokens-on-low-poly but far worse than hand weights (16.7x, volume 0.87). What was good: the joint positions (knees, ankles, toes within 1-3 cm of ours), which confirms the hand skeleton; and no cleanup was needed for unweighted vertices or normalisation.
- **What would make it usable.** (1) `--use_skeleton`: the README and `demo.py` support skin-only generation from a glb that already contains a skeleton (tokenises the input skeleton; the README names `examples/giraffe_skeleton.glb` but the file is not in the repo's `examples/`). **Untested by us**; it would fix the bone layout (our 22 bones, Humanoid names, Tail1-4) but not necessarily the claw bleed. Test order: crew 48k + our humanoid skeleton, then transfer; crawler 48k + our 22-bone skeleton (does the tail get weights?). (2) For a new creature without a metarig: run on the high-poly source, take the skeleton as a proposal, merge toe chains into one bone per foot and add a tail by hand, then skin with our own masked-distance weights (as `rig_crawler.py` does); do not use the SkinTokens weights on a low-poly mesh. (3) Improve the transfer (restrict the nearest-face search to the same limb, or per-limb masking, instead of plain nearest face): not tried; plain smoothing did not help.
