# Characters, animation and audio: licence and feasibility research

Researched **2026-10-12** for the vertical slice (step G6 of [ROADMAP-gamedev.md](../ROADMAP-gamedev.md)): 1 humanoid (500–2000 tris), 1 creature (≤3000 tris), an SFX set (UI blips, doors, metal footsteps, terminal hum, creature sounds) and one dark-ambient music loop. Plan text being checked: B5 and B6 in [game-asset-workflow.md](game-asset-workflow.md). House rules and registry format: [licenses.md](licenses.md).

Nothing was installed and the GPU was not used. Licence texts were read from the repo LICENSE files, HF cards/API (`license:` field) and the licence pages named below, on 2026-10-12. **[verify]** marks a point that could not be confirmed from a primary source. This is an engineering audit, not legal advice.

Hardware assumed: RX 7800 XT 16 GB (gfx1101), ROCm 7.x, Linux, Blender 5.2, Unity 6000.6 URP. EU resident, may sell: shipped assets need permissive or commercial-OK terms; NC/research = prototype only; Tencent HY-* excluded.

## Summary: what changes in the current plan

| Plan text (B5/B6) | Finding | Change |
|---|---|---|
| "SkinTokens (MIT, CUDA ≥14 GB → rent a GPU)" | Code and weights are MIT, but **two open PRs add ROCm support**, one tested on an RX 9070 XT (16 GB, ROCm 7.2.1, Windows): 50 s per rig, plus a CPU-only path (200 s). Not merged, not tested on Linux or gfx1101. The "14 GB NVIDIA" line is a README statement, not a measurement on AMD | No need to rent a GPU. Try it locally with PR #14's patch, after Rigify |
| "or a ROCm port of UniRig" | UniRig hard-depends on `spconv`, `torch_scatter`, `torch_cluster` and `flash_attn` (grep of `ur/src`); there is no ROCm report | Drop UniRig. SkinTokens is its successor and portable |
| "GVHMR" | Not only its code (ZJU, non-profit only): it needs SMPL, whose licence bans commercial use and "production of other artefacts for commercial purposes" | Remove GVHMR from the plan (already NOT OK in licenses.md; now also out of B5). Use SMPL-free tools |
| "Quaternius (CC0)" | Quaternius's licence page is now the **Quaternius Asset License v1.0 (2026-08-28)**, but pack pages and the FAQ still say CC0 | Keep a copy of the licence that came in each zip; don't publish raw packs |
| "Stable Audio 3 Small-SFX" | Stability Community Licence: free **under USD 1M annual revenue**, commercial users must **register**. The model is **CPU-class (433M)**, so ROCm is irrelevant | Keep as a gap filler (creature sounds, hum), not the base of the SFX set |
| "ACE-Step 1.5 (MIT, under 4 GB)" | Confirmed MIT for code and weights. "<4 GB" is the lowest-tier claim (DiT only, INT8, CPU offload). **The repo documents the RX 7800 XT (gfx1101) explicitly** | Confirmed. On 16 GB use the 1.7B LM tier |
| "Vital in LMMS" | Vital's EULA says **businesses and organisations must buy a licence per user** (a sole trader selling a game is arguably one) | Use Surge XT (GPL-3.0, free for any use) as default, or buy Vital Plus |
| MusicGen (alternative) | Weights are CC-BY-NC 4.0, confirmed. **YuE2 weights are also CC BY-NC 4.0**: monetisation by "individual creators" is allowed, "companies" need a licence; needs 24 GB NVIDIA | Neither is used |

## 1. Rigging

### 1.1 Rigify and automatic weights on low-poly AI meshes

| Point | Finding |
|---|---|
| Licence | Rigify and Blender are GPL, but output (rigs, `.blend`, FBX) is not covered. OK ([policy](licenses.md#policy): local GPL tools are fine) |
| Viability for a **humanoid** | Rigify's human metarig plus "With Automatic Weights" (bone heat, [Blender manual](https://docs.blender.org/manual/en/latest/animation/armatures/skinning/parenting.html), read 2026-10-12) is the standard route. Bone heat needs a clean, mostly manifold, single-shell mesh |
| Risk with TRELLIS meshes | The clean path writes **vertex colours on a ~48k-tri mesh** ([trellis.md](trellis.md)); after decimation to 500–2000 tris there can be non-manifold edges, internal faces, loose parts and fused limbs (arms against the torso), which break or smear heat weights **[verify on our own mesh]**. This is expected from how bone heat works, not a measured result here |
| Mitigation (in order) | 1. Rig the **48k mesh or a voxel-remeshed copy**, fix weights, then transfer to the final low-poly mesh with a Data Transfer modifier (vertex groups, nearest face interpolated). 2. A-pose or T-pose the concept at generation time. 3. Weight-paint the remaining 5–10 bad joints by hand; at 500–2000 tris that is minutes |
| Humanoid in Unity | Export as Humanoid-compatible bones (Rigify "deform" bones only, or a generated game rig); Unity maps the Avatar. This keeps animation libraries retargetable |
| Creature | Rigify has `basic_quadruped`/`Basic` metarigs; for an alien creature a hand-placed 10–20 bone chain plus automatic weights is realistic at ≤3000 tris. Procedural motion via Unity Animation Rigging (IK) |

### 1.2 AI auto-riggers

All dates are from the GitHub API or the HF API on 2026-10-12. "ROCm" means a success report found, not a claim by the authors.

| Tool | Code licence | Weights licence | GPU need | ROCm / AMD | Training-data caveat | Verdict |
|---|---|---|---|---|---|---|
| **SkinTokens** (VAST/Tripo, 2026-02; [GH](https://github.com/VAST-AI-Research/SkinTokens) @`273b691d`, [HF](https://huggingface.co/VAST-AI/SkinTokens) @`79736cad`, 2026-04-20) | MIT ([LICENSE](https://github.com/VAST-AI-Research/SkinTokens/blob/273b691d35989d71cd17ff2895fdc735097b92d1/LICENSE), 2025 VAST-AI-Research) | MIT (HF card `license: mit`) | README: "NVIDIA GPU with at least 14 GB". Model is a Qwen3-0.6B-architecture AR transformer plus an FSQ-CVAE, about 1.6 GB of checkpoints | Upstream hard-codes `attn_implementation="flash_attention_2"` (`src/model/tokenrig.py:109`) and imports `flash_attn`. **[PR #10](https://github.com/VAST-AI-Research/SkinTokens/pull/10)** (2026-08-28, "tested on AMD Radeon AI PRO R9700") and **[PR #14](https://github.com/VAST-AI-Research/SkinTokens/pull/14)** (2026-10-11) add an SDPA fallback. PR #14: RX 9070 XT (16 GB), PyTorch 2.9.1 + ROCm 7.2.1, Windows 11, `giraffe.glb`: **50 s full rig, 29 s skin-only**; CPU (7800X3D) 200 s; bf16 weights gave worse skin than fp32+bf16 autocast on ROCm. **Both PRs open; Linux and gfx1101 untested** | ArticulationXL 2.0 (70%), VRoid Hub (20%), ModelsResource (10%). Objaverse-derived and VRoid mixed licences **[verify]**. No SMPL (grep of the repo finds none) | **OK-cond**, the only candidate that is both permissive and portable |
| **UniRig** (Tsinghua/Tripo, SIGGRAPH 2025; [GH](https://github.com/VAST-AI-Research/UniRig) @`6793c664`, [HF](https://huggingface.co/VAST-AI/UniRig) @`36842e2b`) | MIT | MIT (card tag `license:mit`) | README: CUDA GPU ≥8 GB for generation | Imports `spconv`, `torch_scatter`, `torch_cluster` (Pointcept PTv3) and `flash_attn`. The README itself warns that flash-attn "high chance" of install failure. No ROCm issue or PR found (GitHub search 2026-10-12) | Rig-XL and VRoid; "aside from vroid, all models are selected from Objaverse"; RigNet validation data "for academic usage". Released checkpoint trained on Articulation-XL2.0 | **OK-cond but impractical on AMD**; skip |
| **Puppeteer** ([Seed3D/Puppeteer](https://github.com/Seed3D/Puppeteer), arXiv 2508.10898) | Apache-2.0 (GitHub API) | Apache-2.0 (HF card) | README environment: PyTorch 2.1.1 + CUDA 11.8 | No report | Articulation-XL **[verify]**; also does video-guided animation by differentiable optimisation (not needed) | OK-cond, older CUDA stack, no reason to prefer it over SkinTokens |
| **MagicArticulate** ([Seed3D/MagicArticulate](https://github.com/Seed3D/MagicArticulate)) | Apache-2.0 | Apache-2.0 (HF card) | CUDA | No report | Articulation-XL (Objaverse subset) **[verify]** | OK-cond, skeleton only in the older release; superseded |
| **Make-It-Animatable** ([jasongzy/…](https://github.com/jasongzy/Make-It-Animatable)) | MIT | Apache-2.0 (HF card) | CUDA, A100-class for training | No report | **Trained on Mixamo data** (README downloads `jasongzy/Mixamo`). Mixamo's FAQ says content cannot be used to train ML models (section 2.1) | **[verify], treat as NOT OK** until the authors' permission is clear |
| **RigNet** (zhan-xu) | GPL-3.0 | n/a | CUDA | No report | Research dataset | Skip (old) |
| **Mixamo auto-rigger** (Adobe, cloud) | Adobe terms, see 2.1 | n/a | none (upload) | n/a | humanoid only | **OK** for a humanoid that is only ever shipped inside the game; needs an Adobe account. Fallback if Rigify fails |
| **Blender-native** (Rigify, Voxel Heat Diffuse add-ons, Auto-Rig Pro) | GPL / paid | n/a | none | n/a | n/a | OK |

**Is there something that runs locally on 16 GB AMD?** Yes, SkinTokens, with a patch. The 14 GB figure is for NVIDIA with flash-attn; the PR #14 numbers come from a 16 GB RX 9070 XT, so the model fits. Gaps that remain are ours to test: Linux, gfx1101 (RDNA3, ROCm 7.x already works for TRELLIS in this repo), and `bpy>=4.2` as a pip module (Blender's `bpy` wheel is GPL; local tool, output not covered). Estimated effort: 2–4 h to apply the PR diff to a pinned clone, run the giraffe example, then our creature.

## 2. Animation sources

### 2.1 Libraries

| Source | Terms (primary source, read 2026-10-12) | Verdict | Notes |
|---|---|---|---|
| **Quaternius** Universal Animation Library (120+ humanoid animations, FBX/GLB/Blend) | [Pack page](https://quaternius.com/packs/universalanimationlibrary.html) shows "License CC0". The site's [licence page](https://quaternius.com/license.html) is **Quaternius Asset License v1.0, "Last updated 8/28/2026"**: personal, educational and commercial use, modification, no credit; you may **not** "extract, repackage, sublicense, sell, or otherwise redistribute the Assets … as a standalone asset, asset pack, stock file, template" (§3a); perpetual, terminates only on uncured breach of §3 (§5). The [FAQ](https://quaternius.com/faq.html) still says "All models are under the CC0 License" | **OK-cond** | The pages disagree. Save the licence text shipped inside the zip and the download date next to the pack. Don't commit raw packs to a public repo; shipping them inside a built game is covered by §2. "Source" kits sold via Patreon: not needed |
| **Kenney** | [Support FAQ](https://kenney.nl/support): game assets are CC0, "You're free to use them, even in commercial projects", attribution not required. Audio packs seen: Impact Sounds (130 files), Sci-fi Sounds (70), Interface Sounds (100), all "Creative Commons CC0" | **OK** | Kenney has **no humanoid animation library** that I could find (page `animated-characters-1` is 404); his value here is audio and props |
| **Mixamo** (Adobe; auto-rigger, characters, animations) | The Adobe-staff FAQ, "[Mixamo FAQ - Licensing, Royalties, Ownership, EULA and TOS](https://community.adobe.com/questions-696/mixamo-faq-licensing-royalties-ownership-eula-and-tos-589400)" (posted 2022-09-29): free, "no licensing or royalty fees, for unlimited commercial or non commercial use"; video games are an allowed project. **Prohibited**: "blueprints, templates, or asset packages for video game engines which redistribute character or animation raw files as the product"; "DLC or Add-on Content for Games"; asset-store packages; "any type of free distribution of … raw files". Team members may use files; "You may not distribute the files to customers or non-team members". "The only research application Mixamo content can't be used in is training machine-learning models." No credit required. Your use "is covered under the terms at the time you use it" | **OK-cond** | The Adobe help-centre version of the FAQ returns HTTP 403 to curl; the text above is the community repost by Adobe staff. The FAQ calls the service a "limited duration technology preview" and refers to Adobe's Terms of Use for details, so check the current Adobe ToU before release **[verify]**. Practical rules: (1) shipped `.fbx` inside the game build is the intended use, but a **public repo with the raw FBX is "distribution"**, so keep Mixamo files in a private repo or LFS; (2) don't train or fine-tune anything on Mixamo clips; (3) heavy edits do not obviously change this, so keep provenance tags |
| **Hand-keyed** in Blender | own work | **OK** | Needed anyway for terminal interaction, door push, creature attack, death |
| Cascadeur (Nekki, free tier) | proprietary, tier terms not read | **[verify]** | Optional, for physics-assisted keying |

PS1 toggle (stepped 12–15 fps) stays as in B5; it is a Unity-side setting, not a licence matter.

### 2.2 Video-to-mocap

| Tool | Code licence | Model / body-model licence | Verdict for shipped animation |
|---|---|---|---|
| **GVHMR** (zju3dv, @`ee960bb6`) | [LICENSE](https://github.com/zju3dv/GVHMR/blob/main/LICENSE): "for educational, research and non-profit purposes only. Any modification based on this work must be open-source and prohibited for commercial use"; commercial use by e-mail to ZJU | Needs **SMPL / SMPL-X** (below) | **NOT OK** (two independent blockers) |
| **SMPL / SMPL-X body model** ([SMPL-Model licence](https://smpl.is.tue.mpg.de/modellicense.html), read 2026-10-12) | n/a | "Software Copyright License for non-commercial scientific research purposes". Grant: "non-commercial scientific research, non-commercial education, or **non-commercial artistic projects**. Any other use, in particular any use for commercial purposes, is prohibited. This includes … incorporation in a commercial product, use in a commercial service, **or production of other artefacts for commercial purposes**." Also: no use "to train methods/algorithms/neural networks/etc. for commercial use", no redistribution. Commercial licences are sold through Meshcapade (smpl@max-planck-innovation.de) | **NOT OK** for a game you may sell |
| **FreeMoCap** (@`75c8acc2`; repo AGPL-3.0, [LICENSE](https://github.com/freemocap/freemocap/blob/main/LICENSE)) | AGPL-3.0; the authors offer other terms | Uses markerless pose models via `skellytracker` (MediaPipe / RTMPose / YOLO family); weight terms not read **[verify]**. Multi-camera calibrated capture is its design point; single-camera support **[verify]** | **OK-cond**: local tool, motion output not covered by AGPL; never ship its code |
| **MediaPipe Pose Landmarker** (Google, [Apache-2.0](https://github.com/google-ai-edge/mediapipe)) | Apache-2.0 code; docs CC-BY 4.0 / Apache samples | BlazePose GHUM model card terms **[verify]** | **OK-cond**; per-frame 3D landmarks, no SMPL, runs on CPU. Needs retargeting and cleanup |
| **BlendArMocap** (cgtinker, GPL-3.0, 2025-12) | GPL-3.0 | Uses MediaPipe | OK as a local Blender tool, Blender 5.2 support **[verify]** |
| **SAM 3D Body** (Meta, [repo](https://github.com/facebookresearch/sam-3d-body), SAM Licence "Last Updated: November 19, 2025", 2026-02) | **SAM License** (custom): royalty-free grant to use, reproduce, distribute, modify; you own your derivative works; no NC clause found; the contracting entity for the EEA is Meta Platforms Ireland; export-control and military-use limits; patent-litigation termination | Output is **MHR** parameters (Meta's Momentum Human Rig, [Apache-2.0](https://github.com/facebookresearch/MHR)), not SMPL; MHR ships SMPL conversion tools, which we would not use | **OK-cond [verify]**: single-image mesh recovery, so you must smooth per frame jitter yourself; GPU needs and ROCm unknown. Heavier than the slice needs |
| HY-Motion (Tencent) | excluded | excluded | **NOT OK** (EU exclusion, ≥24 GB) |

**Are outputs retargeted to our own rig affected by the SMPL licence?** Taken literally, yes, for anything produced *with* SMPL: the grant covers "non-commercial artistic projects" only and the prohibition names "production of other artefacts for commercial purposes". A motion curve (joint rotations) is not a copy of the SMPL mesh or its shape space, so there is an argument that retargeted rotations carry no SMPL expression, but the licence text does not say that, the licensor sells commercial licences for exactly this use, and the GVHMR code is NC regardless. Conclusion for this project: **treat any SMPL-derived motion as prototype-only**, tag it `smpl-noncommercial` in `art-src/`, and do not ship it. Ask Meshcapade or a lawyer if this ever matters **[verify]**.

## 3. SFX

### 3.1 Generators

| Option | Licence (source) | Commercial outputs | Hardware | Verdict |
|---|---|---|---|---|
| **Stable Audio 3 Small-SFX** ([HF](https://huggingface.co/stabilityai/stable-audio-3-small-sfx) @`05ca3857`, updated 2026-10-06; gated; code [Stability-AI/stable-audio-3](https://github.com/Stability-AI/stable-audio-3) MIT per GitHub API) | HF `license: other` (tag `stable-audio-community`), **Stability AI Community License** ([text read at stability.ai, "Last Updated: July 5, 2024"](https://stability.ai/community-license-agreement)). The repo's own `LICENSE.md` and `NOTICE` are behind the gate **[verify after accepting]**. The card says the Gemma Terms of Use also apply (it bundles T5Gemma, `LICENSE_GEMMA.md`) | Yes under III: "You must register with Stability AI" for a Commercial Purpose. If you "or Your Affiliate(s) … generate more than USD $1,000,000 in annual revenue … any licenses granted to You under this Agreement shall terminate", then you must request a licence (sole discretion). You own outputs "to the extent permitted by applicable law" (IV(c)(iii)). Attribution/notice on distributing the materials (not needed if only outputs ship) | The README table lists Small-SFX as **433M, "CPU", up to 120 s**; peak VRAM 1.7–2.4 GB on an H200; Medium is "GPU (CUDA)" 1.4B. **So no ROCm is needed**: run on CPU (torch or the TFLite path) or PyTorch ROCm if it works **[verify]**. No ROCm statements in the repo | **OK-cond**. Register once, keep the registration e-mail with the asset tags (`stable-audio-community`) |
| Stable Audio Open 1.0 / Small | HF `license: other`, same community licence family | as above | torch | Older; no reason to prefer over SA3 |
| **TangoFlux** | Card: "checkpoints are for **non-commercial research use only**" | **No** | | NOT OK |
| MMAudio (video-to-audio) | Code MIT, **weights CC-BY-NC 4.0** (HF API) | No | | NOT OK |
| AudioLDM 2, Tango 2 | `cc-by-nc-sa-4.0` (HF API) | No | | NOT OK |
| AudioGen / MAGNeT (Meta audiocraft) | weights `cc-by-nc-4.0` | No | | NOT OK |
| HunyuanVideo-Foley | Tencent | EU excluded | | NOT OK |

I found **no open text-to-audio model with permissive weights** that is better than SA3 for SFX. The permissive route is procedural (jsfxr, ChipTone) plus CC0 libraries plus own recordings.

### 3.2 Procedural and library sources

| Source | Terms (read 2026-10-12) | Verdict | Use for |
|---|---|---|---|
| **jsfxr** (chr15m, [Unlicense](https://github.com/chr15m/jsfxr) per GitHub API) | Tool: Unlicense. Generated sounds are your own output | **OK** | UI blips, terminal beeps. Browser, no GPU |
| **ChipTone** (SFB Games, [itch page](https://sfbgames.itch.io/chiptone)) | "All sounds created using ChipTone are FREE to use for any purpose, commercial or otherwise, under the CC0 licence" | **OK** | Same as jsfxr; richer synth |
| **Kenney audio** | CC0 (see 2.1) | **OK** | UI (Interface Sounds), impacts and doors (Impact Sounds), sci-fi hum/machines (Sci-fi Sounds) |
| **Sonniss GameAudioGDC** ([2026 bundle](https://gdc.sonniss.com/), 7.47 GB, 347 files, plus the 2015–2024 [archive](https://sonniss.com/gameaudiogdc); no 2025 bundle) | [Licence v2.0, effective 2026-08-27](https://sonniss.com/gdc-bundle-license/): use "on an unlimited number of projects", "personal and commercial projects without attribution", synchronised use in "games, films, television & interactive projects". **Restrictions**: may not sell the sounds "as they come", may not "distribute, publish, sub-license or otherwise supply the sound effects as sound effects to any other person" (modified or not); "Use for AI/ML training is strictly prohibited" (page banner) | **OK-cond** | Best source for **metal footsteps, doors, machine hum, creature design raw material**. Don't put raw files in a public repo; shipped inside the game build is the intended use. Never train or fine-tune on them. Keep the bundle year and the licence version next to each file. Sounds are tied to the licence version in force at download, so save a PDF of v2.0 **[verify that earlier archive bundles use their own versions]** |
| **Freesound** | Uploads are offered under exactly three licences: "Creative Commons 0", "Attribution", "Attribution Noncommercial" ([FAQ](https://freesound.org/help/faq/)). Filter to CC0 only (site filter or API `license:"Creative Commons 0"`) | **OK-cond** | Gap filling. Freesound does not guarantee that uploaders own what they upload, so keep the URL and download date per file; prefer well-known uploaders. CC-BY is allowed only with credits in the game; skip it for the slice |
| Own Foley | own | **OK** | Footsteps on a metal tray or baking sheet, doors, creature vocals (voice plus pitch/formant shift in Blender or Audacity) |

### 3.3 ROCm / AMD summary for audio

Small-SFX is a 433M model that the vendor targets at CPUs; do not spend GPU effort. If ROCm PyTorch is wanted, it is the same recipe as ACE-Step below **[verify]**. The GPU is used by other jobs here, so the CPU route is also the polite one.

## 4. Music

### 4.1 Models

| Model | Code | Weights | VRAM | ROCm / AMD | Verdict |
|---|---|---|---|---|---|
| **ACE-Step 1.5** (GH [`ace-step/ACE-Step-1.5`](https://github.com/ace-step/ACE-Step-1.5) @`ca1e85fe`, 2026-10-11; HF [`ACE-Step/Ace-Step1.5`](https://huggingface.co/ACE-Step/Ace-Step1.5) @`19671f40`, 2026-02-03) | **MIT** ([LICENSE](https://github.com/ace-step/ACE-Step-1.5/blob/ca1e85fe9430179831e6bc6be790c332190a3866/LICENSE), "Copyright (c) 2026 ACEStep") | **MIT** (card `license: mit`; the XL turbo repo `ACE-Step/acestep-v15-xl-turbo` also MIT). Card: "You can strictly use the generated music for **commercial purposes**"; training data "Licensed Data", "Royalty-Free / No-Copyright Data" and "Synthetic Data" (MIDI-to-audio). That is the authors' statement, not verified | Repo's tiered guide ([GPU_COMPATIBILITY.md](https://github.com/ace-step/ACE-Step-1.5/blob/ca1e85fe9430179831e6bc6be790c332190a3866/docs/en/GPU_COMPATIBILITY.md)): ≤6 GB 2B turbo DiT only (INT8, full CPU offload), "<4 GB" is the README headline; **12–16 GB tier: LM 0.6B/1.7B (1.7B recommended), INT8, CPU offload, max 8 min with LM / 10 min without**; XL (4B) DiT is "marginal" on 12–16 GB (aggressive offload) and fully supported from 20 GB | **Yes, and gfx1101 is named**: [docs/en/INSTALL.md](https://github.com/ace-step/ACE-Step-1.5/blob/ca1e85fe9430179831e6bc6be790c332190a3866/docs/en/INSTALL.md) lists "RX 7800 XT, RX 7700 XT: `HSA_OVERRIDE_GFX_VERSION=11.0.1`" and ships `start_gradio_ui_rocm.sh` / `start_api_server_rocm.sh` (sets `ACESTEP_LM_BACKEND=pt`, `MIOPEN_FIND_MODE=FAST`, separate `venv_rocm`; `torchcodec` unsupported, soundfile fallback). A community Linux (CachyOS) manual using `rocm6.4` wheels is in `docs/en/ACE-Step1.5-Rocm-Manual-Linux.md` (tested on RDNA4/Strix Halo, 2026-02-07). We have not run it on a 7800 XT | **OK**. Condition: check output for similarity to known tracks; the generated music's copyright status in the EU is uncertain for purely AI output **[verify with a lawyer before release]** |
| MusicGen (audiocraft) | MIT | **CC-BY-NC 4.0** (confirmed on HF API for `musicgen-small`, and the audiocraft README) | small | torch | **NOT OK** |
| **YuE (v1, 2025)** | Apache-2.0 | Apache-2.0 (`m-a-p/YuE-s1-7B-anneal-en-cot`, `YuE-s2-1B-general`, HF API) | 7B + 1B, large | no report | OK on paper, but vocal-song oriented and too heavy; not for ambient drones |
| **YuE2 (2026-09)** | Apache-2.0 | **CC BY-NC 4.0 "with additional creator permission"** ([MODEL_LICENSE](https://github.com/multimodal-art-projection/YuE/blob/main/MODEL_LICENSE), dated 2026-09-16): free for "personal users, content creators and musicians acting in an individual capacity" to generate and monetise outputs; "does not authorize commercial use of the model weights by companies" | README: "Linux · Python 3.12 · NVIDIA GPU with BF16 support and 24 GB VRAM" | no | **NOT OK for us** (hardware) and a risk if the game is ever sold through a company |
| Stable Audio 3 Small-Music / Medium | Same community licence as SFX (registration, USD 1M) | CPU (433M) / CUDA (1.4B) | small | CPU | Possible for short loops; no sign it beats ACE-Step for dark ambient **[verify by ear]** |

### 4.2 Traditional tools

| Tool | Licence | Commercial output? | Notes |
|---|---|---|---|
| **Surge XT** (surge-synthesizer, [GPL-3.0](https://github.com/surge-synthesizer/surge)) | GPL-3.0 | **Yes**, the audio you render is yours; don't ship the synth. VST2 builds can't be redistributed | Default for drones: wavetable + FM, free on Linux. Factory patch/wavetable licences not read **[verify]** if you ship a sampled stem built from "unaltered" factory sounds |
| **Vital** ([EULA](https://vital.audio/eula/), read 2026-10-12) | proprietary | "can be used for commercial or non-commercial music and audio productions without the prior permission". **But**: "Businesses and organizations must purchase a license for each user of the software." You may not redistribute presets, samples, wavetables … "created from any unaltered VITAL AUDIO content". "May be used to create samples for commercial products such as sample packs" | **OK-cond**: as a private individual the free tier is fine; if the game is sold through a business (sole trader, AB), buy Plus (price not checked). Linux build exists |
| **LMMS** ([GPL-2.0](https://github.com/LMMS/lmms)) | GPL-2.0 | Yes (local tool) | Use as the sequencer/arranger; bundled sample/preset licences not audited **[verify]**: render only with your own or CC0 samples |
| Own recordings, Surge/Vital drones processed in Audacity/Reaper | own | Yes | Slow evolving pads, filtered noise, sub drones are well within a synth's ability |
| jsfxr/ChipTone | see 3.2 | Yes | Stingers only |

## 5. Recommendation: the smallest commercially clean stack

Order of trying, per asset, with expected effort (solo dev, evenings). Stop at the first step that works.

### Humanoid (500–2000 tris)

| # | Step | Effort | Licence state |
|---|---|---|---|
| 1 | Rig the TRELLIS humanoid with **Rigify human metarig + automatic weights**; if weights are bad, rig a voxel-remeshed copy and Data-Transfer the weights to the low-poly mesh; hand-fix the worst joints | 2–4 h | clean |
| 2 | Animations: **Quaternius Universal Animation Library** (idle, walk, run, interact), retargeted through Unity Humanoid; hand-key 2–3 specials in Blender | 3–5 h | OK-cond (keep licence copy; private raw files) |
| 3 | Fallback for rigging and base locomotion: **Mixamo** auto-rig + animations | 1–2 h | OK-cond (private repo, no ML training) |
| 4 | Optional: SkinTokens (local, PR #14 patch) to compare skin weights | 2–4 h | OK-cond |

### Creature (≤3000 tris)

| # | Step | Effort |
|---|---|---|
| 1 | Hand-placed 10–20 bone chain in Blender, automatic weights, **hand-keyed** idle/move/attack (3 short cycles), Unity Animation Rigging for head/tail IK | 4–8 h |
| 2 | **SkinTokens** (local patch) to propose skeleton + weights for the creature, then fix by hand. This is where it helps most, because there is no creature metarig | 3–5 h incl. the ROCm patch |
| 3 | Rigify animal metarig if the creature is quadruped | 2–3 h |

Not recommended: UniRig, Make-It-Animatable, GVHMR, any SMPL tool, HY-Motion.

### SFX set

| # | Step | Effort |
|---|---|---|
| 1 | **jsfxr / ChipTone** for UI blips and terminal beeps; **Kenney** Interface/Impact/Sci-fi packs for the rest of the UI, door clunks and machine textures | 1–2 h |
| 2 | **Sonniss GDC 2026 bundle and archive** (7.47 GB, search "metal", "door", "footstep", "hum", "drone") for footsteps on metal and doors | 2–3 h |
| 3 | **Stable Audio 3 Small-SFX** on CPU for creature sounds and a terminal hum; register with Stability first | 2 h (+ 15 min registration) |
| 4 | Own Foley, pitch/formant shifted, for the creature | optional |

### Music loop

| # | Step | Effort |
|---|---|---|
| 1 | **ACE-Step 1.5** (MIT) with the repo's ROCm launcher on the 7800 XT (1.7B LM tier, INT8), prompts for "dark ambient industrial drone, no vocals, no percussion"; pick a 60–120 s result, loop with a crossfade in Audacity | 2–4 h incl. setup; GPU must be free |
| 2 | **Surge XT** (GPL, free) in LMMS or a DAW: two detuned wavetable pads, filtered noise, a slow LFO, then render a seamless loop | 2–3 h |
| 3 | Vital only after buying Plus if the game is sold via a business | n/a |

### Do before shipping anything from this list

1. Create `art-src/` provenance tags (a file tagged `smpl-noncommercial` must never ship): `quaternius-qal`, `mixamo`, `kenney-cc0`, `sonniss-gdc-v2.0`, `freesound-cc0:<url>`, `stable-audio-community`, `acestep-mit`.
2. Register with Stability for SA3 and save the gated `LICENSE.md` and `NOTICE`.
3. Update the section 5 rows in [licenses.md](licenses.md) (Quaternius, Mixamo, Sonniss, Stable Audio, Surge XT, SkinTokens, FreeMoCap) when each step is actually used. Not done here, since nothing is adopted yet.

## Results: ACE-Step 1.5 and SkinTokens on gfx1101 (2026-10-12)

First real runs on the RX 7800 XT (Linux, ROCm 7.x host, container wheels torch 2.11/2.14 + rocm7.2). Details, recipes and licence checks: [acestep.md](acestep.md), [skintokens.md](skintokens.md).

| Tool | Works? | Numbers | Notes |
|---|---|---|---|
| ACE-Step 1.5 (12–16 GB tier: turbo DiT INT8, CPU offload, LM 1.7B on the `pt` backend) | **Yes** | 80 s track: 30–38 s with LM (+24 s load), 12–16 s DiT-only (+7 s load); whole-card VRAM peak 14.7 GiB with LM, 13.8 GiB without | `HSA_OVERRIDE_GFX_VERSION=11.0.1` as upstream documents. LM-planned runs added a regular transient pulse despite "no drums"; DiT-only gave the beatless drone. Thin headroom with the LM: use `--no-lm` while Unity/Blender are open. Used for `station_ambient_loop.wav` |
| SkinTokens / TokenRig, PR #14 (`b576ed8d`) | **Yes**, no own patches | ~33 s per mesh (load excluded), VRAM peak 11.0 GiB; 18 s / 13.4 GiB with `TORCH_ROCM_AOTRITON_ENABLE_EXPERIMENTAL=1` | Crew humanoid and high-poly crawler: clean rigs (43–46 bones, 4 weights/vertex); 2.5k-tri crawler FBX: usable but blurrier weights. Feasibility only, outputs not in the game repo |

Resolved from the open list below: "SkinTokens on Linux + gfx1101" and "ACE-Step 1.5 on a 7800 XT: first real run". Still open: SkinTokens training-data licences (and the vendored GPL Michelangelo code), ACE-Step training-data claims, EU copyright of AI audio.

## Open [verify] items

- SkinTokens on **Linux + gfx1101**: apply PR #14, run `giraffe.glb`, then the crate/creature; record time and VRAM. Re-check whether upstream merges #10 or #14.
- Whether auto-weights on our decimated TRELLIS humanoid are usable (step 1 above).
- Training-data licences for SkinTokens (ArticulationXL 2.0 / Objaverse / VRoid Hub / ModelsResource).
- Stable Audio 3's gated `LICENSE.md`/`NOTICE` (the licence read is the Stability Community text dated 2024-07-05) and its Gemma terms (T5Gemma bundle).
- Quaternius: which licence each downloaded pack actually carries (CC0 vs QAL v1.0).
- Mixamo: the current Adobe Terms of Use (the FAQ is a 2022 community repost and speaks of a "technology preview").
- ACE-Step 1.5 on a 7800 XT: first real run, VRAM peak and generation time; training-data claims are the authors'.
- Vital: price of Plus and whether a sole trader counts as a "business" under the EULA.
- Surge XT factory patch/wavetable licences; LMMS bundled samples.
- MediaPipe BlazePose model card terms; FreeMoCap single-camera mode and its model weights; SAM 3D Body hardware needs.
- SMPL-derived motion: the legal reading is untested; policy is prototype-only.
- EU copyright status of purely AI-generated audio (no protection may mean others can copy the music).
