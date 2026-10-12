# Game asset workflow: PS1 / Signalis-style isometric game

Status: **planned**, nothing implemented yet. Part B of the approved plan, researched 2026-10-10/11. Items marked **[verify]** could not be confirmed from primary sources.

Related: [README](README.md) · [models.md](models.md) · [colibri.md](colibri.md) · [ROADMAP](../ROADMAP.md) (Phase 8)

## Context

A hobby isometric 3D game in Unity with a PS1, Signalis-inspired look. The author isn't strong at modelling, rigging or animation, and prefers OSS tools and local AI.

| Goal | Meaning |
|---|---|
| Editable assets | Every asset stays manually editable |
| Runtime PS1 style | The PS1 look is a runtime parameter, so the style can change later |
| Full coverage | Characters, creatures, props, environments, textures, UI and audio |

## Licence policy

EU user, may sell later.

- Default to permissive licences: Apache, MIT, or commercial-OK under thresholds.
- Non-commercial or research models are for concept and prototyping only. Tag their outputs in `art-src/`.
- **Avoid Tencent Hunyuan3D / HY-\* models.** Their licence excludes the EU.

## Core principle: "high-ish source, PS1 at the edges"

```
art-src/ (git-lfs): concept PNG + prompt/seed/workflow.json + licence tag
 → raw AI mesh (.glb) → cleaned .blend (300–3k tris, UVs, 1024px tex, rig, actions)
 → Blender export script → FBX/GLB + derived 128/256px quantised texture
 → Unity PSX shader + post-process (one PsxSettings asset)
```

## B1. Runtimes

| Purpose | Tool | Notes |
|---|---|---|
| LLMs | llama-swap at `:11434/v1` (Part A) | Point Continue.dev or Rider's AI at it. Use `qwen3.8-27b` for coding and `qwen3.6-35b-a3b` for long context. See [models.md](models.md) |
| Image / 3D nodes | **ComfyUI** in a container (`/dev/kfd` + `/dev/dri`, models in `~/llm/comfyui/models`), **torch 2.13.0+rocm7.2**, `PYTORCH_TUNABLEOP_ENABLED=1` | ⚠ ComfyUI issue #15585 matches this card: older torch wheels lack the aotriton files, which slows generation about 3x. Pin 2.13.0 |
| Painting | Krita 5 + krita-ai-diffusion v1.53 → own ComfyUI | |
| Isolated repos | docker `rocm/pytorch` | For 3D repos that compile extensions |
| Editors | Blender 5.2, Unity 6000.6.0f1 | Signalis itself was made in Unity |

Roles:

| Tool | Used for |
|---|---|
| Claude Code | Multi-file work, pipeline scripts, MCP-driven Blender/Unity |
| ChatGPT agent | Research and licence checks |
| Local LLMs | Offline coding help, prompt expansion, batch text (colibri) |

## B2. Concept, characters, textures and UI (ComfyUI)

| Need | Pick | Licence |
|---|---|---|
| Main text-to-image | **Krea 2 Turbo fp8** (about 13 GB) | Community licence; commercial use OK under thresholds |
| Fast and small | **FLUX.2 [klein] 4B** (multi-reference editing), **Z-Image Turbo/Base** (6B) | Apache-2.0 |
| Best quality, non-commercial only | Qwen-Image-2.1 / Turbo | Qwen research; concept use only |
| Edits and variations | **Qwen-Image-Edit-2511** | Apache-2.0 |
| Turnarounds | **CharacterSheet LoRAs** (TripleView/QuadView) for Krea 2 / Klein | Check HF card |
| PS1 look | Unity PSX layer (B7). Optional, skipped for now: Civitai #707972 "PSX Style" LoRA (trigger "3D low-poly CGI-render") | Check page |
| Signalis look | Train your own style LoRA later | Yours |
| Textures and PBR | Z-Image + seamless tiling → Ubisoft CHORD (research-only); Material Maker (MIT) | Check per tool |
| PS1 palette | Pillow script (nearest-neighbour, quantise, dither) + Pixelorama | |

## B3. Image → 3D

1. **TRELLIS.2** (MIT) at 512³ via the ROCm ports (`toastmanAu/trellis-2-rocm-comfyui`, `steelxt/comfyui-trellis2-gguf-rocm`), with arch set to gfx1101, in docker. *Status 2026-10-11: works — image → textured .glb in ~67 s warm / 133 s cold at 512, 4.7 GiB VRAM peak, in `comfyui-trellis2-rocm:0.39.0-b3b9e95` (profile `trellis`, :8189). Licence (current, 2026-10-12): licence-clean image with no nvdiffrast or MeshLib; output is vertex-coloured (~48k tris), and the PS1 texture is baked in Blender. See [trellis.md](trellis.md) and [licenses.md](licenses.md).*
2. **Watch Pixal3D** (MIT, low-VRAM mode).
3. **Low-poly artist meshes:** FastMesh, DeepMesh or MeshAnything V2 on a rented CUDA GPU.
4. **Fallback:** HF Spaces.
5. **Architecture and modular kits:** script them in Blender from primitives, through MCP.

## B4. Cleanup in Blender (`tools/blender/ps1_prep.py`)

1. Decimate or QuadriFlow to budget:

   | Asset | Triangle budget |
   |---|---|
   | Props | 100–800 |
   | Characters | 500–2000 |
   | Creatures | up to 3000 |

2. Smart UV project.
3. Bake, or project the turnaround views. Keep a 1024px source.
4. Export, plus the derived 128/256px texture.

**MCP for Blender** (MIT) gives Claude Code live control of Blender. Safe mode is not a sandbox: the add-on socket runs arbitrary Python, so keep it on loopback. Setup, pin updates and moving the server to jupiter (`MCP_HOST` in `.mcp.json`) are in psx-iso `Tools/mcp/README.md`.

## B5. Rigging and animation

- **Humanoids:** Rigify, then automatic weights.
- **AI auto-rig:** SkinTokens (MIT, CUDA ≥14 GB → rent a GPU) or a ROCm port of UniRig.
- **Creatures:** SkinTokens or Rigify animal metarigs, plus Unity Animation Rigging for procedural IK.
- **Animation sources:** CC0 libraries (Quaternius, Kenney), plus video2mocap (GVHMR) or FreeMoCap.
- **Skip HY-Motion:** it needs 24 GB or more, and its Tencent licence excludes the EU.
- **PS1 trick:** stepped 12–15 fps animation as a toggle.
- **Research (2026-10-12):** licences, ROCm status (SkinTokens PRs) and the slice order of attack are in [characters-audio-research.md](characters-audio-research.md); GVHMR and UniRig are dropped there.

## B6. Audio

| Type | Tool | Notes |
|---|---|---|
| SFX | Stable Audio 3 Small-SFX plus jsfxr | |
| Music | ACE-Step 1.5 (MIT, under 4 GB) plus Vital in LMMS for drones | |

Research (2026-10-12): [characters-audio-research.md](characters-audio-research.md). Vital needs a paid licence for a business, so Surge XT is the default; Small-SFX needs registration and runs on CPU.

## B7. Unity

- **Render:** URP, orthographic isometric camera (about 30° pitch, 45° yaw), low-res RenderTexture with point upscale.
- **PSX layer:** Natteens/retropsx-urp or Clufin/URP-PSX-Unity6 (add its shaders to *Always Included*). All values live in `PsxSettings`.
- **Import automation:** `AssetPostprocessor` (point filter, no mips, Humanoid).
- **Unity MCP:** CoplayDev unity-mcp v10.3.
- **Version control:** git + lfs.

## B8. Setup order

1. Part A (llama-swap, see [ROADMAP.md](../ROADMAP.md)).
2. ComfyUI container with the models and LoRAs, then a test turnaround.
3. Krita + krita-ai-diffusion. *Status 2026-10-11: server side ready (plugin models in `comfyui/models.txt`, workload check passes for Klein 4B, Z-Image, Krea 2, Qwen Edit Plus); Krita-side steps in [krita.md](krita.md).*
4. TRELLIS.2 ROCm: one prop end to end. *Done 2026-10-11, see [trellis.md](trellis.md).*
5. Blender MCP + `ps1_prep.py`.
6. Unity project with PSX, `PsxSettings`, postprocessor, unity-mcp, git-lfs.
7. **Vertical slice:** 1 room tileset, 3 props, 1 humanoid, 1 creature, SFX, a music loop.
8. Colibri with one big model (disk is now sufficient), plus an overnight batch-text test. See [colibri.md](colibri.md).

## Verification

| Check | Pass condition |
|---|---|
| Image speed | Krea 2 Turbo at 1024² takes under about 30 s. Five minutes or more means bug #15585 |
| Style toggle | PSX on and off via `PsxSettings` without re-authoring assets |
| Round trip | Blender edit → re-export → Unity references intact |
| Switching | `gpu-mode comfy`, then `gpu-mode llm`, leaves exactly one large VRAM consumer, and the NVMe swap stays at 0 B |

## Which local model for game-dev tasks

| Task | Use | Why |
|---|---|---|
| Focused coding: one C# system or shader, a tricky bug in 1–3 files, Blender Python scripts, isometric projection or physics maths | `qwen3.8-27b` | Quality over context. Strongest local coder in the plan |
| Vision critique of renders and screenshots of the PS1 look, reference images, UI mockups | `qwen3.8-27b` | Strongest local VLM in the plan |
| Long context, or any session while Unity or Blender is open | `qwen3.6-35b-a3b` | 128k context. `--fit` moves experts to RAM instead of spilling dense layers. See [models.md](models.md) |
| Multi-file work and pipeline scripts | Claude Code | Plan's split: multi-file work goes to Claude Code |
| Overnight lore batches, item text, localisation drafts | colibri (`colibri-big`) | Slow, unattended tier. Needs Unity, Blender and ComfyUI closed. See [colibri.md](colibri.md) |

Notes:
- Unity and Blender take another 1–3 GiB of VRAM. Qwen3.8 can't then keep 48k on the GPU, so while the editors are open, use Qwen3.6.
- Colibri is not for chat, autocomplete, agents, or anything that needs Unity or Blender open. It runs at 1–5 tok/s.
