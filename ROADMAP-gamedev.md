# Roadmap: game development track

Status: **G2-G6 done (vertical slice playable 2026-10-13)**; G1 and G4b not marked done. This is the game asset pipeline that used to be Phase 8 of [ROADMAP.md](ROADMAP.md), renumbered G1-G6. The LLM hosting it runs on (llama-swap, `gpu-mode`, GPU arbitration) is tracked there.

Items marked **[verify]** could not be confirmed from primary sources during research.

Background docs: [docs/README.md](docs/README.md).

*This track may later move to its own repo/project.*

| Step | Goal | Status |
|---|---|---|
| G1 | ComfyUI models/LoRAs, test character turnaround | Not marked done |
| G2 | Krita + krita-ai-diffusion on local ComfyUI | **Done** 2026-10-11 |
| G3 | TRELLIS.2: prop from concept to mesh | **Done** 2026-10-11 |
| G4 | Blender MCP + `ps1_prep.py` (4b: reliable agent-built assets) | Done 2026-10-12 (4b: plan, not marked done) |
| G5 | Unity URP project: PSX shader, `PsxSettings`, unity-mcp | **Done** 2026-10-12 |
| G6 | Vertical slice (environment, characters, audio, playable) | **Done** 2026-10-13 |

---

## Game asset pipeline (was Phase 8)


These steps live in the game project repos. Full detail: [docs/game-asset-workflow.md](docs/game-asset-workflow.md).

G1. ComfyUI models and LoRAs, then a test character turnaround.
G2. Krita + krita-ai-diffusion connected to the local ComfyUI. *Done 2026-10-11: Krita Flatpak 5.3.4.1 + plugin 1.53.0 on the custom server; generate (Klein, Krea 2), inpaint and upscale verified by the user. MAT → LaMa and NMKD → OmniSR 4x on 2026-10-12 (licence). See [docs/krita.md](docs/krita.md).*
G3. TRELLIS.2 (ROCm port, gfx1101): take one prop from concept to mesh. *Done 2026-10-11: prop-terminal concept → 188k-tri textured .glb (2048² PBR) in 67 s warm / 133 s cold at 512, VRAM peak 4.7 GiB, NVMe swap 0; Aero-Ex nodes + egore/toast ROCm recipe, all clones pinned. On 2026-10-12 the image was made licence-clean (no nvdiffrast/MeshLib; vertex colours, ~48k tris, 63 s warm, 4.1 GiB). The PS1 texture is baked in Blender later. See [docs/trellis.md](docs/trellis.md) and [docs/licenses.md](docs/licenses.md).*
G4. Blender MCP plus `ps1_prep.py` (decimate, UV, bake, export at 128/256px).
   *Done 2026-10-12 (psx-iso `main`, cf3a423; that repo has no remote):*
   - *`ps1_prep.py`: vertex-colour bake, hard triangle maximum, 1024px source to `art-src/` via `--src-dir`.*
   - *Blender MCP pinned, safe mode, `MCP_HOST` switch for jupiter; works from opencode.*
   - *The `bpy.ops.psx.*` helper add-on and the cheat sheet, loaded via `AGENTS.md`.*
   - *Round trip: `prop-terminal_00008_` → 784 tris, 256² point-filtered texture → Unity; upright, true size
     (0.77 × 0.86 × 1.0 m). Check with `PsxAssetReport` (`-executeMethod PsxIso.Editor.PsxAssetReport.Run`).*

   *Gap, moved to G5: imported materials get URP/Lit, not `PsxIso/PsxLit`.*
   **G4b. Make agent-built assets reliable, using only commercially usable approaches.** Details:
   [docs/blender-agent-models.md](docs/blender-agent-models.md). Try them in this order and measure each on
   the same crate brief (size, ground contact, triangles, part placement, honest report):
   1. **Verifier rule:** before answering, the agent compares `psx.report` size and bounds with the brief and
      fixes mismatches. This only changes the cheat sheet.
   2. **Render → inspect → edit loop**, the VIGA pattern (MIT). After building, call `look` from fixed views
      and run a second "review only" pass that lists fixes, then apply them. Best-of-N if it helps.
   3. **Separate local vision critic** (Qwen3-VL-8B class) as a llama-swap entry, given the screenshots,
      the report and the brief.
      - Before using it: check that its licence allows commercial use and that the GGUF runs on Vulkan
        **[verify]**, and that it fits beside Blender and the coder in 16 GB.
   4. **BlenderLLM** (8B, Apache-2.0 weights and code) as a one-shot bpy object-script generator, called by
      the agent. It needs a self-made GGUF, and its quality claims come from the authors' own benchmark.
   5. **Optional:**
      - **Docs retrieval:** a Blender API retrieval skill (the BlenderRAG idea; we implement it ourselves, not
        LL3M's academic-only code).
      - **Reasoning cap** or no-thinking mode for qwen3.8-27b.
      - **Mesh generators:** MeshXL (MIT) and DeepMesh (LICENSE unclear) only after their licences are
        verified, and on a rented CUDA GPU.

      Excluded as non-commercial: LLaMA-Mesh, MeshAnything, LL3M (and FastMesh until verified).
G5. Unity URP project: PSX shader, `PsxSettings`, AssetPostprocessor, unity-mcp, git-lfs. **Done 2026-10-12.**
   *Already there: `PsxLit` shader and material, `PsxSettings`, `PsxImportPostprocessor` (texture and model
   defaults), git-lfs rules. Done 2026-10-12:*
   - *Imported materials under `Art/` use `PsxIso/PsxLit` with their texture; `PsxAssetReport` fails otherwise.*
   - *unity-mcp: MCP for Unity v10.3.0 (MIT) via `Tools/mcp/unity-mcp.sh` (`MCP_HOST` switch), stdio bridge
     on 127.0.0.1:6400. `McpForUnityPrefs.cs` pins stdio, telemetry off and no client-config rewrites. Tested
     from opencode (console, scene, create/find/delete). Roslyn DLLs kept for `execute_code`.*
   - *Packages added: Cinemachine, glTFast, ProBuilder.*

   - *Re-export check: `PsxIso > Re-export check` (Snapshot, then Verify) in the editor. Re-exporting
     `prop_terminal.fbx` 784 → 588 tris kept the FBX GUID, mesh/material fileIDs and the `PropTerminal` prefab's
     references (12/12 PASS); restored to 784 afterwards. Rule: re-export with the same `--out` over the file.
     The mesh name comes from the source glb node (`geometry_0`), not from `--out`.*
G6. **Vertical slice:** 1 room tileset, 3 props, 1 humanoid, 1 creature, an SFX set and a music loop.
   *2026-10-12, environment part done (psx-iso `Scenes/Slice.unity`, built by `PsxIso/Build Slice scene`):*
   - *Room tileset: `Tools/blender/kit_scifi_room.py`, 7 scripted pieces (10–56 tris), 2 m grid, 3 m walls,
     one 256 px procedural atlas; `TestRoom` prefab.*
   - *Props: terminal 784, locker 490, crate 392 tris; Krea 2 Turbo concept → TRELLIS.2 → `ps1_prep`
     (`--max-dim`, `--ground`; mesh named after `--out`) → prefab. All pass `PsxAssetReport`.*
   - *Known gaps: vertex-colour TRELLIS loses fine detail (the locker's stencilled "07"); uniform scaling
     keeps concept proportions (crate flatter than briefed); Play-mode render path not checked.*
   - *Characters and audio: plan in [docs/characters-audio-research.md](docs/characters-audio-research.md).*

   *2026-10-12/13, characters and audio done (all in `Slice.unity`, `PsxAssetReport` failures=0):*
   - *Prop fixes: `ps1_prep --size W D H` (crate 1.0 × 0.6 × 0.7 m) and `--decal` (locker "07").*
   - *Crew: Krea 2 → TRELLIS.2 → `ps1_prep` (1433 tris) → `rig_humanoid.py` (Rigify-derived 22 deform bones,
     automatic weights) → Unity Humanoid (valid avatar); Quaternius UAL clips (CC0 in the zip) idle/walk/run/
     hit/death; Crew.controller and prefab.*
   - *Crawler: 2442 tris, `rig_crawler.py` (22 bones, Generic, automatic weights), hand-keyed Idle/Crawl/Attack
     at 12 keys/s; Crawler.controller and prefab.*
   - *SFX: 15 procedural sounds (`Tools/audio/sfx_gen.py`); footsteps and doors replaced 2026-10-13 by DXMD-style
     clips built from Sonniss GDC recordings (`Tools/audio/sfx_sonniss.py`, picked by ear: steps set F, doors set E,
     plus run steps and `door_locked`; licence in [docs/licenses.md](docs/licenses.md)). Other SFX not listened to yet.*
   - *Music: ACE-Step 1.5 (MIT) DiT-only, 60 s station drone loop, -18 LUFS
     ([docs/acestep.md](docs/acestep.md)). Not listened to yet.*
   - *SkinTokens (MIT; vendored GPL-3.0 part) runs on gfx1101 via PR #14: ~35 s, 11 GiB per rig
     ([docs/skintokens.md](docs/skintokens.md)); compared 2026-10-13: the hand rigs win for both characters.*
   - *2026-10-13, playable (psx-iso `2fdd8e9`, `d512a24`): player (walk/run, footsteps), keycard + terminal unlock a
     locked door, crawler chase/attack, crew NPC patrol with hit/death, HUD on the PSX path, music crossfade to a
     tension loop when the crawler engages. 11 PlayMode tests pass (batchmode). Crew clips were facing 180° (fixed);
     foot drift 2% walk / 5% run. Not done: crawler can't be hurt, crawler crawl speed vs clip not checked.*
   - *Audio redone DXMD-style 2026-10-13 (user picks by ear): UI/terminal/item/creature set G (`sfx_dxmd.py`), station
     music ACE-Step instrumental seed 4222, tension loop seed 5211; captions name no artists. Old Ollama data removed
     (hosting track Phase 5).*
   - *GPU: automatic LLM ↔ ComfyUI arbitration ([docs/gpu-switching.md](docs/gpu-switching.md)) after
     parallel GPU use crashed the desktop.*

---

## Next

1. **P0-c Blender bake in `ps1_prep.py`**: done (bake existed since psx-iso `9cf5508`; all five slice assets ship 256 px bakes, re-runs byte-identical; 128 px island gap fixed in `29a50da`, checked 2026-10-13). The locker "07" still needs `--decal` (TRELLIS source shows "4D").
2. **unity-mcp server moves to jupiter**, Blender MCP stays on mars: configured 2026-10-13 (psx-iso `d428913`; `MCP_HOST=jupiter.internal`, uv 0.13.0 on jupiter, server start + tool list over the tunnel checked, cloud asset-gen tools disabled). End-to-end call with the editor open: still to do.
3. **Game-design and pipeline-optimisation phase**: placeholder, not yet planned.
4. **Known gaps of the slice:**
   - The crawler can't be hurt.
   - Crawler crawl speed vs clip not checked.
   - `PsxSettings` PSX toggle.
