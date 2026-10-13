# Models and techniques for driving Blender with an agent

Research notes from 2026-10-12, to pick up later. Context: opencode + MCP for Blender (safe mode) + the
`bpy.ops.psx.*` helpers in psx-iso. With the cheat sheet loaded, qwen3.6-35b-a3b builds a correct-ish crate.
It misplaces parts and doesn't notice from its own report.

## Bottom line

No open Blender-specialised model found beats the current Qwen models as the agent driver. Gains are more
likely from technique than from a new model:

- **a verifier step:** renders or measured bounds checked against the brief;
- **API retrieval:** the cheat sheet, possibly as an opencode skill;
- **a separate critic model**, so the coder doesn't grade itself.

Coverage was about 12 searches; no 2025–2026 open Blender agent model was confirmed.

## Shortlist

1. **VIGA-style render → inspect → edit loop.** The code is MIT, but it's API-oriented, so we'd copy the
   pattern, not the code.
   - Measured with Qwen3-VL-8B on BlenderBench: point-level error drops from 60.8 to 10.5 (task 1) and from
     33.1 to 3.9 (task 2).
   - BlenderGym: +22.6% best-of-1, +42.9% best-of-4.
   - Links: <https://github.com/Fugtemypt123/VIGA>, <https://arxiv.org/abs/2601.11109>
2. **A small local VLM as critic** (Qwen3-VL-8B class), as a second llama-swap entry that reviews `look`
   screenshots and `psx.report` output. Its GGUF/Vulkan support and licence are not verified.
   - qwen3.6-35b-a3b already accepts images through its mmproj and called `look` in the tests. A separate
     critic is about not grading its own work.
3. **BlenderLLM** (8B, Apache-2.0 weights and code; based on Qwen2.5; Dec 2024).
   - Results: CADBench-Sim 0.748 / Wild 0.664, against Claude-3.5-Sonnet 0.593 / 0.489. That's the authors'
     own benchmark with a GPT-4o judge, so treat it as optimistic.
   - It has no tool calling and no GGUF (we'd convert it ourselves), so at most it's a one-shot "object
     script" tool, not the agent.
   - Links: <https://huggingface.co/FreedomIntelligence/BlenderLLM>, <https://arxiv.org/abs/2412.14203>
4. **LLaMA-Mesh** (8B, text to OBJ). **Non-commercial** (NSCLv1 + Llama 3.1), so prototyping only.
5. **MeshCoder** (MIT code, Llama-3.2-1B LoRA): point cloud → part-structured bpy code. Its stack is CUDA, so
   it's a poor fit, but its bpy wrapper library is a useful idea for the psx helpers.

## Others found

| Name | Licence | Notes |
|---|---|---|
| LL3M | academic/evaluation only | Multi-agent with API LLMs; the hosted server is gone. Its BlenderRAG (docs retrieval) idea is useful |
| MeshAnything V1/V2 | S-Lab, non-commercial (verified) | Mesh → artist mesh, 800/1600 faces |
| DeepMesh | LICENSE unclear (Apache vs MIT), [verify] | 0.5B, point cloud or image conditioned, CUDA |
| MeshXL | MIT (sidebar), [verify] | 125M–1.3B; text-to-mesh checkpoint is 350M; CUDA |
| FastMesh | not checked, likely non-commercial | Weights on a third-party mirror |
| CAD-Coder (gudo7208) | Apache-2.0 (HF metadata) | Qwen2.5-7B, CadQuery rather than bpy; CAD parts only |
| SceneOrchestra | none found | 2026 paper, no code |

## Baseline: crate brief (2026-10-12)

**The brief:**
- an 0.8 m wooden crate on the ground at (4.8, 0.6), rotated 15°;
- a darker plank frame;
- under 150 tris;
- two materials;
- parts parented to `Crate`.

Each run was measured in Blender afterwards, not taken from the model's report.

| Setup | Time | Result |
|---|---|---|
| qwen3.6-35b-a3b, raw bpy, cheat sheet **not loaded** (opencode ignores `instructions`) | a few min | 276 tris (it claimed 138), wrong size, below the ground, duplicate materials, a false report |
| qwen3.8-27b, same | 18 min | 0 tool calls; it hit the 16k output limit while still reasoning |
| qwen3.6-35b-a3b + `AGENTS.md` cheat sheet + `psx` helpers | 6 min, 14 calls | right place and rotation, 132 tris (true), no duplicates. Corner posts 0.38 m too high (the crate ended up 1.14 m tall), not noticed |
| qwen3.8-27b + `AGENTS.md` + helpers | 18 min, 9 calls, about 14 tok/s with Blender open | correct: 0.8 m body on the ground, 108 tris, posts and top rails. The report is accurate except a lamp clearance of "0.25 m" (really about 0.1 m) |

The cheat sheet only works when opencode loads it as `AGENTS.md`. The helpers remove the duplicates and the
made-up numbers. What's left is that 3.6 doesn't notice its own placement mistakes, which is what step 4b
items 1–3 target.

## Plan

[ROADMAP-gamedev.md](../ROADMAP-gamedev.md) step G4b lists the commercially usable approaches to try, in order, measured on the same crate
brief. The cheap ones:

## Cheap next experiments

- **Verifier rule in the cheat sheet:** after building, compare `psx.report` size and bounds with the brief
  and fix it before answering. This targets the floating-posts mistake.
- **Critic pass:** a second `opencode run` with a "review only" prompt that gets the brief, a `look`
  screenshot and the report, and outputs fixes.
- **Reasoning cap for qwen3.8-27b** (no-thinking mode or a token limit), if it keeps over-thinking.
