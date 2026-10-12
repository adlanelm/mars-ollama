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

## Plan

ROADMAP Phase 8 step 4b lists the commercially usable approaches to try, in order, measured on the same crate
brief. The cheap ones:

## Cheap next experiments

- **Verifier rule in the cheat sheet:** after building, compare `psx.report` size and bounds with the brief
  and fix it before answering. This targets the floating-posts mistake.
- **Critic pass:** a second `opencode run` with a "review only" prompt that gets the brief, a `look`
  screenshot and the report, and outputs fixes.
- **Reasoning cap for qwen3.8-27b** (no-thinking mode or a token limit), if it keeps over-thinking.
