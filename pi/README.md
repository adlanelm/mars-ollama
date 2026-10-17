# pi config for mars

General config for the [pi](https://pi.dev) coding agent against llama-swap. Install with `bin/pi-setup`:
`models.json`, `extensions/`, `agents/` and `prompts/` are symlinked into `~/.pi/agent/` (edits apply on the next
pi start or `/reload`); `settings.json` is copied, because pi writes to it, so re-run after editing it.
Project-specific skills and prompts go in the project's `.pi/settings.json` (gamekit's game template does this).

## Subagents

`extensions/subagent/` is pi's bundled subagent example (1.3.0), adapted for local models with small windows.
The main session delegates reading and self-contained edits to `agents/*.md` (scout, planner, worker, reviewer),
each a fresh `pi` process. Only the short report comes back. Changes from the example:

- Subagents get no `model:`, so they inherit the parent's model. llama-swap loads one chat model at a time, so a
  different model would swap the parent's out (10-20 s each way).
- `MAX_CONCURRENCY` is 2, matching `-np 2`.
- Output is capped at 12 KB per task, in every mode, and agents are told to keep the final answer under 600 words.
- Recursion guard: children run with `PI_SUBAGENT=1` and don't register the tool.
- Delegation rules go in the tool's `promptGuidelines`, so only the parent sees them. Children also load AGENTS.md.
- Prompts: `/implement`, `/scout-and-plan`, `/implement-and-review` (chains).

## Compaction

`-np 2 --kv-unified` means the parent and a subagent share one 131072-token KV pool. The parent therefore compacts
at about 64k (`reserveTokens` 65536) and keeps 16k. qwen3.8-27b (49152 window) overrides this: reserve 8192 and
keep 10000, with `maxTokens` 8192.

2026-10-17, qwen3.6: worker reported tools `read, bash, edit, write` with no `subagent`; 3 parallel scouts answered
correctly in 65 s; no model swap.
