# pi config for mars

General config for the [pi](https://pi.dev) coding agent against llama-swap. Install with `bin/pi-setup`:
`models.json`, `extensions/`, `agents/` and `prompts/` are symlinked into `~/.pi/agent/` (edits apply on the next
pi start or `/reload`); `settings.json` is copied, because pi writes to it, so re-run after editing it.
Project-specific skills and prompts go in the project's `.pi/settings.json` (gamekit's game template does this).

## Subagents (enforced delegation)

`extensions/subagent/` is loaded in every project (the user extensions dir). It is pi's subagent example rewritten
so that delegation and context limits are enforced by code, not left to the model.

**Main session**
- `subagent` tool. Modes: single (`goal`, `agent` (default: worker if there is a scope, else scout), `scope`, `inputs`,
  `done_when`, `check`), `tasks` (parallel, 2 at a time,
  max 4), `chain` (each step gets the previous report as input; `scope: ["@previous"]` takes the plan's `FILES:`
  line, or else the existing file paths the report mentions), and `resume` (optionally with a new `scope`).
- Tools are withdrawn, not just blocked: the main session's active tool set (`setActiveTools`) never includes
  `write`, loses `edit` in plan-only prompts and after 2 edited files, and loses read, bash, grep, find and ls
  once the budget is used up. The full set comes back on the next user prompt. A blocked call reads to a local
  model as "try a variation" (29 blocked calls in one psx-iso run); a missing tool doesn't. The guards below
  remain as a fallback.
- Context guard:
  - `read` is clamped to 200 lines.
  - Every read, bash, grep, find and ls result is capped at 6 KB.
  - After 24 KB of direct output in one prompt, read, grep, find, ls and bash are blocked ("delegate"). The
    exceptions are check commands: Tools/check, test runners, git status/diff --stat, and any `check` given to a
    subagent. They may be wrapped in `cd X &&`, `timeout N`, `bash`, `2>&1`, `> log` or `| tail`, and their output
    is cut to 1 KB. Reads of handoff reports always work (capped at 4 KB) and don't count toward the budget. The
    budget resets on each user prompt.
  - The main session may edit at most 2 existing files per prompt and never creates or overwrites files
    (`write`); new files go to a worker. SKILL.md reads are whole (up to 16 KB) and don't count toward the
    budget.
- Plan-only prompts: a prompt containing `[plan-only]` (`/plan`, `/scout-and-plan`, gamekit's `/backlog-item`)
  can read and run scouts and planners (a scope given to a read-only agent is dropped), but edit/write, writing bash, worker tasks and resumes with a scope are
  blocked until the next user prompt. "Wait for my OK" therefore always holds.
- Project check: a writing task without `check` gets `bash <project>/.pi/check` if that file exists (searched
  from the task cwd up to the git root). Verification doesn't depend on the main session remembering to ask for
  it. gamekit's templates add one: Unity runs Tools/check plus the PlayMode tests, Godot runs Tools/check.
- Automatic review: when a prompt that changed files is about to finish (`agent_before_settle`), the reviewer
  agent reads the diff of exactly those files (worker scopes plus the main session's edits) and lists concrete
  defects. Its SUMMARY comes back as a message, with one more turn to fix the issues through a worker or explain
  why not. There are at most 2 reviews per prompt (the second after fixes), none in plan-only prompts, and a
  clean review just lets the main session finish.
- Block messages name the exact next call, e.g. `subagent({agent: "worker", goal: ..., scope: ["<the file>"]})`.
- What comes back is a status line, a report path and the `SUMMARY:` line, never the report itself. If a subagent
  leaves out the SUMMARY line, it is resumed once and asked for exactly that line.
- `check`: a command the extension runs itself after the subagent finishes (skipped when the run changed no files), logged to `NN-agent.check.log`. Exit 0
  is a pass. On failure the subagent is resumed with the last lines, up to 2 times, then the task is reported as
  `failed` with the log path. Verification doesn't depend on what the subagent says (on psx-iso a worker
  reported "both checks pass" while a PlayMode test was failing).
- The delegation rules live in the tool's `promptGuidelines`, which subagents never see.

**Subagents** (`PI_SUBAGENT=1`; they don't get the `subagent` tool, so they can't recurse)
- Narrow task: the extension builds the task message (GOAL, DONE WHEN, WRITABLE SCOPE, INPUTS, REPORT FORMAT).
  Each subagent inherits the main session's model, because llama-swap holds only one chat model and a different
  one would swap.
- Scope enforcement:
  - `edit`/`write` calls outside the scope are blocked.
  - After every run, a git check undoes changes outside the scope, including bash writes. Tracked files are
    restored and new files are moved to `<run>/quarantine/`. Files that were already dirty before the run and
    weren't touched are left alone. Build artefacts (`__pycache__`, `node_modules`, `Library`...) and Unity-generated `.meta` files next to an existing asset are ignored. Quarantine moves work across filesystems, and a failing scope check is reported instead of crashing pi.
  - Concurrent parallel runs don't undo each other's in-scope work.
  - Subagents can't change git state: commit, reset, checkout, stash, add, rebase, push and the like are blocked
    in bash. If HEAD moves anyway, it is moved back with `reset --soft` before the scope check, so a commit can't
    hide changes outside the scope.
  - Outside git, only the edit/write guard applies. `<file>.meta` belongs to `<file>`'s scope (Unity).
- Out of scope: the subagent calls `request {need, why}` and stops (`terminate`). The main session dispatches a
  helper with `for: "<id>"` (or makes a one-line edit itself), then sends `resume: "<id>"`. The resumed subagent
  is told where the helper's report is.

**Handoff area**: `$PI_HANDOFF_ROOT` (default `/tmp/pi-handoff`), one folder per main session, holding
`NN-agent.md` (the report, written by the extension), `NN-agent.jsonl` (the session, for resume),
`NN-agent.prompt.md` and `quarantine/`. Only paths go through the main session.

Prompts: `/implement` (scout, planner, worker with `@previous` scope), `/scout-and-plan`, `/implement-and-review`.

## Compaction

`-np 2 --kv-unified` means the parent and a subagent share one 131072-token KV pool. The parent therefore compacts
at about 64k (`reserveTokens` 65536) and keeps 16k. qwen3.8-27b (49152 window) overrides this: reserve 8192 and
keep 10000, with `maxTokens` 8192.

Tests 2026-10-17, qwen3.6, synthetic repos:
- `/implement` (add a parameter and a test) took one `subagent` call and 2 min. The main session peaked at 3.5k
  tokens, and the planner's FILES became the worker's scope.
- A worker scoped to calc.py, told to write main.py and a new file with bash: main.py was restored from HEAD, the
  new file was quarantined, and the in-scope edit and the uncommitted work that was already there were kept.
- Out of scope: block, then `request`, then NEEDS, then the main session fixes it, then `resume`, then done. The
  main session stayed at about 4k tokens.
- Main session told to read a 55k-token file itself: it was blocked at the budget and peaked at 15k.
2026-10-17 KV pressure, qwen3.6: parent at 57k plus a read-only subagent at 43k (about 100k in the shared pool).
No server errors, the subagent's answer was correct, and the parent's next turn re-processed only 185 tokens
(cacheRead 56983), so its prefix survived the subagent.
