# pi config for mars

General config for the [pi](https://pi.dev) coding agent against llama-swap. Install with `bin/pi-setup`:
`models.json`, `extensions/`, `agents/` and `prompts/` are symlinked into `~/.pi/agent/` (edits apply on the next
pi start or `/reload`); `settings.json` is copied, because pi writes to it, so re-run after editing it.
Project-specific skills and prompts go in the project's `.pi/settings.json` (gamekit's game template does this).

## Any project

The harness is on in every pi session (user-level extension), no project setup needed:
- Checks: `.pi/check-fast` and `.pi/check` win. Without them the harness derives checks from the project files,
  in this order:

  | Project file | After each step | At the end |
  |---|---|---|
  | `package.json` scripts | typecheck, build, lint or check (npm/pnpm/yarn/bun) | test |
  | `Cargo.toml` | `cargo check` | `cargo test` |
  | `go.mod` | `go build` + `go vet` | `go test` |
  | `.sln`/`.csproj` | `dotnet build` | `dotnet test` |
  | Python | `compileall` | `pytest` (when there are tests) |
  | `Makefile` | build or check target | test target |

  The main session is told which commands apply. With none found it is told changes are unverified, and to pass
  `check` or ask for a .pi/check.
- Red baselines: before a prompt's first writing step the per-step check runs once on the untouched tree. If it
  already fails, its error lines are recorded (normalized: relative paths, no line numbers, durations or summary
  counts), and a step's failing check counts as passed when it adds no new errors ("no new failures: 20 known
  errors before, 6 now"). Retries list only the new errors. ehi-ledger's build was red from known-vulnerable
  packages, so every step had failed although each fixed its part. The full end check has no baseline.
- Git: outside a git repository writing subagents are refused, because scoping, snapshots and undo all need git.
  Run `git init` and commit, or set `allowNoGit`.
- `.pi/harness.json` (per project, searched up to the git root): `{"enabled": false}` turns the harness off.
  Overrides: `maxStepFiles`, `turnBudgetKB`, `maxReviews`, `allowNoGit`, `check`, `fullCheck` (commands run in
  the project root). `PI_HARNESS=off` turns it off for one session.
- New game projects from gamekit's `game-init` get `.pi/settings.json`, `.pi/check-fast` and `.pi/check` from
  the templates.

## Tested on a non-game project: ehi-ledger (2026-10-18)

The project: a .NET 10 solution (`Ehi.Ledger.slnx`, 8 src projects, unit and integration tests), cloned, with no
pi setup. Its build was red at the start: 20 restore errors, NU1902/NU1903 known-vulnerable packages treated as
errors. The task: `/plan` "make it build by moving to patched versions, without turning the warnings off", then
"OK, implement it".
- Detection: per-step `dotnet build`, `dotnet test` at the end, both from the .slnx.
- Plan (8.5 min, 61 tool calls, peak 40k tokens): `dotnet list package --vulnerable` and the NuGet API showed
  that all four packages are transitive (MessagePack via Aspire, Microsoft.OpenApi and OpenTelemetry.Api via
  AspNetCore and Akka.Hosting, SSH.NET via Testcontainers). The fix: pin patched versions, one .csproj per step.
  Plan-only blocked an attempt to `sed` TreatWarningsAsErrors off while investigating.
- Implementation (12.5 min, peak 51k tokens): a chain of 3 workers. Build errors went 20, 14, 2, 0; each step
  passed as "no new failures" against the red baseline. Before the baseline fix, step 1 was marked failed,
  retried twice for nothing, and the chain stopped there.
- The end check `dotnet test` passed (4/4). The automatic review called the override pins "dead" packages; the
  main session rejected that correctly (transitive overrides, proven by the build).
- Result: 4 PackageReference pins in 3 .csproj files, 0 errors and 0 warnings, tests green. One defect a human
  must catch: a pin comment cites a CVE number that does not match the reported GHSA advisories (likely invented).

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
  - After 48 KB of direct output in one prompt (24 KB was too small to plan a psx-iso item: the files involved are 43 KB, and the model worked around it with verbatim-copy scouts), read, grep, find, ls and bash are blocked ("delegate"). The
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
- Project check: the project check always runs for writing tasks, and a task `check` is added to it, never replaces it (the main session once passed `check: "echo done"`). A writing task without `check` gets `bash <project>/.pi/check` if that file exists (searched
  from the task cwd up to the git root). Verification doesn't depend on the main session remembering to ask for
  it. gamekit's templates add one: Unity runs Tools/check plus the PlayMode tests, Godot runs Tools/check.
- Automatic review: when a prompt that changed files is about to finish (`agent_before_settle`), the reviewer
  agent reads the diff of exactly those files (worker scopes plus the main session's edits) and lists concrete
  defects. Its SUMMARY comes back as a message, with one more turn to fix the issues through a worker or explain
  why not. There are at most 2 reviews per prompt (the second after fixes), none in plan-only prompts, and a
  clean review just lets the main session finish.
- The main session's bash runs commands but cannot change files: each bash call is bracketed by a git snapshot, and any file it changed is undone (tracked files restored, new files quarantined, commits reset), with a note telling it to use edit or a worker. Git commands that change state are blocked. On psx-iso it had patched 3 files with heredoc Python scripts and committed the user's uncommitted settings.
- Subagent tasks asking for files "verbatim" or "in full" are refused without spawning; subagents report findings.
- Small steps: a writing subagent task may change at most 2 files, named one by one (no directories);
  bigger tasks are refused with "split into steps". Plans (`/plan`, `/backlog-item`, the planner agent) come as
  numbered steps of 1-2 files that each leave the project building, with tests as their own step, run as a
  chain of workers. On psx-iso a single 4-file task left a compile error spread over the AI, the scene builder
  and the tests, and the fix broke the shared test setup.
- A subagent whose final message has no answer text (reasoning only) reports its last reasoning, marked as such, instead of "no output".
- Writing subagents run one at a time (also across parallel tool calls); read-only scouts stay parallel. Two writers at once fail each other's checks.
- Two check levels: `.pi/check-fast` (build/validate) after every step; `.pi/check` (full tests) once before
  the prompt finishes while files are unverified, at most 2 fix rounds, before the review. Without a
  check-fast, `.pi/check` runs per step as before.
- Waiting runs: while a subagent waits with NEEDS, other runs' project checks are deferred (its half-done change would fail them) and run when it is resumed; a prompt that would finish with runs still waiting is sent back once to resume them.
- Grounding: a plan-only prompt that finishes without having looked at code (no non-Markdown read, search or subagent) is sent back once.
- Path check: before a prompt finishes, file paths in the final answer that are presented as existing (not on a "new"/"create" line) and do not exist send the session back once to look at the code. On psx-iso a plan read only GAME.md and BACKLOG.md and named wrong paths.
- Nudge: a prompt that ends with no tool call and a short answer that announces an action ("- Read GAME.md", "Let me…") is sent back once to do it.
- Settle hooks (nudge, path check, review) add a custom message and continue; they must not gate on `event.context.canContinue`, which is false whenever the assistant spoke last (always, at settle). Until 2026-10-17 that gate silently disabled all three. `PI_SUBAGENT_DEBUG=<file>` logs each settle.
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

## Thinking per task

The harness picks the thinking level per task, and the level picks a reasoning budget:
- Agents declare `thinking:` in their frontmatter: scout `minimal`, worker `low`, planner and reviewer `medium`;
  the main session keeps its own level. A worker retried after a failed check thinks one level higher (up to
  `high`).
- `models.json` sends `thinking_budget_tokens` per level (`samplingParamsByThinkingLevel`): minimal 256, low
  1024, medium 3072, high 8192. qwen3.6 ignores `reasoning_effort` (minimal reasoned the most), so the budget is
  the real control.
- At the limit llama-server closes the reasoning (a hard cut; the model does not plan for it) after injecting
  `--reasoning-budget-message` (llama-swap `chat_common`): "Thinking budget reached. Act now with what I have."

Synthetic shop task (2026-10-18, qwen3.6): reasoning across subagents dropped from 23.6k to 16.1k chars (max per
turn 5.5k to 3.5k), with the same result, no budget cuts and the same wall time (~9 min). The automatic review
fired for the first time: review 1 found a missing bounds check, which was fixed; review 2 raised out-of-scope
nits and the 2-review cap ended it.

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
