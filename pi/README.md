# pi config for mars

General config for the [pi](https://pi.dev) coding agent against llama-swap. Install with `bin/pi-setup`:
`models.json`, `extensions/`, `agents/` and `prompts/` are symlinked into `~/.pi/agent/` (edits apply on the next
pi start or `/reload`); `settings.json` is copied, because pi writes to it, so re-run after editing it (the Flint
default and the qwen3.8-gsq compaction override sat unapplied until it was re-run on 2026-10-24) (a live copy that differs is
kept as `settings.json.bak.<timestamp>` first).
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
  | tracked `.py` files (none of the above gave a check) | `compileall` | `pytest` (when there are tests) |

  The main session is told which commands apply. With none found it is told changes are unverified, and to pass
  `check` or ask for a .pi/check. Each writing step without a check also ends with
  `check: none (no check detected; add .pi/check or pass check)`.
- Red baselines: before a prompt's first writing step in a directory, the per-step check runs once on the
  untouched tree (one baseline per cwd and check, so a step in another directory or repo gets its own). If it
  already fails, its exit code and error lines are recorded (normalized: relative paths, no line numbers,
  durations or summary counts). A step's failing check is compared only when it exits with the baseline's code,
  and counts as passed when it adds no new errors ("no new failures: 20 known errors before, 6 now"); retries
  list only the new errors. A different exit code (a crash, an OOM kill, another failing stage), a timeout or
  an abort fails as usual. Limit: the same code with only known error lines passes. When the baseline failed
  without error lines (a bare `exit 1`), a failure with its code can't be compared: the step fails at once, not
  retried, and the report says the failure may predate the step. ehi-ledger's build was red from
  known-vulnerable packages, so every step had failed although each fixed its part. The full end check has no
  baseline.
- Git: outside a git repository writing subagents are refused, because scoping, snapshots and undo all need git.
  Run `git init` and commit, or set `allowNoGit`.
- `.pi/harness.json` (per project, searched up to the git root): `{"enabled": false}` turns the harness off.
  Overrides: `maxStepFiles`, `turnBudgetKB`, `maxReviews`, `allowNoGit`, `check`, `fullCheck` (commands run in
  the project root), `writeCommands` (program prefixes, e.g. `["Tools/comfy/gen"]`, whose file changes the main
  session's bash keeps), `pauseWhile` (`[{"process": "krita", "message": "..."}]`: while a process with that exact name
  runs, prompts are not sent to the model; gamekit pauses for Krita, whose ComfyUI and a reloading chat model together
  ran the host out of RAM). `PI_HARNESS=off` turns it off for one session.
- New game projects from gamekit's `game-init` get `.pi/settings.json`, `.pi/harness.json` (its generators as
  `writeCommands`), `.pi/check-fast` and `.pi/check` from the templates.

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
  once the budget is used up. In `/implement` (`[workers-edit]`) it has only read and subagent from the start: no
  edit, bash, grep, find or ls (refused calls made it try variations: find, ls, pwd, `echo hello`). The full set comes back on the next user prompt. A blocked call reads to a local
  model as "try a variation" (29 blocked calls in one psx-iso run); a missing tool doesn't. The guards below
  remain as a fallback.
- Context guard:
  - `read` is clamped to 200 lines.
  - Every read, bash, grep, find and ls result is capped at 6 KB.
  - After 48 KB of direct output in one prompt (24 KB was too small to plan a psx-iso item: the files involved are 43 KB, and the model worked around it with verbatim-copy scouts), read, grep, find, ls and bash are blocked ("delegate"). The
    exceptions are check commands: Tools/check, test runners, git status/diff --stat, and any `check` given to a
    subagent. They may be wrapped in `cd X &&`, `timeout N`, `bash`, `2>&1`, `> log` or `| tail`, and their output
    is cut to 1 KB. Reads of handoff reports always work (capped at 8 KB, so a whole report fits) and don't count toward the budget. The
    budget resets on each user prompt.
  - The main session may edit at most 2 existing files per prompt and never creates or overwrites files
    (`write`); new files go to a worker. SKILL.md reads are whole (up to 16 KB) and don't count toward the
    budget.
- Plan-only prompts: a prompt containing `[plan-only]` (`/plan`, `/scout-and-plan`, gamekit's `/backlog-item`)
  can read and run scouts and planners (a scope given to a read-only agent is dropped), but edit/write, writing bash, worker tasks and resumes with a scope are
  blocked until the next user prompt. "Wait for my OK" therefore always holds.
- Project check: the project check always runs for writing tasks, and a task `check` is added to it, never replaces it (the main session once passed `check: "echo done"`). A writing task without `check` gets `bash <project>/.pi/check-fast` per step when that exists (the full `.pi/check` then runs once at the end), else `bash <project>/.pi/check` if that file exists (searched
  from the task cwd up to the git root). Verification doesn't depend on the main session remembering to ask for
  it. gamekit's templates add one: Unity runs Tools/check plus the PlayMode tests, Godot runs Tools/check.
- Automatic review: when a prompt that changed files is about to finish (`agent_before_settle`), the reviewer
  agent reads the diff of exactly those files (the files writing runs touched, also failed ones, plus the main
  session's edits) and lists concrete
  defects. Its SUMMARY comes back as a message, with one more turn to fix the issues through a worker or explain
  why not. There are at most 2 reviews per prompt (the second after fixes), none in plan-only prompts, and a
  clean review just lets the main session finish.
- The main session's bash runs commands but cannot change files: each bash call is bracketed by a git snapshot, and any file it changed is undone (tracked files restored, new files quarantined, commits reset), with a note telling it to use edit or a worker. Git commands that change state are blocked. On psx-iso it had patched 3 files with heredoc Python scripts and committed the user's uncommitted settings. Exception: a single command whose program matches `writeCommands` keeps its files (gamekit's `Tools/comfy/gen` concept images were quarantined the moment they were written); chaining (`;`, `&&` after a leading `cd`, pipes other than `| tail`, here-docs, `$(...)`) loses the exception.
- Subagent tasks asking for files "verbatim" or "in full" are refused without spawning; subagents report findings.
  Negated mentions ("do not dump full file contents") don't count, and the refusal names the phrase and says to
  resend without it: on psx-iso (2026-10-24) that exact negated sentence was refused twice, the main session took
  "refused" to mean subagents can't read files, scouted itself and edited a test file.
- Small steps: a writing subagent task may change at most 2 files, named one by one (no directories);
  bigger tasks are refused with "split into steps". Plans (`/plan`, `/backlog-item`, the planner agent) come as
  numbered steps of 1-2 files that each leave the project building, with tests as their own step, run as a
  chain of workers. On psx-iso a single 4-file task left a compile error spread over the AI, the scene builder
  and the tests, and the fix broke the shared test setup.
- A subagent whose final message has no answer text (reasoning only) reports its last reasoning, marked as such, instead of "no output".
- Writing subagents run one at a time (also across parallel tool calls); read-only scouts stay parallel. Two writers at once fail each other's checks.
- Two check levels: `.pi/check-fast` (build/validate) after every step; `.pi/check` (full tests) once before
  the prompt finishes while files are unverified, at most 2 fix rounds, before the review. A full check marks
  the files it covered as verified, pass or fail (a failure is reported); files changed after it started stay
  unverified, also into the next prompt. Without a check-fast, `.pi/check` runs per step as before.
  Changes to docs only (`.md`, `.txt`) don't trigger it. Before the prompt's first code change (a writing step or
  a main-session edit) the full check runs once as a baseline; a failure with no new error lines is reported as
  "still fails as before, tell the user" instead of a fix round. On psx-iso a known red PlayMode test sent the
  agent to fix crawler code after it had only added an ASSETS.md row.
- Waiting runs: while a subagent waits with NEEDS, other runs' checks are deferred (its half-done change would fail them) and recorded with their command. When the last waiting run's resume ends (whatever its outcome), each deferred check runs once and its result comes back in that resume's tool result (reported, not retried). A prompt that would finish with runs still waiting is sent back once to resume them.
- Grounding: a plan-only prompt that finishes without having looked at code (no non-Markdown read, search or subagent) is sent back once.
- Path check: before a prompt finishes, file paths in the final answer that are presented as existing (not on a "new"/"create" line) and do not exist send the session back once to look at the code. On psx-iso a plan read only GAME.md and BACKLOG.md and named wrong paths.
  Paths inside code blocks (command outputs) and placeholders (`<audio>/Music/x.wav`) are skipped; the message says it is automatic, not the user's (psx-iso: the agent answered a misfire on a future loop file with "good catch" and read the music code).
- Correction turns (path check, full check, review) end with "give your whole answer again": the user reads only the last message, and a correction answer had replaced the music-takes hand-off.
- Main-session edit guard: an edit with old text = new text is refused ("to look, use read"); after 3 failed edits in a row to one file, edits to it are blocked for the prompt. Flint used no-op edits to look at a `.prov.json`, then corrupted it in a 50-edit loop (2026-10-27).
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
- Narrow task: the extension builds the task message (GOAL, DONE WHEN, WRITABLE SCOPE, INPUTS, CHECK, SHARED NOTES, CODE QUESTIONS, UNITY FILES in Unity projects, REPORT FORMAT).
  Each subagent inherits the main session's model, because llama-swap holds only one chat model and a different
  one would swap (a reload, and the main session's prompt cache is gone). The exceptions are pinned with `model:`
  or escalated: `looker` runs on qwen3.6 (vision); `reviewer` runs on qwen3.8-27b-gsq, a different model family
  from Flint's workers (two swaps per review, at most 2 reviews per prompt); and a step that still fails is taken
  over by the escalation model, the same qwen3.8-27b-gsq:
  - check: after the 2nd failed retry, it resumes the worker's session (it sees what was tried) for one more
    attempt, told to look at the diff and find the cause instead of repeating the fixes. The tool result says
    `escalated: ... retry 3 on qwen3.8-27b-gsq`. A step already on that model fails after retry 2 as before.
  - overflow: retry 2 (below).
- Scope enforcement:
  - `edit`/`write` calls outside the scope are blocked.
  - After every run, a git check undoes changes outside the scope, including bash writes. Tracked files are
    restored and new files are moved to `<run>/quarantine/`. Files that were already dirty before the run and
    weren't touched are left alone. Build artefacts (`__pycache__`, `node_modules`, `Library`...) and Unity-generated `.meta` files next to an existing asset are ignored. Quarantine moves work across filesystems, and a failing scope check is reported instead of crashing pi.
  - Concurrent parallel runs don't undo each other's in-scope work.
  - Subagents can't change git state: commit, reset, checkout, stash, add, rebase, push and the like are blocked
    in bash. If HEAD moves anyway (during the run or its check), it is not undone (a commit from another terminal looks the same); the report
    says so, and the files in those commits are not scope-checked.
  - Package installs that write outside the project are blocked in bash: `pip install` (also `python -m pip`,
    `uv pip install`), `pipx install`, `uv tool install`, `cargo install`, `gem install`, `go install`, `npm i -g`,
    `yarn global add`, also after `sudo`, env prefixes, `&&` or `;`. The scope check only sees the git tree, and a
    worker once ran `uv pip install pytest --target ~/.local/...`. A missing tool is reported, or asked for with
    `request`; `npm install`, `yarn`, `uv sync`/`uv add` and `dotnet restore` stay allowed. The main session isn't
    guarded, since it answers those requests.
  - Outside git, only the edit/write guard applies. `<file>.meta` belongs to `<file>`'s scope (Unity).
  - Other runs' internals are not inputs: reading their session logs (`*.jsonl`), prompts (`*.prompt.md`) or
    request files in the handoff area is blocked (read or bash); reports (`*.md`) and `notes.md` stay readable.
  - No disk-wide searches: `grep -r`, `rg`, `ag`, `find`, `fd`, `locate` (bash) and the grep/find tools are blocked
    when the target is above the project root (`/`, `/home`, `~`, `/tmp` for a project in /tmp), a top-level
    directory (`/var`, `/usr`...) or the handoff area (except a named report). A specific path elsewhere (a
    library in site-packages or node_modules) is allowed. Found 2026-10-24: a Flint worker puzzled by a rigged
    check read another run's session log and grepped /tmp, /home and /var for 20 minutes until its context was
    compacted; after the /tmp block, a test agent tried `grep -r /tmp/pi-handoff` instead.
- Out of scope: the subagent calls `request {need, why}` and stops (`terminate`). The main session dispatches a
  helper with `for: "<id>"` (or makes a one-line edit itself), then sends `resume: "<id>"`. The resumed subagent
  is told where the helper's report is.

**Handoff area**: `$PI_HANDOFF_ROOT` (default `/tmp/pi-handoff`), one folder per main session, holding
`NN-agent.md` (the report, written by the extension), `NN-agent.jsonl` (the session, for resume; `NN-agent.rN.jsonl`
for overflow retries), `NN-agent.prompt.md`, `notes.md`, `image-N.*` and `quarantine/`. Only paths go through the
main session.
- Shared notes: subagents pass facts to each other without the main session relaying them. Each task message names
  `notes.md` (with its fact count); the `note` tool appends one line, `- [pN NN-agent] fact`, where N is the user
  prompt that wrote it. The task message counts this prompt's facts and earlier prompts' separately and says the
  earlier ones may be outdated if their files changed since. The main session only gets the path. A plain file was chosen over a memory/blackboard MCP server: no extra tool schemas in every local
  model's prompt, no service, handoff reads already budgeted, and readable when a run goes wrong.
- `inputs` take run ids (`"01-scout"` or `"@01-scout"`) as well as paths; ids resolve to that run's report.
- Images: a prompt with images to a text-only session model (Flint) would lose them. The `input` hook saves them as
  `image-N.*` and tells the main session to ask the `looker` agent (qwen3.6, read-only) about them. The session
  keeps its model; only the looker's run swaps.

**Overflow recovery** (the main session never sees the attempts):
- An overflow is a subagent launch that ended with its thinking cut by the budget on the answer turn (llama-server
  marks the cut with `--reasoning-budget-message`, "Thinking budget reached..."), ran out of output tokens
  (stopReason length), or was compacted (`compaction_start` in its event stream). Cuts on tool-calling turns do not
  count, however many: qwen3.6 at its low budget is cut on most turns and still gets on with the work.
- Loops are stopped while running: the same tool call with the same arguments 4 times in a row, or 120 turns.
  Found with thinking budget 0 on Flint: 381 reads of the same (then missing) notes file in 10 minutes. A run that
  calls `request` more than 3 times is a loop too (the same test asked for its own task 13 times, and the main
  session resumed it each time without a helper); unlike other resumed runs it gets the fresh retries. A resume
  with no helper report and no message now says "not answered", not "handled".
- An overflow is not retried as is. A fresh session gets the task again, reformulated: what ran out, where it
  stalled (the end of its reasoning), sub-steps first and tools instead of long reasoning (or "read less" for
  context, "change approach" for loops), notes for facts, `git diff` first for a half-done change, and a
  `REMAINING:` line if the task does not fit. Retry 1 thinks one level higher; retry 2 also switches to the
  escalation model (`escalationModel`, default `mars/qwen3.8-27b-gsq`, another model family; `.pi/harness.json` or
  `PI_ESCALATION_MODEL`, `""` turns it off). Still overflowing: a report written under a budget cut stays `done` with a warning, anything
  else is `failed` with "split this goal" (and its REMAINING line).
- Only unusable outcomes are retried: no answer at all, output tokens running out, or a loop. A finished answer whose
  last turn was cut, or whose run compacted on the way, is kept as `done` with a warning (a writer's check judges
  it anyway). On psx-iso (2026-10-24) the stricter first version threw away a complete scout report and a complete
  4-step plan, 30 minutes of retries, and the run was aborted. A resumed run is not restarted unless it loops.
- Output cap: each read/bash/grep/find/ls result of a subagent is cut at 12 KB with a note to read a line range or
  narrow the command (images pass whole). The same psx-iso scout `cat`-ed whole files to 59k tokens in 31 turns.
- Seen in tests (2026-10-24, synthetic repo): Flint with no shell and a cut turn moved its working into the answer
  text (21k characters) and was right (2531); with a shell it computed instead of reasoning (55005, three methods).
  Neither is retried. With budget 0 on a no-shell agent: cut on its answer, retry 1 (low, Flint) looped on a read and
  was stopped, retry 2 (medium, qwen3.8-27b-gsq) answered 49 (correct) in 2 min; the main session got 3 lines.
- Overflow notes name models without the provider: `mars/flint-40b-a2.5b` read as a missing file to the path check,
  which sent the main session searching the disk for it.
- Images at session start: pi itself picks a vision model (qwen3.6) for a first prompt with images. The hook is for
  images in a session already on Flint (tested with `--model mars/flint-40b-a2.5b @ocr.png`: saved, asked the
  looker, correct transcription).

Prompts: `/implement` (scout, planner, then one worker per plan step with that step's files as scope), `/scout-and-plan`, `/plan`, `/implement-and-review`.

## Web: searxng on jupiter

`extensions/web/` adds two tools:
- `web_search`: the searxng JSON API at `https://jupiter.internal:8888` (override with `PI_SEARXNG_URL`).
  It returns up to 8 results (max 15): title, URL and a snippet of about 300 chars, plus searxng answers and infoboxes.
- `web_fetch`: one http(s) page as text. HTML loses scripts, styles and page chrome. Output comes 12 KB at a time,
  and an `offset` continues from where the last call stopped.

searxng is served by the homelab chart `openwebui-searxng` behind Traefik's `searxng` entrypoint, with TLS only.
Plain `http://jupiter:8888` returns Traefik's 404. The certificate comes from step-ca, so requests also trust
the system CA store (`tls.getCACertificates("system")`). Node's bundled list alone fails with
`UNABLE_TO_GET_ISSUER_CERT_LOCALLY`.

The main session has the tools, and so do scout and planner (through their `tools:`); workers, reviewers and the
looker do not. The caps are in the tools themselves, because the main session's output budget does not count them.
If jupiter is down, the tools return a one-line error and the agent continues without the web.

## Code questions: `cq`

`bin/cq` answers code questions from an index instead of reading files. The subagent extension puts `pi/bin` on
the PATH of the main session and every subagent, and the scout, planner, worker and reviewer prompts say to ask it
before reading a file.

| Command | Tool | Notes |
|---|---|---|
| `cq outline <file>` | universal-ctags | symbols with line numbers; then read only those lines |
| `cq defs <Name> [path]` | universal-ctags (`ctags -x`, no tags file) | all overloads and overrides; tracked and new files |
| `cq refs <Name> [path]` | ast-grep (C# identifiers) + `rg -w` (other files) | fields and properties too; Unity scenes via `rg` |
| `cq callers/callees <Name>` | codegraph (`.codegraph/` SQLite index) | method calls only, not property or field reads |

- Output is cut at `CQ_MAX` lines (40) with a count of the rest; exit 1 = nothing found.
- codegraph's index is built on first use, synced before each query (~0.3 s, so a worker's edit from the step before
  is seen without a watcher), listed in `.git/info/exclude`, and ignored by the scope check. Telemetry is off
  (`codegraph telemetry off`, and `DO_NOT_TRACK=1` in `cq`). Install it with npm, not its installer script, which
  writes into CLAUDE.md/AGENTS.md and registers MCP servers.
- Checked on psx-iso (Unity C#, 37 files, 2026-10-24) against `git grep`: defs exact (4 `Interact` overloads and
  overrides), refs 12/12 for `IsDead`, callers right (also through `StartCoroutine(Attack())`, method vs enum member
  told apart). codegraph alone found 0 of 11 `IsDead` uses, hence `refs` on ast-grep.
- Scout A/B on the psx-iso clone (2026-10-24, one run each, the same 4-question goal): old prompts, no `cq`: stopped
  after 35 min and 99 turns with no answer (compacted twice, re-reading files); new prompts (answer what was asked,
  line ranges, 12 KB cap) with a broken `cq outline`: 12 min, 43 turns, 1 compaction; new prompts with `cq` fixed:
  8.5 min, 31 turns, no compaction, complete report, and 0 `cq` calls. The gain came from the prompts and caps; a
  system-prompt mention alone did not get Flint to use `cq` (its first try had failed: `outline` took one file).
  With a `CODE QUESTIONS:` line in every task message (agents with bash) and a one-line hint on wasteful reads (a
  source file read past 150 lines without a range, a recursive grep, a long `cat`/`sed`; at most 3 per run): 7.5 min,
  28 turns, peak 49k (58k before), 4 `cq` calls (outlines of the 4 key files first; Unity YAML still by grep, which
  ctags does not parse). The planner got `bash` (read-only by prompt and scope check) so it can run `cq` too.
- Chosen after a docs and reviews pass (2026-10-24): graphify ranked last (C# overloads merged into one node, delegates
  dropped; an independent test found ~5% on-target output against `rg`); Roslyn tools need a .sln and are MCP-only;
  CLIs instead of MCP servers keep tool schemas out of every small-model prompt.

## Thinking per task

The harness picks the thinking level per task, and the level picks a reasoning budget:
- Agents declare `thinking:` in their frontmatter: scout `low` (at `minimal` every Flint scout ran out of budget mid-answer), worker `low`, planner and reviewer `medium`;
  the main session keeps its own level. A worker retried after a failed check thinks one level higher (up to
  `high`).
- An agent with its own `model:` never gets the main session's level: it gets its frontmatter `thinking:`, or
  nothing (its model's `:level` suffix applies). A retry raises from its own level (frontmatter, else the
  `:level` suffix); with neither, the retry passes nothing either.
- `models.json` sends `thinking_budget_tokens` per level (`samplingParamsByThinkingLevel`): minimal 256, low
  1024, medium 3072, high 8192, xhigh 16384. Flint (default model) gets minimal 512, low 3072, medium 6144, high
  12288, xhigh 16384, off 0: a budget only caps, and Flint's own lengths on bench/suite.py are short but long-tailed
  (median 582, code median 2363, max 7299 apart from one runaway), so 1024 would cut most code steps. Its template
  ignores `reasoning_effort`; the budget is the only control (qwen3.8-27b-gsq 8192: xhigh stays at or below half of `maxTokens`, so
  the answer still fits). qwen3.6 ignores `reasoning_effort` (minimal reasoned the most), so the budget is
  the real control.
- pi also sends the level as top-level `reasoning_effort`. The Qwen3.8 template knows only low, medium and xhigh
  (default when unset; `none` turns thinking off) and returns a 500 for `minimal`, so qwen3.8-27b-gsq has a
  `thinkingLevelMap`: off none, minimal low, low low, medium medium, high and xhigh xhigh. Without it, off
  still thought at xhigh and xhigh was clamped to high. Low or medium beat the xhigh default on bench/suite.py.
- At the limit llama-server closes the reasoning (a hard cut; the model does not plan for it) after injecting
  `--reasoning-budget-message` (llama-swap `chat_common`): "Thinking budget reached. Act now with what I have."

Synthetic shop task (2026-10-18, qwen3.6): reasoning across subagents dropped from 23.6k to 16.1k chars (max per
turn 5.5k to 3.5k), with the same result, no budget cuts and the same wall time (~9 min). The automatic review
fired for the first time: review 1 found a missing bounds check, which was fixed; review 2 raised out-of-scope
nits and the 2-review cap ended it.

## Compaction

`-np 2 --kv-unified` means the parent and a subagent share one 131072-token KV pool (Flint and qwen3.6 alike). The parent therefore compacts
at about 64k (`reserveTokens` 65536) and keeps 16k. qwen3.8-27b-gsq (65536 window) overrides this: reserve 16384 and
keep 12000, with `maxTokens` 16384. pi uses the GSQ quant of Qwen3.8 (64k instead of stock's 48k, bench/gsq-2026-10-24.md);
stock `qwen3.8-27b` stays in llama-swap for other clients.

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

## Notes

- `pi -p --mode json` waits on stdin when it isn't a terminal, so run it with `</dev/null` from scripts and background jobs.
