/**
 * Subagent delegation for local models with small context windows.
 *
 * Main session (orchestrator): gets the `subagent` tool; its own read/bash output is capped so it cannot
 * fill its context. Subagents (PI_SUBAGENT=1): get a narrow task, a writable scope, and a `request` tool.
 * Out-of-scope needs stop the subagent; the main agent dispatches a helper with `for`, then resumes it.
 *
 * Handoff area: every run writes to $PI_HANDOFF_ROOT/<run>/ (default /tmp/pi-handoff):
 *   NN-agent.md            final report (written by this extension, not the model)
 *   NN-agent.jsonl         subagent session, kept so the subagent can be resumed
 *   NN-agent.request.json  written by the subagent's `request` tool
 *   notes.md               one-line facts subagents share with each other (`note` tool); the main agent only gets the path
 *   image-N.png            images attached to a prompt while the session model cannot see them (for the looker agent)
 * Only paths and one-line summaries go back to the main agent.
 */

import { execFileSync, spawn } from "node:child_process";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { Message } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { type AgentConfig, discoverAgents } from "./agents.ts";

// ---------- per-project harness config ----------

/**
 * `.pi/harness.json` in the session directory or a parent (up to the git root), e.g.
 *   { "enabled": false }                                  turn the harness off for this project
 *   { "maxStepFiles": 3, "turnBudgetKB": 64, "maxReviews": 1, "allowNoGit": true,
 *     "check": "make lint", "fullCheck": "make test",       overrides (commands run in the project root)
 *     "escalationModel": "mars/qwen3.8-27b-gsq" }
 * PI_HARNESS=off turns it off for one session.
 */
interface HarnessConfig {
	enabled?: boolean;
	maxStepFiles?: number;
	turnBudgetKB?: number;
	maxReviews?: number;
	allowNoGit?: boolean;
	check?: string;
	fullCheck?: string;
	escalationModel?: string; // provider/id that takes over a failing step (overflow retry 2, check retry 3); "" = off
	root?: string; // directory holding .pi/harness.json
	error?: string;
}

function gitRoot(cwd: string): string | undefined {
	try {
		return execFileSync("git", ["rev-parse", "--show-toplevel"], { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
	} catch {
		return undefined;
	}
}

function loadHarnessConfig(cwd: string): HarnessConfig {
	const stop = gitRoot(cwd) ?? path.parse(path.resolve(cwd)).root;
	for (let d = path.resolve(cwd); ; d = path.dirname(d)) {
		const f = path.join(d, ".pi", "harness.json");
		if (fs.existsSync(f)) {
			try {
				return { ...(JSON.parse(fs.readFileSync(f, "utf8")) as HarnessConfig), root: d };
			} catch (e) {
				return { root: d, error: `${f}: ${(e as Error).message}` };
			}
		}
		if (d === stop || d === path.dirname(d)) return {};
	}
}

const HC = loadHarnessConfig(process.cwd());
const HARNESS_ON = process.env.PI_HARNESS !== "off" && HC.enabled !== false;
const num = (v: unknown, d: number) => (typeof v === "number" && v > 0 ? v : d);

const IS_CHILD = process.env.PI_SUBAGENT === "1";
const HANDOFF_ROOT = process.env.PI_HANDOFF_ROOT || "/tmp/pi-handoff";
const MAX_CONCURRENCY = 2; // llama-swap serves -np 2 slots per model
const MAX_PARALLEL_TASKS = 4;
const SUMMARY_CAP = 600; // chars of the report returned inline to the main agent
const PARENT_READ_MAX_LINES = 200; // main agent: read calls are clamped to this many lines
const PARENT_RESULT_CAP = 6 * 1024; // main agent: bytes per read/bash/grep/find/ls result
const PARENT_TURN_BUDGET = num(HC.turnBudgetKB, 48) * 1024; // main agent: direct output bytes per user prompt, then reads are blocked
const PARENT_BASH_AFTER_BUDGET = 1024; // check commands still work after the budget, with this much output
const PARENT_MAX_EDIT_FILES = 2; // main agent: distinct files it may edit/write per user prompt; more = delegate
const PARENT_SKILL_READ_CAP = 16 * 1024; // main agent: SKILL.md files are instructions, read whole, not budgeted
const PARENT_HANDOFF_READ_CAP = 4 * 1024; // main agent: handoff reports stay readable after the budget, capped
const MAX_REVIEWS = num(HC.maxReviews, 2); // automatic reviews per user prompt (a second one after fixes)
const MAX_STEP_FILES = num(HC.maxStepFiles, 2); // files a writing subagent may change in one task: bigger changes are split into steps
const MAX_FULL_CHECKS = 2; // full-check runs (and fix rounds) before the prompt finishes
const CHECK_RETRIES = 2; // a failed `check` resumes the subagent with the failure this many times
const CHECK_TIMEOUT_MS = Number(process.env.PI_SUBAGENT_CHECK_TIMEOUT_MS) || 30 * 60 * 1000;
const CHECK_TAIL_LINES = 15;
// Overflow: a subagent whose thinking budget was cut while it wrote its answer, that ran out of output tokens, or whose
// own context was compacted, is retried with a reformulated task, at most this many times. Cuts on tool-calling turns
// do not count, however many: qwen3.6 at its low budget is cut on most turns and still gets on with the work.
const MAX_RECOVERIES = 2;
// a launch that loops never ends on its own: it is stopped while running (thinking budget 0 on Flint: 381 reads of the
// same missing file in 10 minutes) and handled like an overflow
const KILL_REPEATS = 4; // the same tool call with the same arguments this many times in a row
const KILL_TURNS = 120; // turns in one launch
// a run that keeps asking the main agent (request -> resume -> request) is looping too: with thinking budget 0, Flint
// asked for its own task ("give me the last two digits of 7^2026") 13 times
const MAX_REQUESTS = 3;
// start of llama-swap's --reasoning-budget-message (chat_common): llama-server appends it to the reasoning it cuts
const BUDGET_MARK = "Thinking budget reached";
// Escalation: another model family takes over a step its model could not do, after the second overflow retry and after
// the second failed check retry. A model that loops on a task tends to loop on it again (bench/suite.py: qwen3.6 thought
// 16k+ tokens where Flint and Qwen3.8 needed 1-3k). The reviewer agent runs on the same model.
const ESCALATION_MODEL = process.env.PI_ESCALATION_MODEL ?? HC.escalationModel ?? "mars/qwen3.8-27b-gsq";
const modelName = (m?: string) => m?.split("/").pop() ?? "session model"; // no provider: "mars/x" reads as a file path

// after the budget, bash is limited to checks (no sed/cat paging through files)
const CHECK_CORE = /^(\.\/)?(Tools\/check|git (status|diff --stat|log --oneline)|python3? -m pytest|pytest|npm (test|run (test|check|lint))|cargo (test|check)|go test|dotnet test|make (test|check))\b[^;&|<>]*$/;

/** True for a check command, also when wrapped: `cd X &&`, `timeout N`, `bash`, `2>&1`, `> log`, `| tail -N`. */
function isCheckCommand(cmd: string, extra: Set<string>): boolean {
	if (extra.has(cmd.trim())) return true;
	let c = cmd.trim();
	c = c.replace(/^cd\s+(?:'[^']*'|\\.|[^\s'\\])+\s*&&\s*/, ""); // the path may be shq-quoted
	c = c.replace(/^timeout\s+\d+[smh]?\s+/, "");
	c = c.replace(/^(bash|sh)\s+/, "");
	for (let prev = ""; prev !== c; ) {
		prev = c;
		c = c.replace(/\s*(2>&1|>\s*\S+|\|\s*(tail|head)(\s+-n)?(\s+-?\d+)?)\s*$/, "");
	}
	return extra.has(c) || CHECK_CORE.test(c);
}
const ALL_TOOLS = ["read", "bash", "edit", "write", "grep", "find", "ls"];
const LEVELS = ["off", "minimal", "low", "medium", "high", "xhigh"]; // pi's levels (it also has max)
// a retry raises up to `high` (xhigh is only set explicitly); a level at or above it, or unknown, stays as is
const RAISE_CAP = LEVELS.indexOf("high");
const raiseLevel = (l?: string) => {
	const i = LEVELS.indexOf(l ?? "low");
	return i < 0 || i >= RAISE_CAP ? (l ?? "medium") : LEVELS[i + 1];
};
/** An agent's own level: frontmatter `thinking:`, else the `:level` suffix of its own `model:` (provider/id:high). */
const ownLevel = (a: AgentConfig) => {
	const suffix = a.model?.match(/:(\w+)$/)?.[1];
	return a.thinking ?? (suffix && (LEVELS.includes(suffix) || suffix === "max") ? suffix : undefined);
};
/** An agent with edit or write (no `tools:` list = all tools). */
const agentWrites = (a?: AgentConfig) => !a?.tools || a.tools.some((t) => t === "edit" || t === "write");
const PLAN_ONLY = /\[plan-only\]/i;
const PLAN_ONLY_REASON =
	"This prompt is plan-only: no edits, no writing subagents. Present the plan (files and why) and stop; the user replies with OK to implement.";

// ---------- shared helpers ----------

/** Single-quoted for bash: nothing inside expands ($, backticks, \); a ' becomes '\''. */
const shq = (s: string) => `'${s.replace(/'/g, `'\\''`)}'`;

function textOf(content: { type: string; text?: string }[] | undefined): string {
	return (content ?? []).filter((c) => c.type === "text").map((c) => c.text ?? "").join("\n");
}

function finalText(messages: Message[]): string {
	for (let i = messages.length - 1; i >= 0; i--) {
		const m = messages[i];
		if (m.role === "assistant") {
			const t = textOf(m.content as any).trim();
			if (t) return t;
		}
	}
	// a local model sometimes ends with reasoning only and no answer text (psx-iso: "[18-worker] ERROR: no output"
	// although its reasoning held the result): use the last reasoning, marked as such
	for (let i = messages.length - 1; i >= 0; i--) {
		const m = messages[i];
		if (m.role !== "assistant") continue;
		const thought = ((m.content as any[]) ?? [])
			.filter((c) => c.type === "thinking")
			.map((c) => c.thinking ?? "")
			.join("\n")
			.trim();
		if (thought) return `(no answer text; last reasoning follows)\n${thought.slice(-3000)}`;
	}
	return "";
}

const thoughtOf = (m: Message) =>
	((m.content as any[]) ?? [])
		.filter((c) => c.type === "thinking")
		.map((c) => c.thinking ?? "")
		.join("\n");

/**
 * Did this launch end badly although it may have produced text? Thinking cut by the budget on the final answer (the
 * answer was forced: "act now with what I have"), output tokens running out (stopReason length), its context
 * compacted (it lost what it had read), or stopped for looping. Budget cuts on tool-calling turns are normal and not
 * counted: the model acts and goes on.
 */
function overflowOf(messages: Message[], compacted: boolean, looped?: string): Overflow | undefined {
	const asst = messages.filter((m) => m.role === "assistant");
	if (!asst.length) return undefined;
	const last = asst.at(-1)!;
	const lastThought = [...asst].reverse().map(thoughtOf).find((t) => t.trim()) ?? "";
	const tail = lastThought.split(BUDGET_MARK)[0].trim().slice(-1200);
	if (looped) return { kind: "loop", why: looped, tail };
	if (asst.some((m) => (m as any).stopReason === "length")) return { kind: "length", why: "ran out of output tokens", tail };
	if (compacted) return { kind: "context", why: "filled its context (it was compacted and lost what it had read)", tail };
	if (thoughtOf(last).includes(BUDGET_MARK)) return { kind: "budget", why: "ran out of thinking budget while writing its answer", tail };
	return undefined;
}

/**
 * Files for a "@previous" scope: the `FILES:` line of the latest report that has one, else every existing
 * file path mentioned in the latest report (models do not always follow the report format).
 */
function filesFromReports(reports: string[], cwd: string): string[] {
	for (const r of [...reports].reverse()) {
		const line = fs.readFileSync(r, "utf8").match(/^\**FILES:?\**:?(.*)$/m)?.[1];
		if (line) return line.split(",").map((f) => f.trim().replace(/^`|`$/g, "").replace(/:\d+(-\d+)?$/, "")).filter((f) => f && f.toLowerCase() !== "none");
	}
	const last = reports.at(-1);
	if (!last) return [];
	const found = new Set<string>();
	for (const m of fs.readFileSync(last, "utf8").matchAll(/[\w./-]+\.\w+/g)) {
		const abs = path.resolve(cwd, m[0]);
		if (abs.startsWith(cwd + path.sep) && fs.existsSync(abs) && fs.statSync(abs).isFile()) found.add(path.relative(cwd, abs));
	}
	return [...found];
}

/**
 * Which report a subagent goal asks for, when it only asks for one: undefined = real work; `file` = a named handoff
 * file; `id` = that run's report; neither = the latest report. Needs report intent (a handoff file, "handoff", or
 * "report" in a phrase about a subagent's report): a run id alone is not enough ("read the function 03-worker
 * changed" is real work). With several run ids, the one next to "report" is served, or none.
 */
function reportGoal(goal: string, ids: string[], handoffRoot: string): { file?: string; id?: string } | undefined {
	const g = goal.trim();
	if (g.length > 200 || !/\b(return|show|give|read|print|output|fetch|get)\b/i.test(g)) return undefined;
	// any file named in the goal must be a handoff report; "read the content of README.md" is real work
	const files = g.match(/[\w./~-]*\.\w{1,5}\b/g) ?? [];
	const root = path.resolve(handoffRoot) + path.sep;
	if (files.some((f) => !f.startsWith(root))) return undefined;
	const named = ids.filter((id) => new RegExp(`\\b${id}\\b`).test(g));
	// "01-scout's report", "01-scout report", "the report of/from/by 01-scout"
	const nextTo = named.filter((id) =>
		new RegExp(`\\b${id}(?:'s)?\\s+(?:full\\s+|final\\s+)?reports?\\b|\\breports?\\s+(?:of|from|by)\\s+${id}\\b`, "i").test(g),
	);
	const aboutReport =
		files.length > 0 ||
		nextTo.length > 0 ||
		/\bhandoff\b/i.test(g) ||
		/\b(last|latest|previous|its|their|subagent'?s?|scout'?s?|worker'?s?|reviewer'?s?|planner'?s?)\s+(full\s+|final\s+)?reports?\b/i.test(g) ||
		// "the report" only as a noun phrase of its own, not "the report generator in src/"
		/\b(the|that)\s+(full\s+|final\s+)?report\b(?=\s*([.,;:!?]|$|\s+(from|by)\b))/i.test(g);
	if (!aboutReport) return undefined;
	if (files.length) return { file: files[0] };
	if (nextTo.length === 1) return { id: nextTo[0] };
	if (named.length === 1 && nextTo.length === 0) return { id: named[0] };
	return named.length ? undefined : {}; // several ids and none (or several) next to "report": ambiguous
}

// untracked build artefacts a subagent's check run may create; never treated as scope violations
const ARTEFACT = /(^|\/)(__pycache__|\.pytest_cache|\.mypy_cache|node_modules|\.venv|target|build|dist|obj|Library|Temp|Logs|\.godot)(\/|$)|\.pyc$/;

/** `.pi/<name>` in the task cwd or a parent (up to the git root), run as `bash <file>`. */
function projectFile(cwd: string, name: string): string | undefined {
	let root = "/";
	try {
		root = git(cwd, ["rev-parse", "--show-toplevel"]).trim();
	} catch {}
	for (let d = path.resolve(cwd); ; d = path.dirname(d)) {
		const f = path.join(d, ".pi", name);
		if (fs.existsSync(f) && fs.statSync(f).isFile()) return `bash ${shq(f)}`;
		if (d === root || d === path.dirname(d)) return undefined;
	}
}

interface Detected {
	fast?: string; // build / compile / typecheck: after every step
	full?: string; // tests: once at the end
	source: string; // what it was derived from, told to the main agent
}

const detectCache = new Map<string, Detected | undefined>();

/**
 * Checks for a project without .pi/check, from its standard files. Commands run in the project root. Order:
 * harness.json overrides, then package.json, Cargo.toml, go.mod, .sln/.csproj, Python (pytest), Makefile, and
 * last, with none of these giving a check, tracked .py files.
 */
function detectChecks(cwd: string): Detected | undefined {
	const root = gitRoot(cwd) ?? path.resolve(cwd);
	if (detectCache.has(root)) return detectCache.get(root);
	const has = (f: string) => fs.existsSync(path.join(root, f));
	const inRoot = (c: string) => `cd ${shq(root)} && ${c}`;
	const python = (what: string): Detected => {
		const tests = has("tests") || has("test") || has("pytest.ini") || fs.readdirSync(root).some((f) => /^test_.*\.py$|_test\.py$/.test(f));
		return {
			fast: inRoot("python3 -m compileall -q -x '/(\\.venv|venv|node_modules|\\.git)/' ."),
			full: tests ? inRoot("python3 -m pytest -q") : undefined,
			source: tests ? `${what} (compileall, pytest)` : `${what} (compileall)`,
		};
	};
	let d: Detected | undefined;
	if (HC.check || HC.fullCheck) {
		d = { fast: HC.check && inRoot(HC.check), full: HC.fullCheck && inRoot(HC.fullCheck), source: ".pi/harness.json" };
	} else if (has("package.json")) {
		let scripts: Record<string, string> = {};
		try {
			scripts = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).scripts ?? {};
		} catch {}
		const pm = has("pnpm-lock.yaml") ? "pnpm" : has("yarn.lock") ? "yarn" : has("bun.lockb") || has("bun.lock") ? "bun" : "npm";
		const fastScript = ["typecheck", "build", "lint", "check"].find((k) => scripts[k]);
		const testScript = scripts.test && !/no test specified/.test(scripts.test) ? "test" : undefined;
		if (fastScript || testScript) {
			d = {
				fast: fastScript && inRoot(`${pm} run ${fastScript}`),
				full: testScript && inRoot(`${pm} run ${testScript}`),
				source: `package.json scripts (${[fastScript, testScript].filter(Boolean).join(", ")})`,
			};
		}
	} else if (has("Cargo.toml")) {
		d = { fast: inRoot("cargo check --quiet"), full: inRoot("cargo test --quiet"), source: "Cargo.toml" };
	} else if (has("go.mod")) {
		d = { fast: inRoot("go build ./... && go vet ./..."), full: inRoot("go test ./..."), source: "go.mod" };
	} else if (fs.readdirSync(root).some((f) => /\.(sln|slnx|csproj|fsproj)$/.test(f))) {
		d = { fast: inRoot("dotnet build --nologo -v q"), full: inRoot("dotnet test --nologo -v q"), source: ".NET solution/project" };
	} else if (has("pyproject.toml") || has("setup.py") || has("pytest.ini") || has("requirements.txt")) {
		d = python("Python project");
	} else if (has("Makefile")) {
		const mk = fs.readFileSync(path.join(root, "Makefile"), "utf8");
		const target = (t: string) => new RegExp(`^${t}\\s*:`, "m").test(mk);
		if (target("check") || target("test") || target("build")) {
			d = {
				fast: target("build") ? inRoot("make build") : target("check") ? inRoot("make check") : undefined,
				full: target("test") ? inRoot("make test") : target("check") && target("build") ? inRoot("make check") : undefined,
				source: "Makefile",
			};
		}
	}
	if (d && !d.fast && !d.full) d = undefined;
	// a Python repo without project files: tracked .py files (git's index, no tree walk), else .py files in the root
	if (!d) {
		let py = false;
		try {
			py = git(root, ["ls-files", "--", "*.py"]).length > 0;
		} catch {
			py = fs.readdirSync(root).some((f) => f.endsWith(".py"));
		}
		if (py) d = python("Python files");
	}
	detectCache.set(root, d);
	return d;
}

/**
 * Check after every writing step: `.pi/check-fast` (compile, validate), else `.pi/check`, else a detected build
 * check (or detected tests when there is no build step).
 */
const projectCheck = (cwd: string) =>
	projectFile(cwd, "check-fast") ?? projectFile(cwd, "check") ?? detectChecks(cwd)?.fast ?? detectChecks(cwd)?.full;
/** Full check (tests) once before the prompt finishes; only when it differs from the per-step check. */
const projectFullCheck = (cwd: string) => {
	if (projectFile(cwd, "check-fast")) return projectFile(cwd, "check");
	if (projectFile(cwd, "check")) return undefined;
	const d = detectChecks(cwd);
	return d?.fast && d.full ? d.full : undefined;
};

interface CheckResult {
	code: number; // killed by a signal: 128 + its number, as bash reports it
	timedOut: boolean; // killed by us after CHECK_TIMEOUT_MS
	aborted: boolean; // killed by us because the step was aborted
	tail: string;
	out: string;
}

function runCheck(cmd: string, cwd: string, log: string, signal?: AbortSignal): Promise<CheckResult> {
	return new Promise((resolve) => {
		const proc = spawn("bash", ["-c", cmd], { cwd, stdio: ["ignore", "pipe", "pipe"] });
		let out = "";
		let timedOut = false;
		let aborted = false;
		proc.stdout.on("data", (d) => (out += d.toString()));
		proc.stderr.on("data", (d) => (out += d.toString()));
		const onTimeout = () => ((timedOut = true), proc.kill("SIGTERM"));
		const onAbort = () => ((aborted = true), proc.kill("SIGTERM"));
		const timer = setTimeout(onTimeout, CHECK_TIMEOUT_MS);
		if (signal?.aborted) onAbort();
		else signal?.addEventListener("abort", onAbort, { once: true });
		proc.on("close", (code, sig) => {
			clearTimeout(timer);
			signal?.removeEventListener("abort", onAbort);
			fs.writeFileSync(log, out);
			const tail = out.trimEnd().split("\n").slice(-CHECK_TAIL_LINES).join("\n").slice(-1536);
			resolve({ code: code ?? 128 + ((sig && os.constants.signals[sig]) || 0), timedOut, aborted, tail, out });
		});
	});
}

/** A failure compareToBaseline can't compare: says it may predate the step, so the main agent doesn't blame the step. */
const uncomparableText = (r: CheckResult, log: string) =>
	`FAILED (exit ${r.code}), but it was already failing before this step without error lines, so it can't be compared; not retried. Log: ${log}\n    ${r.tail.split("\n").slice(-4).join("\n    ").slice(0, 600)}`;

/** How a failed check ended, for reports: "exit 2", "timed out after 30 min", "aborted". */
const exitText = (r: CheckResult) =>
	r.timedOut ? `timed out after ${Math.round(CHECK_TIMEOUT_MS / 60000)} min` : r.aborted ? "aborted" : `exit ${r.code}`;

/**
 * The error lines of a check's output, normalized so two runs compare: paths relative, line/column numbers,
 * durations and counts removed. Summary lines ("6 Error(s)", "Build FAILED", "failures=3") are dropped.
 */
function errorLines(out: string, cwd: string): Set<string> {
	const root = (gitRoot(cwd) ?? path.resolve(cwd)) + path.sep;
	const set = new Set<string>();
	for (let l of out.split("\n")) {
		if (!/\b(error|errors|fail|failed|failure|exception)\b|✗|✘/i.test(l)) continue;
		if (/^\s*\d+\s+error\(s\)|^\s*\d+\s+warning\(s\)|build failed|failures=\d+|check: exit|playmode: exit|^\s*(tests?|total).*\d+/i.test(l)) continue;
		l = l
			.split(root).join("")
			.replace(/\[[^\]]*\.(slnx?|csproj|fsproj)\]/g, "")
			.replace(/[(:]\d+(,\d+)?\)?:?/g, ":")
			.replace(/\b\d+(\.\d+)?\s*(ms|s|sec|seconds)\b/g, "")
			.replace(/\s+/g, " ")
			.trim();
		if (l) set.add(l);
	}
	return set;
}

/** The check's result before this prompt's first step, per cwd and check (baselineKey). */
interface Baseline {
	code: number;
	errors: Set<string>;
	timedOut: boolean; // its error lines are partial: nothing compares against it
}
const baselineKey = (cwd: string, check: string) => `${cwd}\0${check}`;

/**
 * A step's check against the red baseline. Only a check that exits with the baseline's own exit code is compared,
 * by error lines: a different code (a crash like 139, an OOM kill, a different failing stage) is a normal failure,
 * and so is a timed-out or aborted check. Limit: the same code with only known error lines passes, even if the
 * step broke something that prints no error line.
 * - pass: exit 0, or the same failure with no new error lines
 * - new: error lines that were not there before (newErrors), retried with only those
 * - uncomparable: the baseline failed with this code but printed no error lines, so there is nothing to compare
 *   against (a bare `exit 1` would always look like "no new failures"); it may predate the step, so not retried
 * - fail: anything else, a normal failure
 */
function compareToBaseline(
	res: CheckResult,
	base: Baseline | undefined,
	cwd: string,
): { verdict: "pass" | "new" | "uncomparable" | "fail"; newErrors?: string[]; why?: string } {
	if (res.timedOut || res.aborted) return { verdict: "fail", why: exitText(res) };
	if (res.code === 0) return { verdict: "pass" };
	if (!base || base.code === 0) return { verdict: "fail" };
	if (base.timedOut) return { verdict: "fail", why: "the baseline timed out, so there is nothing to compare against" };
	if (res.code !== base.code) return { verdict: "fail", why: `exit ${res.code}, the baseline failed with exit ${base.code}` };
	if (base.errors.size === 0) return { verdict: "uncomparable" };
	const now = errorLines(res.out, cwd);
	if (now.size === 0) return { verdict: "fail", why: "no error lines to compare" };
	const newErrors = [...now].filter((e) => !base.errors.has(e));
	return newErrors.length
		? { verdict: "new", newErrors }
		: { verdict: "pass", why: `baseline was already failing: ${base.errors.size} known errors before the first step, ${now.size} now` };
}

function inScope(file: string, cwd: string, scope: string[]): boolean {
	// Unity writes <asset>.meta next to every asset: it belongs to the asset's scope
	const abs = path.resolve(cwd, file).replace(/\.meta$/, "");
	return scope.some((s) => abs === s || abs.startsWith(s.endsWith(path.sep) ? s : s + path.sep));
}

// ---------- scope check by git (catches bash writes too) ----------

interface Snapshot {
	root: string;
	head: string; // HEAD before the run ("" in a repo without commits)
	dirty: Map<string, string | null>; // repo-relative path -> content before the run (null: did not exist)
}

function git(cwd: string, args: string[]): string {
	return execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
}

function changedPaths(root: string): string[] {
	// tracked changes plus untracked, non-ignored files; -z keeps odd file names intact
	const out = git(root, ["status", "--porcelain", "-z", "-uall"]);
	const paths: string[] = [];
	const parts = out.split("\0");
	for (let i = 0; i < parts.length; i++) {
		const e = parts[i];
		if (!e) continue;
		paths.push(e.slice(3));
		if (e[0] === "R" || e[0] === "C") i++; // rename/copy: next part is the source path
	}
	return paths;
}

function snapshot(cwd: string): Snapshot | undefined {
	try {
		const root = git(cwd, ["rev-parse", "--show-toplevel"]).trim();
		let head = "";
		try {
			head = git(root, ["rev-parse", "HEAD"]).trim();
		} catch {}
		const dirty = new Map<string, string | null>();
		for (const p of changedPaths(root)) {
			const abs = path.join(root, p);
			dirty.set(p, fs.existsSync(abs) && fs.statSync(abs).isFile() ? fs.readFileSync(abs, "latin1") : null);
		}
		return { root, head, dirty };
	} catch {
		return undefined; // not a git repo: only the edit/write guard applies
	}
}

const hash = (s: string | null) => (s === null ? "-" : crypto.createHash("sha1").update(s, "latin1").digest("hex"));

/** rename, or copy + unlink across filesystems (the handoff area is on /tmp, often tmpfs) */
function moveFile(src: string, dest: string) {
	try {
		fs.renameSync(src, dest);
	} catch (e) {
		if ((e as NodeJS.ErrnoException).code !== "EXDEV") throw e;
		fs.copyFileSync(src, dest);
		fs.unlinkSync(src);
	}
}

/** Files whose content differs from the snapshot (after scope enforcement): what the run really changed. */
function touchedFiles(snap: Snapshot): string[] {
	const out: string[] = [];
	for (const p of changedPaths(snap.root)) {
		const abs = path.join(snap.root, p);
		const now = fs.existsSync(abs) && fs.statSync(abs).isFile() ? fs.readFileSync(abs, "latin1") : null;
		if (!snap.dirty.has(p) || hash(snap.dirty.get(p)!) !== hash(now)) out.push(abs);
	}
	return out;
}

/**
 * Restores files changed outside scope since the snapshot; new untracked files are moved to quarantine.
 * `headMoved`: HEAD changed meanwhile. Not undone: a commit from another terminal (the user's) looks the same as
 * one the run made, and the changes it holds are hidden from this check.
 */
function enforceScope(snap: Snapshot, scope: string[], cwd: string, quarantine: string): { fixed: string[]; headMoved?: string } {
	const fixed: string[] = [];
	let headMoved: string | undefined;
	let tracked: Set<string>;
	try {
		const head = snap.head ? git(snap.root, ["rev-parse", "HEAD"]).trim() : "";
		if (snap.head && head !== snap.head) headMoved = `${snap.head.slice(0, 8)} -> ${head.slice(0, 8)}`;
		tracked = new Set(snap.head ? git(snap.root, ["ls-tree", "-r", "-z", "--name-only", "HEAD"]).split("\0").filter(Boolean) : []);
	} catch {
		return { fixed };
	}
	for (const p of changedPaths(snap.root)) {
		const abs = path.join(snap.root, p);
		if (inScope(abs, cwd, scope)) continue;
		const now = fs.existsSync(abs) && fs.statSync(abs).isFile() ? fs.readFileSync(abs, "latin1") : null;
		if (snap.dirty.has(p)) {
			const before = snap.dirty.get(p)!;
			if (hash(before) === hash(now)) continue; // already dirty before the run, untouched
			if (before === null) fs.rmSync(abs, { force: true });
			else fs.writeFileSync(abs, before, "latin1");
			fixed.push(`${p} (restored)`);
		} else if (tracked.has(p)) {
			git(snap.root, ["checkout", "HEAD", "--", p]);
			fixed.push(`${p} (restored from HEAD)`);
		} else if (now !== null && !ARTEFACT.test(p) && !(p.endsWith(".meta") && fs.existsSync(abs.slice(0, -5)))) {
			try {
				git(snap.root, ["reset", "-q", "--", p]); // unstage a new file that a commit/add brought in
			} catch {}
			const dest = path.join(quarantine, p);
			fs.mkdirSync(path.dirname(dest), { recursive: true });
			moveFile(abs, dest);
			fixed.push(`${p} (new file moved to ${dest})`);
		}
	}
	return { fixed, headMoved };
}

/** HEAD's commit, "" outside git or in a repo without commits. */
function headOf(cwd: string): string {
	try {
		return git(cwd, ["rev-parse", "HEAD"]).trim();
	} catch {
		return "";
	}
}

/** A check runs (for minutes) after the run's scope check: a commit made meanwhile is reported like headMoved. */
function headMovedNote(before: string, cwd: string): string {
	const now = headOf(cwd);
	if (!before || !now || now === before) return "";
	return `\n  HEAD moved during the check (${before.slice(0, 8)} -> ${now.slice(0, 8)}); not undone, the files in those commits were not scope-checked. Tell the user.`;
}

function piInvocation(args: string[]): { command: string; args: string[] } {
	const script = process.argv[1];
	if (script && !script.startsWith("/$bunfs/root/") && fs.existsSync(script)) {
		return { command: process.execPath, args: [script, ...args] };
	}
	if (!/^(node|bun)(\.exe)?$/.test(path.basename(process.execPath).toLowerCase())) {
		return { command: process.execPath, args };
	}
	return { command: "pi", args };
}

// ---------- subagent side ----------

const GIT_WRITE =
	/\bgit\b[^|;&\n]*\s(commit|push|pull|fetch|reset|checkout|switch|restore|stash|rebase|merge|cherry-pick|revert|am|apply|clean|rm|mv|add|tag|update-ref|worktree|branch\s+-[dDmM])\b/;

// package installs that write outside the project (user site-packages, a global prefix, ~/.cargo, ~/go), which the
// scope check (git tree only) never sees. A live worker ran `uv pip install pytest --target ~/.local/...`. Project-local
// installs (npm/pnpm install, yarn, uv sync/add, dotnet restore/add package) are not matched. The main session is not
// guarded: it is the one that answers a subagent's request for a tool, with the user watching.
const INSTALL_PREFIX = /^(?:(?:sudo|doas)(?:\s+(?:-[ugCh]\s+\S+|-\S+))*\s+|env(?:\s+-\S+)*\s+|\w+=\S*\s+|(?:time|nohup|command|exec)\s+)*/;
const GLOBAL_INSTALL =
	/^(?:\S*\/)?(?:(?:python[\d.]*\s+-m\s+)?pip[\d.]*\s+install|uv\s+pip\s+install|pipx\s+install|uv\s+tool\s+install|cargo\s+install|gem\s+install|go\s+install|yarn\s+global\s+add)\b/;

function isGlobalInstall(cmd: string): boolean {
	// quoted text is data: grep "pip install" README.md is a mention; each command of a chain is checked from its start
	const parts = cmd.replace(/'[^']*'|"(?:\\.|[^"\\])*"/g, "''").split(/&&|\|\||[;|&\n(){}`]/);
	return parts.some((p) => {
		const c = p.trim().replace(INSTALL_PREFIX, "");
		if (GLOBAL_INSTALL.test(c)) return true;
		// npm/pnpm install is project-local unless -g/--global
		return /^(?:\S*\/)?p?npm\s/.test(c) && /\s(?:install|i|add)(?:\s|$)/.test(c) && /\s(?:-g|--global)(?:\s|=|$)/.test(c);
	});
}

function childSide(pi: ExtensionAPI) {
	const id = process.env.PI_SUBAGENT_ID ?? "subagent";
	const dir = process.env.PI_HANDOFF_DIR ?? HANDOFF_ROOT;
	const promptNo = process.env.PI_PROMPT_NO ?? "?"; // the main session's user prompt this run belongs to
	let scope: string[] = [];
	try {
		scope = JSON.parse(process.env.PI_SUBAGENT_SCOPE ?? "[]");
	} catch {}

	pi.registerTool({
		name: "request",
		label: "Request",
		description:
			"Ask the main agent for something outside your task or writable scope (information you cannot find, a change to a file outside your scope, a decision). Stops you; you are resumed with the answer.",
		parameters: Type.Object({
			need: Type.String({ description: "Exactly what you need, as a task someone else can do without your context" }),
			why: Type.String({ description: "One sentence: why your task needs it" }),
		}),
		async execute(_id, params) {
			fs.writeFileSync(path.join(dir, `${id}.request.json`), JSON.stringify(params));
			return {
				content: [{ type: "text", text: "Request recorded. Stop now; you will be resumed with the answer." }],
				details: {},
				terminate: true,
			};
		},
	});

	// subagents share facts through a file, not through the main agent's context: each one is told its path
	pi.registerTool({
		name: "note",
		label: "Note",
		description:
			"Add one durable fact to the shared notes that the other subagents of this session read: a path:line, a signature, a decision, a gotcha. One line; not progress updates, not file contents.",
		parameters: Type.Object({ fact: Type.String({ description: "One self-contained line, e.g. `Config is loaded in src/config.ts:40 (loadConfig, returns Config | undefined)`" }) }),
		async execute(_id, params) {
			fs.appendFileSync(path.join(dir, "notes.md"), `- [p${promptNo} ${id}] ${params.fact.replace(/\s+/g, " ").trim().slice(0, 300)}\n`);
			return { content: [{ type: "text", text: "Noted." }], details: {} };
		},
	});

	pi.on("tool_call", async (event, ctx) => {
		if (event.toolName === "bash") {
			const cmd = (event.input as { command: string }).command;
			if (GIT_WRITE.test(cmd)) {
				return {
					block: true,
					reason: "Subagents do not change git state (commit, reset, checkout, stash, ...). The main agent and the user own git; edit files in your scope instead.",
				};
			}
			if (isGlobalInstall(cmd)) {
				return {
					block: true,
					reason: "Subagents do not install packages outside the project (pip/uv pip/pipx/uv tool/cargo/gem/go install, npm -g). If a tool is missing, report it, don't install it; if the task needs it, call request to ask the main agent.",
				};
			}
			return;
		}
		if (event.toolName !== "edit" && event.toolName !== "write") return;
		const file = (event.input as { path: string }).path;
		if (inScope(file, ctx.cwd, scope)) return;
		const allowed = scope.length ? scope.join(", ") : "none (read-only task)";
		return {
			block: true,
			reason: `${file} is outside your writable scope (${allowed}). Call the request tool if the task needs this change.`,
		};
	});
}

// ---------- main agent side ----------

interface Run {
	id: string;
	agent: AgentConfig;
	cwd: string;
	scope: string[];
	session: string;
	report: string;
	promptFile: string;
	answers: string[]; // reports produced for this run's requests, injected on resume
	check?: string;
	level?: string; // raised thinking level after a failed check or an overflow
	touched?: Set<string>; // files this run changed (all launches); undefined outside git = unknown
	task?: string; // the first task message, reformulated on an overflow retry
	model?: string; // ESCALATION_MODEL once a retry has escalated
	recoveries?: number; // overflow retries so far
	resumed?: boolean; // resumed after NEEDS: a fresh retry would lose the answer, so overflows are only reported
	requests?: number; // NEEDS so far (MAX_REQUESTS)
}

/** Why a launch ended badly although it may have produced text (see overflowOf). */
interface Overflow {
	kind: "budget" | "length" | "context" | "loop";
	why: string; // "ran out of thinking budget while answering", for reports and the retry message
	tail: string; // the end of its last reasoning: where it stalled
}

interface Outcome {
	id: string;
	status: "done" | "needs" | "error" | "failed";
	text: string;
	contextTokens: number;
	out?: string; // done: the subagent's final message
	scopeNote?: string;
	overflow?: Overflow;
}

const TaskSpec = Type.Object({
	agent: Type.Optional(Type.String({ description: "Agent name; default worker when scope is set, else scout" })),
	goal: Type.String({ description: "The narrow task, self-contained: the subagent has not seen this conversation" }),
	scope: Type.Optional(
		Type.Array(Type.String(), {
			description: 'Files or directories the subagent may edit/write. Omit for read-only tasks. In a chain, "@previous" means the FILES: line of the latest earlier report.',
		}),
	),
	inputs: Type.Optional(Type.Array(Type.String(), { description: "Handoff reports or files to read first" })),
	done_when: Type.Optional(Type.String({ description: "Acceptance criteria in words" })),
	check: Type.Optional(
		Type.String({
			description: `Shell command run by the extension (not the subagent) after it finishes; exit 0 = pass. On failure the subagent is resumed with the output, up to ${CHECK_RETRIES} times. You get the exit code and the last lines.`,
		}),
	),
	for: Type.Optional(Type.String({ description: "Run id whose request this task answers; its report is handed to that run on resume" })),
	cwd: Type.Optional(Type.String()),
});

const Params = Type.Object({
	agent: Type.Optional(Type.String()),
	goal: Type.Optional(Type.String()),
	scope: Type.Optional(Type.Array(Type.String())),
	inputs: Type.Optional(Type.Array(Type.String())),
	done_when: Type.Optional(Type.String()),
	check: Type.Optional(Type.String()),
	for: Type.Optional(Type.String()),
	cwd: Type.Optional(Type.String()),
	tasks: Type.Optional(Type.Array(TaskSpec, { description: `Independent tasks, run ${MAX_CONCURRENCY} at a time` })),
	chain: Type.Optional(Type.Array(TaskSpec, { description: "Sequential tasks; each step gets the previous report as input" })),
	resume: Type.Optional(Type.String({ description: "Run id of a subagent that stopped with a request" })),
	// with resume, `scope` replaces the run's writable scope
	message: Type.Optional(Type.String({ description: "With resume: extra note for the subagent" })),
});

type Spec = {
	agent?: string;
	goal: string;
	scope?: string[];
	inputs?: string[];
	done_when?: string;
	check?: string;
	for?: string;
	cwd?: string;
};

const CONTRACT = (id: string, scope: string[]) => `

## Delegation contract
You are subagent ${id}. The main agent gave you one narrow task; stay inside it.
- Writable scope: ${scope.length ? scope.join(", ") : "none: this is a read-only task"}. Edits outside it are blocked.
- If the task needs anything outside it (information you cannot find, a change outside your scope, a decision), call the \`request\` tool with exactly what you need and stop. Do not work around it; you will be resumed with the answer.
- Your final message is saved as your report. Its first line must be \`SUMMARY: <at most 25 words>\`. Then details, at most 600 words, citing path:line instead of pasting file contents.`;

/** What the main agent is told about this project's checks and git (computed once per session). */
function checkGuidelines(cwd: string): string[] {
	const out: string[] = [];
	const step = projectCheck(cwd);
	const full = projectFullCheck(cwd);
	const src = projectFile(cwd, "check-fast") || projectFile(cwd, "check") ? ".pi" : (detectChecks(cwd)?.source ?? "");
	if (step) {
		out.push(
			`Project checks (from ${src}): \`${step}\` runs automatically after every step that changes files` +
				(full ? `, and \`${full}\` once when you finish` : "") +
				". They cannot be replaced; pass `check` only for something they do not cover. When the user asks for checks to pass, these verify them.",
		);
	} else {
		out.push(
			"This project has no check the harness can find (no .pi/check, and no package.json scripts, Cargo.toml, go.mod, .NET project, Python tests or Makefile targets): changes are not verified automatically. Give writing tasks a `check` command, or tell the user to add .pi/check.",
		);
	}
	if (!gitRoot(cwd) && !HC.allowNoGit) {
		out.push(
			"This directory is not a git repository: subagent changes cannot be scoped or undone, so writing subagents are refused. Tell the user to run `git init` and commit, or to set allowNoGit in .pi/harness.json.",
		);
	}
	if (HC.error) out.push(`.pi/harness.json could not be read (${HC.error}); defaults apply.`);
	return out;
}

function parentSide(pi: ExtensionAPI) {
	const runs = new Map<string, Run>();
	// parallel runs share the worktree: a run's scope check must not undo a concurrent run's in-scope work
	const active = new Set<Run>();
	let seq = 0;
	const finished: { seq: number; scope: string[] }[] = [];
	let runDir: string | undefined;
	let counter = 0;
	let promptNo = 0; // user prompts so far: notes are tagged with it, so later prompts can tell old facts from new
	const waiting = new Set<string>(); // runs stopped with NEEDS, not yet resumed
	// runs whose check was deferred because another run was waiting; run when the waiting set empties
	const deferred = new Map<string, { run: Run; check: string }>();
	const baselines = new Map<string, Baseline>(); // per cwd and check (baselineKey), before this prompt's first step
	let writeChain: Promise<unknown> = Promise.resolve();
	const serialWrite = <T,>(f: () => Promise<T>): Promise<T> => {
		const next = writeChain.then(f, f);
		writeChain = next.catch(() => {});
		return next;
	};
	const unverified = new Set<string>(); // files changed and not yet covered by a full check (kept until one has run on them)
	let fullChecks = 0;
	const track = (run: Run, o: Outcome) => {
		if (o.status === "needs") waiting.add(run.id);
		else waiting.delete(run.id);
	};
	// files changed in this prompt (worker scopes and the main agent's own edits) and not reviewed since
	const changed = new Set<string>();
	/**
	 * A writing run that ended (done, failed or error): its files (all launches) go to the auto-review and the full
	 * check. NEEDS is still in progress and is recorded when it is resumed.
	 */
	const recordChanges = (run: Run, o: Outcome) => {
		if (o.status === "needs" || !agentWrites(run.agent)) return;
		for (const f of run.touched ?? run.scope) {
			changed.add(f);
			unverified.add(f);
		}
	};
	let reviews = 0;
	// a prompt containing [plan-only] (e.g. /plan, /backlog-item) may read and plan but not change anything
	let planOnly = false;
	const checks = new Set<string>(); // check commands given to subagents; the main agent may run them too

	/** Scopes of runs active now or finished since `startSeq`: their in-scope changes are not a violation. */
	const concurrentScopes = (startSeq: number) => [
		...[...active].flatMap((r) => r.scope),
		...finished.filter((f) => f.seq > startSeq).flatMap((f) => f.scope),
	];

	const ensureRunDir = () => {
		if (!runDir) {
			const stamp = new Date().toISOString().slice(5, 16).replace(/[-:T]/g, "");
			runDir = path.join(HANDOFF_ROOT, `${stamp}-${process.pid}`);
			fs.mkdirSync(runDir, { recursive: true, mode: 0o700 });
			// exists from the start: a subagent told about a missing file reads it again and again
			fs.writeFileSync(path.join(runDir, "notes.md"), "# Shared notes: one fact per line, written by subagents with the note tool\n");
		}
		return runDir;
	};

	function launch(run: Run, message: string, model: string | undefined, thinking: string | undefined, signal?: AbortSignal) {
		const tools = [...(run.agent.tools ?? ALL_TOOLS).filter((t) => t !== "request" && t !== "note"), "request", "note"];
		const args = ["--mode", "json", "-p", "--session", run.session, "--tools", tools.join(",")];
		const model_ = run.model ?? run.agent.model ?? model;
		if (model_) args.push("--model", model_);
		// thinking level per task: a retry's raised level, else the agent's own (scouts think little, planners and
		// reviewers more), else the main session's. The level selects the reasoning budget in models.json.
		// An agent with its own model never gets the session's level: only its frontmatter level or one raised from
		// its own level (see settle); with neither, nothing is passed and its model's `:level` suffix applies.
		const level = run.level ?? run.agent.thinking ?? (run.agent.model ? undefined : thinking);
		if (level) args.push("--thinking", level);
		args.push("--append-system-prompt", run.promptFile, message);

		const snap = snapshot(run.cwd);
		const startSeq = seq;
		active.add(run);
		return new Promise<Outcome>((resolve) => {
			const messages: Message[] = [];
			let contextTokens = 0;
			let compacted = false;
			let looped: string | undefined; // set when the launch is stopped for looping (KILL_*)
			let turns = 0;
			let repeats = 0;
			let lastCalls = "";
			let stderr = "";
			let buf = "";
			const inv = piInvocation(args);
			const proc = spawn(inv.command, inv.args, {
				cwd: run.cwd,
				shell: false,
				stdio: ["ignore", "pipe", "pipe"],
				env: {
					...process.env,
					PI_SUBAGENT: "1",
					PI_SUBAGENT_ID: run.id,
					PI_SUBAGENT_SCOPE: JSON.stringify(run.scope),
					PI_HANDOFF_DIR: path.dirname(run.report),
					PI_PROMPT_NO: String(promptNo),
				},
			});
			const onLine = (line: string) => {
				let ev: any;
				try {
					ev = JSON.parse(line);
				} catch {
					return;
				}
				if (ev.type === "compaction_start") compacted = true;
				if (ev.type === "message_end" && ev.message) {
					messages.push(ev.message);
					if (ev.message.role === "assistant" && !looped) {
						turns++;
						const calls = JSON.stringify(((ev.message.content as any[]) ?? []).filter((c) => c.type === "toolCall").map((c) => [c.name, c.arguments]));
						repeats = calls !== "[]" && calls === lastCalls ? repeats + 1 : 0;
						lastCalls = calls;
						if (repeats + 1 >= KILL_REPEATS) looped = `repeated the same ${JSON.parse(calls)[0]?.[0] ?? "tool"} call ${repeats + 1} times in a row and was stopped`;
						else if (turns >= KILL_TURNS) looped = `ran ${turns} turns without finishing and was stopped`;
						if (looped) proc.kill("SIGTERM");
					}
					if (ev.message.role === "assistant" && ev.message.usage) contextTokens = ev.message.usage.totalTokens || contextTokens;
				}
			};
			proc.stdout.on("data", (d) => {
				buf += d.toString();
				const lines = buf.split("\n");
				buf = lines.pop() ?? "";
				for (const l of lines) if (l.trim()) onLine(l);
			});
			proc.stderr.on("data", (d) => (stderr += d.toString()));
			const kill = () => proc.kill("SIGTERM");
			signal?.addEventListener("abort", kill, { once: true });
			proc.on("close", (code) => {
				signal?.removeEventListener("abort", kill);
				if (buf.trim()) onLine(buf);
				active.delete(run);
				const allowed = [...run.scope, ...concurrentScopes(startSeq)];
				finished.push({ seq: ++seq, scope: run.scope });
				let violations: string[] = [];
				let headMoved: string | undefined;
				try {
					if (snap) ({ fixed: violations, headMoved } = enforceScope(snap, allowed, run.cwd, path.join(path.dirname(run.report), "quarantine", run.id)));
				} catch (e) {
					violations = [`scope check failed (${(e as Error).message.slice(0, 200)}); review git status`];
				}
				const shown = violations.slice(0, 5).join(", ") + (violations.length > 5 ? `, +${violations.length - 5} more` : "");
				const scopeNote =
					(violations.length ? `\n  SCOPE VIOLATION, undone: ${shown}. Widen the scope (resume with scope) or delegate those files.` : "") +
					(headMoved ? `\n  HEAD moved during the run (${headMoved}); not undone, the files in those commits were not scope-checked. Tell the user.` : "");
				// files this launch changed, recorded for every outcome: a resumed launch's snapshot already sees a NEEDS
				// launch's edits as dirty, so the run's own set is the only record of them
				if (snap) {
					run.touched ??= new Set();
					try {
						for (const f of touchedFiles(snap)) if (!ARTEFACT.test(f) && !f.endsWith(".meta")) run.touched.add(f);
					} catch {}
				}
				const reqFile = path.join(path.dirname(run.report), `${run.id}.request.json`);
				if (fs.existsSync(reqFile)) {
					let req = { need: "?", why: "?" };
					try {
						req = JSON.parse(fs.readFileSync(reqFile, "utf8"));
					} catch {}
					fs.renameSync(reqFile, reqFile.replace(/\.json$/, `.${Date.now()}.json`));
					run.requests = (run.requests ?? 0) + 1;
					if (run.requests > MAX_REQUESTS) {
						const why = `asked the main agent for help ${run.requests} times (last: "${req.need.slice(0, 120)}") and was stopped`;
						resolve({ id: run.id, status: "error", contextTokens, overflow: { kind: "loop", why, tail: "" }, text: `[${run.id}] ERROR: it ${why}${scopeNote}` });
						return;
					}
					resolve({
						id: run.id,
						status: "needs",
						contextTokens,
						text:
							`[${run.id}] NEEDS: ${req.need}\n  why: ${req.why}\n` +
							`  Dispatch a subagent with for: "${run.id}" to get it, then call subagent({resume: "${run.id}"}).` +
							scopeNote,
					});
					return;
				}
				const out = finalText(messages);
				const overflow = overflowOf(messages, compacted, looped);
				if (code !== 0 || !out) {
					resolve({
						id: run.id,
						status: "error",
						contextTokens,
						overflow,
						text: `[${run.id}] ERROR (exit ${code}): ${(stderr || "no output").trim().slice(-SUMMARY_CAP)}${scopeNote}`,
					});
					return;
				}
				fs.writeFileSync(run.report, out + "\n");
				resolve({ id: run.id, status: "done", contextTokens, out, scopeNote, overflow, text: doneText(run, out, scopeNote) });
			});
		});
	}

	const hasSummary = (out: string) => /^\W*SUMMARY:/m.test(out);

	function doneText(run: Run, out: string, scopeNote: string): string {
		// SUMMARY line if the agent wrote one, else the first line that is not a heading or blank
		const summary = (
			out.match(/^\W*SUMMARY:.*$/m)?.[0] ??
			out.split("\n").find((l) => l.trim() && !/^\s*(#|---)/.test(l)) ??
			""
		)
			.trim()
			.slice(0, SUMMARY_CAP);
		return `[${run.id}] done, report: ${run.report} (read it yourself with read if you need more than the summary)\n  ${summary}${scopeNote}`;
	}

	/**
	 * After a subagent finishes: ask once for a missing SUMMARY line, then run the task's check (by the extension,
	 * so the result does not depend on the subagent's claims) and resume the subagent with failures.
	 */
	async function settle(run: Run, outcome: Outcome, check: string | undefined, model?: string, thinking?: string, signal?: AbortSignal): Promise<Outcome> {
		const notes: string[] = []; // HEAD moved during a check: added to whatever the final outcome is
		const o = await settleChecks(run, await recover(run, outcome, model, thinking, signal), check, notes, model, thinking, signal);
		return notes.length ? { ...o, text: o.text + notes.join("") } : o;
	}

	/**
	 * An overflowed launch is not retried as is (the same prompt on the same model stalls the same way): a fresh session
	 * gets the task reformulated (where it stalled, sub-steps first, tools instead of long reasoning, a REMAINING line if
	 * it does not fit) one thinking level higher; the second retry also switches to ESCALATION_MODEL. All of it stays in
	 * the handoff area; the main agent gets one line. Still overflowing: a report written under a budget cut is kept as
	 * done with a warning, anything else fails with "split this goal".
	 * A verified writing run (it has a check) whose answer was only cut short is left to its check: the check decides,
	 * and a failure is retried one level higher anyway. A resumed run is not restarted (it would lose the answer to its
	 * request); its overflow is only reported.
	 */
	async function recover(run: Run, outcome: Outcome, model?: string, thinking?: string, signal?: AbortSignal): Promise<Outcome> {
		const verified = !!run.check && run.scope.length > 0 && agentWrites(run.agent);
		const log: string[] = [];
		while (
			outcome.overflow &&
			outcome.status !== "needs" &&
			run.task &&
			(!run.resumed || outcome.overflow.kind === "loop") &&
			!(verified && outcome.status === "done" && outcome.overflow.kind === "budget") &&
			(run.recoveries ?? 0) < MAX_RECOVERIES
		) {
			const n = (run.recoveries = (run.recoveries ?? 0) + 1);
			const ov = outcome.overflow;
			run.requests = 0;
			const cur = run.model ?? run.agent.model ?? model;
			const from = run.level ?? (run.agent.model ? ownLevel(run.agent) : (run.agent.thinking ?? thinking));
			if (from || !run.agent.model) run.level = raiseLevel(from);
			if (n >= 2 && ESCALATION_MODEL && ESCALATION_MODEL !== cur) run.model = ESCALATION_MODEL;
			run.session = path.join(path.dirname(run.report), `${run.id}.r${n}.jsonl`); // fresh context
			log.push(`${ov.why} (${modelName(cur)}, ${from ?? "default level"}); retry ${n} reformulated at ${run.level ?? "the same level"}${run.model && run.model !== cur ? ` on ${modelName(run.model)}` : ""}`);
			outcome = await launch(run, reformulate(run, ov), model, thinking, signal);
		}
		const note = log.length ? `\n  overflow: ${log.join("; ")}` : "";
		const ov = outcome.overflow;
		if (!ov || outcome.status === "needs") return note ? { ...outcome, text: outcome.text + note } : outcome;
		const left = outcome.out?.match(/^\W*REMAINING:.*$/m)?.[0]?.trim();
		const keep = outcome.status === "done" && ov.kind === "budget";
		const advice = keep
			? `the answer was written under a budget cut; trust its check, or verify it before relying on it${left ? `. ${left}` : ""}`
			: `split this goal into smaller subagent tasks (one question or one file each)${left ? `. ${left}` : ""}`;
		return {
			...outcome,
			status: keep ? "done" : "failed",
			text: `${outcome.text}${note}\n  overflow: ${ov.why}${run.recoveries ? ` after ${run.recoveries} reformulated retries` : ""}: ${advice}.`,
		};
	}

	/** The task again, for a fresh session, with what went wrong and how to avoid it (recover). */
	function reformulate(run: Run, ov: Overflow): string {
		const how =
			ov.kind === "context"
				? "Read less: find the exact symbols with grep, read only the line ranges you need, and do not re-read files."
				: ov.kind === "loop"
					? "Do not repeat a call that already failed or gave the same result: a missing file stays missing. Change approach, or report what blocks you."
					: "Do not work it out in your head at length: first list the sub-steps (one line each), then settle each fact with a tool (grep, read the exact lines, run a short script or the check) and move on.";
		return [
			`RETRY: a previous attempt at this task ${ov.why}.`,
			ov.tail ? `Where it stalled (the end of its reasoning; do not repeat it):\n> ${ov.tail.replace(/\n+/g, "\n> ")}` : "",
			how,
			"Put facts you will need again in the shared notes (note tool), not in long reasoning.",
			run.scope.length ? "Files in your scope may already hold part of the change: look at `git diff -- <file>` first." : "",
			"If the whole task does not fit, finish a useful part completely and add the line `REMAINING: <what is left>` after FILES.",
			"",
			run.task!.replace(/^SHARED NOTES:.*$/m, notesLine(run)),
		]
			.filter(Boolean)
			.join("\n");
	}

	async function settleChecks(run: Run, outcome: Outcome, check: string | undefined, notes: string[], model?: string, thinking?: string, signal?: AbortSignal): Promise<Outcome> {
		for (let attempt = 0; ; attempt++) {
			if (outcome.status !== "done") return outcome;
			if (!hasSummary(outcome.out ?? "")) {
				const original = outcome.out ?? "";
				const fix = await launch(
					run,
					"Your report is missing its first lines. Reply with exactly two lines and nothing else: `SUMMARY: <at most 25 words>` and `FILES: <paths you changed, or none>`.",
					model,
					thinking,
					signal,
				);
				if (fix.status === "done" && hasSummary(fix.out ?? "")) {
					const merged = `${fix.out!.trim()}\n\n${original}`;
					fs.writeFileSync(run.report, merged + "\n");
					const note = (outcome.scopeNote ?? "") + (fix.scopeNote ?? "");
					outcome = { ...outcome, out: merged, scopeNote: note, text: doneText(run, merged, note) };
				} else {
					fs.writeFileSync(run.report, original + "\n"); // keep the real report
				}
			}
			if (!check) {
				// a writing run with no check at all: say so, or the change looks verified
				const writes = run.scope.length && agentWrites(run.agent);
				return writes ? { ...outcome, text: `${outcome.text}\n  check: none (no check detected; add .pi/check or pass check)` } : outcome;
			}
			// nothing changed (a read-only task given a scope): the check cannot have changed either
			if (run.touched && run.touched.size === 0) return { ...outcome, text: `${outcome.text}\n  check: skipped (no files changed)` };
			// another subagent is waiting mid-change (NEEDS): the project would fail for its half-done work, not ours.
			// The check is recorded and runs once no run is waiting (runDeferred, after the last resume ends).
			const others = [...waiting].filter((id) => id !== run.id);
			if (others.length) {
				deferred.set(run.id, { run, check });
				return { ...outcome, text: `${outcome.text}\n  check: deferred (runs when ${others.join(", ")} is resumed and finishes)` };
			}
			deferred.delete(run.id); // a resumed run that had a deferred check: this one covers it
			const log = path.join(path.dirname(run.report), `${run.id}.check${attempt ? `-${attempt}` : ""}.log`);
			const head = headOf(run.cwd);
			const res = await runCheck(check, run.cwd, log, signal);
			const moved = headMovedNote(head, run.cwd);
			if (moved) notes.push(moved);
			// the project was already failing before this prompt's first step: only new errors count (compareToBaseline)
			const base = baselines.get(baselineKey(run.cwd, check));
			const cmp = compareToBaseline(res, base, run.cwd);
			const newErrors = cmp.newErrors;
			if (cmp.verdict === "pass" && res.code !== 0) return { ...outcome, text: `${outcome.text}\n  check: no new failures (${cmp.why}). Log: ${log}` };
			// the failure may predate the step: relaunching the subagent to fix it would spend the retries on it
			if (cmp.verdict === "uncomparable") return { ...outcome, status: "failed", text: `${outcome.text}\n  check: ${uncomparableText(res, log)}` };
			if (res.aborted) return { ...outcome, status: "failed", text: `${outcome.text}\n  check: aborted, log: ${log}` };
			if (cmp.verdict === "pass") {
				// the lines that say what passed (test totals, exit lines), not just the last two (on psx-iso those were the
				// main agent's own grep counts, which hid "12/12 passed" and sent it re-verifying for 35 minutes)
				const lines = res.tail.split("\n").filter((l) => l.trim());
				const key = lines.filter((l) => /\b(pass(ed)?|fail(ed|ures)?|exit|result|error|green|ok)\b/i.test(l)).slice(-4);
				const last = (key.length ? key : lines.slice(-2)).join(" | ");
				return { ...outcome, text: `${outcome.text}\n  check: exit 0 (${last.slice(0, 300)})` };
			}
			// after the second failed retry, ESCALATION_MODEL takes over the same session (it sees what was tried) for one
			// more attempt, unless the step already runs on it
			const cur = run.model ?? run.agent.model ?? model;
			const escalate = attempt === CHECK_RETRIES && !!ESCALATION_MODEL && ESCALATION_MODEL !== cur;
			if (attempt >= CHECK_RETRIES && !escalate) {
				const last = newErrors ? `new errors: ${newErrors.slice(0, 4).join(" | ")}` : res.tail.split("\n").slice(-4).join("\n    ");
				return {
					...outcome,
					status: "failed",
					text: `${outcome.text}\n  check: FAILED (${cmp.why ?? exitText(res)}) after ${attempt} retries${attempt > CHECK_RETRIES ? ` (the last on ${modelName(cur)})` : ""}, log: ${log}\n    ${last.slice(0, 600)}`,
				};
			}
			if (escalate) {
				run.model = ESCALATION_MODEL;
				notes.push(`\n  escalated: the check still failed after ${CHECK_RETRIES} retries on ${modelName(cur)}; retry ${attempt + 1} on ${modelName(ESCALATION_MODEL)}`);
			}
			// a failed check is where reasoning pays: think one level higher on the retry. An agent with its own model
			// raises from its own level (frontmatter, else the model's `:level` suffix), never the session's; with
			// neither it is left alone
			const from = run.level ?? (run.agent.model ? ownLevel(run.agent) : (run.agent.thinking ?? thinking));
			if (from || !run.agent.model) run.level = raiseLevel(from);
			const takeOver = escalate
				? `You are taking over this step from another model: its ${attempt + 1} attempts did not pass the check (they are above in this session). Look at \`git diff\` of your scope first, find the cause instead of repeating its fixes.\n`
				: "";
			outcome = await launch(
				run,
				takeOver +
				(newErrors
					? `The check \`${check}\` shows errors that were not there before your change (the project already had ${base!.errors.size} known errors, which are not yours to fix):\n${newErrors.slice(0, 12).join("\n")}\nFull log: ${log}\nFix the new errors within your scope, then report again.`
					: `The check \`${check}\` failed (${cmp.why ?? exitText(res)}). Its last lines:\n${res.tail}\nFull log: ${log}\nFix the cause within your scope, then report again.`),
				model,
				thinking,
				signal,
			);
		}
	}

	/**
	 * Once no run is waiting: the checks deferred meanwhile (one run per distinct command and cwd). Reported, not
	 * retried: those subagents are done, and the main agent sends a fix step for a failure.
	 */
	async function runDeferred(signal?: AbortSignal): Promise<Outcome[]> {
		if (waiting.size || !deferred.size) return [];
		const groups = new Map<string, { check: string; rs: Run[] }>();
		for (const { run, check } of deferred.values()) {
			const key = `${run.cwd}\0${check}`;
			if (!groups.has(key)) groups.set(key, { check, rs: [] });
			groups.get(key)!.rs.push(run);
		}
		deferred.clear();
		const outs: Outcome[] = [];
		for (const { check, rs } of groups.values()) {
			const run = rs[0];
			const ids = rs.map((r) => r.id).join(", ");
			const log = path.join(path.dirname(run.report), `${run.id}.check-deferred.log`);
			const head = headOf(run.cwd);
			const res = await runCheck(check, run.cwd, log, signal);
			const moved = headMovedNote(head, run.cwd);
			const cmp = compareToBaseline(res, baselines.get(baselineKey(run.cwd, check)), run.cwd);
			const result =
				cmp.verdict === "pass"
					? res.code === 0
						? "exit 0"
						: `no new failures (${cmp.why}). Log: ${log}`
					: cmp.verdict === "uncomparable"
						? uncomparableText(res, log)
						: `FAILED (${cmp.why ?? exitText(res)}), log: ${log}\n    ${(cmp.newErrors ? `new errors: ${cmp.newErrors.slice(0, 4).join(" | ")}` : res.tail.split("\n").slice(-4).join("\n    ")).slice(0, 600)}`;
			const ok = cmp.verdict === "pass";
			outs.push({ id: run.id, status: ok ? "done" : "failed", contextTokens: 0, text: `[${ids}] deferred check: ${result}${moved}` });
		}
		return outs;
	}

	function newRun(spec: Spec, defaultCwd: string, agents: AgentConfig[]): Run | string {
		const name = spec.agent ?? (spec.scope?.length ? "worker" : "scout");
		const agent = agents.find((a) => a.name === name);
		if (!agent) return `Unknown agent "${name}". Available: ${agents.map((a) => a.name).join(", ")}`;
		if (spec.for && !runs.has(spec.for)) return `Unknown run id "${spec.for}" in for`;
		const dir = ensureRunDir();
		const id = `${String(++counter).padStart(2, "0")}-${agent.name}`;
		const cwd = path.resolve(defaultCwd, spec.cwd ?? ".");
		const scope = (spec.scope ?? []).map((s) => path.resolve(cwd, s));
		const promptFile = path.join(dir, `${id}.prompt.md`);
		fs.writeFileSync(promptFile, agent.systemPrompt + CONTRACT(id, scope), { mode: 0o600 });
		const run: Run = { id, agent, cwd, scope, session: path.join(dir, `${id}.jsonl`), report: path.join(dir, `${id}.md`), promptFile, answers: [] };
		runs.set(id, run);
		return run;
	}

	/**
	 * Where the shared notes are and what they hold now (a retry gets a fresh count). Facts are tagged `[pN id]` with the
	 * user prompt that wrote them: ones from earlier prompts may describe code that has changed since.
	 */
	function notesLine(run: Run): string {
		const notes = path.join(path.dirname(run.report), "notes.md");
		const facts = fs.existsSync(notes) ? fs.readFileSync(notes, "utf8").split("\n").filter((l) => l.startsWith("- ")) : [];
		const now = facts.filter((l) => l.startsWith(`- [p${promptNo} `)).length;
		const old = facts.length - now;
		const what = !facts.length
			? " (no facts yet, nothing to read)"
			: ` (${[now && `${now} from this prompt (p${promptNo})`, old && `${old} from earlier prompts, which may be outdated if those files changed since: check before relying on them`].filter(Boolean).join(", ")}; read it first)`;
		return `SHARED NOTES: ${notes}${what}. Record facts later subagents will need with the note tool (one line each: path:line, signatures, decisions).`;
	}

	function taskMessage(spec: Spec, run: Run): string {
		const lines = [`GOAL: ${spec.goal}`];
		if (spec.done_when) lines.push(`DONE WHEN: ${spec.done_when}`);
		if (spec.check) {
			lines.push(
				`CHECK: \`${spec.check}\` runs automatically when you finish and must pass. Do not run it yourself (it can take minutes); if it fails you are resumed with its output.`,
			);
		}
		lines.push(`WRITABLE SCOPE: ${run.scope.length ? run.scope.join(", ") : "none (read-only)"}`);
		if (spec.inputs?.length) lines.push(`INPUTS (read these first): ${spec.inputs.join(", ")}`);
		lines.push(notesLine(run));
		lines.push(
			"REPORT FORMAT: your final message must start with the line `SUMMARY: <at most 25 words>`, then `FILES: <comma-separated paths you changed, or for a plan the paths the change must touch, or none>`, then details.",
		);
		return lines.join("\n");
	}

	/**
	 * "Return the full content of the report" as a subagent task: answered from the handoff file, no subagent.
	 * (The main agent spawned three subagents in a row for this on psx-iso.)
	 */
	function reportFetch(spec: Spec): Outcome | undefined {
		if (spec.scope?.length) return undefined;
		const target = reportGoal(spec.goal, [...runs.keys()], HANDOFF_ROOT);
		if (!target) return undefined;
		const named = target.file ?? (target.id ? runs.get(target.id)?.report : undefined);
		const latest = [...runs.values()].reverse().find((r) => fs.existsSync(r.report))?.report;
		const file = named ?? latest;
		if (!file || !fs.existsSync(file)) return undefined;
		const text = fs.readFileSync(file, "utf8");
		const kept = Buffer.from(text, "utf8").subarray(0, PARENT_HANDOFF_READ_CAP).toString("utf8");
		return {
			id: "-",
			status: "done",
			contextTokens: 0,
			text: `[report ${file}, served directly; you can read handoff reports yourself]\n${kept}${text.length > kept.length ? "\n[truncated]" : ""}`,
		};
	}

	async function runSpec(spec: Spec, defaultCwd: string, agents: AgentConfig[], model?: string, thinking?: string, signal?: AbortSignal) {
		const served = reportFetch(spec);
		if (served) return served;
		// inputs may name earlier runs ("01-scout" or "@01-scout"): the main agent passes ids, not report contents
		if (spec.inputs?.length) spec = { ...spec, inputs: spec.inputs.map((i) => runs.get(i.replace(/^@/, ""))?.report ?? i) };
		// a subagent asked to copy files out costs minutes of generation (46 KB reports on psx-iso) and the
		// main context gets them anyway: refuse before spawning
		if (/\b(verbatim|full (file )?contents?|entire (file|contents?)|in full|whole file|complete contents?)\b/i.test(spec.goal)) {
			return {
				id: "-",
				status: "error",
				contextTokens: 0,
				text: "Refused: subagents report findings, not file contents. Ask a question (what X does, where Y is, which fields/methods exist and at which path:line), or read the file yourself with read.",
			} as Outcome;
		}
		if (planOnly) {
			const name = spec.agent ?? "scout"; // nothing writes in a plan-only prompt: an unnamed agent is a scout
			if (agentWrites(agents.find((x) => x.name === name))) return { id: "-", status: "error", text: PLAN_ONLY_REASON, contextTokens: 0 } as Outcome;
			spec = { ...spec, agent: name, scope: [] }; // read-only agent: a scope means nothing, drop it
		}
		{
			// one step = at most MAX_STEP_FILES files (psx-iso: a 4-file task left a compile error spread over the AI,
			// the scene builder and the tests, and the fix broke the shared test setup)
			const name = spec.agent ?? (spec.scope?.length ? "worker" : "scout");
			const writes = agentWrites(agents.find((x) => x.name === name));
			const cwd0 = path.resolve(defaultCwd, spec.cwd ?? ".");
			const dirs = (spec.scope ?? []).filter((f) => {
				const abs = path.resolve(cwd0, f);
				return fs.existsSync(abs) && fs.statSync(abs).isDirectory();
			});
			if (writes && !HC.allowNoGit && !gitRoot(cwd0)) {
				return {
					id: "-",
					status: "error",
					contextTokens: 0,
					text: "Refused: not a git repository, so this subagent's changes could not be scoped, checked against a snapshot or undone. Ask the user to run `git init` and commit first (or set allowNoGit in .pi/harness.json).",
				} as Outcome;
			}
			if (writes && ((spec.scope?.length ?? 0) > MAX_STEP_FILES || dirs.length)) {
				return {
					id: "-",
					status: "error",
					contextTokens: 0,
					text:
						`Refused: one step changes at most ${MAX_STEP_FILES} files, listed by name (got ${spec.scope?.length ?? 0}${dirs.length ? `, directories: ${dirs.join(", ")}` : ""}). ` +
						"Split it into steps that each leave the project compiling, and run them as a chain (one worker per step; tests as their own step).",
				} as Outcome;
			}
		}
		const run = newRun(spec, defaultCwd, agents);
		if (typeof run === "string") return { id: "-", status: "error", text: run, contextTokens: 0 } as Outcome;
		// a writing task without a check gets the project's check (.pi/check), so verification never depends on
		// the main agent remembering to ask for it
		const canWrite = agentWrites(run.agent);
		// the project check always runs for writing tasks; a task check adds to it, never replaces it (psx-iso: the
		// main agent passed check: "echo done" for a worker editing 3 files)
		const pc = run.scope.length && canWrite ? projectCheck(run.cwd) : undefined;
		// the project check in a subshell: a detected one starts with `cd <git root>`, the task check runs in run.cwd
		if (pc && spec.check?.trim() !== pc) spec = { ...spec, check: spec.check ? `( ${pc} ) && { ${spec.check}; }` : pc };
		if (spec.check) checks.add(spec.check.trim());
		run.check = spec.check;
		// writing steps run one at a time: two at once share the worktree, and each one's check then fails on the
		// other's half-done change (psx-iso: two parallel tool calls, CrawlerAI.cs and PlayerController.cs)
		const outcome = await (canWrite && run.scope.length ? serialWrite : (f: () => Promise<Outcome>) => f())(async () => {
			let baseNote = "";
			let headNote = "";
			// one baseline per cwd and check: a step in another directory or repo gets its own
			const key = spec.check && baselineKey(run.cwd, spec.check);
			if (spec.check && key && canWrite && run.scope.length && !baselines.has(key)) {
				const log = path.join(path.dirname(run.report), `baseline-${baselines.size + 1}.log`);
				const head = headOf(run.cwd);
				const r = await runCheck(spec.check, run.cwd, log, signal);
				// an aborted baseline is not kept: the next step runs it again
				if (!r.aborted) baselines.set(key, { code: r.code, errors: errorLines(r.out, run.cwd), timedOut: r.timedOut });
				const n = baselines.get(key)?.errors.size ?? 0;
				if (r.timedOut) baseNote = `\n  baseline: the check timed out before this step (log ${log}); a failing check is not compared.`;
				else if (r.code !== 0 && !r.aborted)
					baseNote = n
						? `\n  baseline: the check was already failing before this step (exit ${r.code}, ${n} errors, log ${log}); only new errors with the same exit code count.`
						: `\n  baseline: the check was already failing before this step without error lines (exit ${r.code}, log ${log}); a failure with that code can't be compared, so it fails without retries.`;
				headNote = headMovedNote(head, run.cwd);
			}
			run.task = taskMessage(spec, run);
			const o = await settle(run, await launch(run, run.task, model, thinking, signal), spec.check, model, thinking, signal);
			// the baseline note explains a failing check; after a pass ("check: exit 0") it only contradicts it
			return { ...o, text: o.text + (o.status === "done" ? "" : baseNote) + headNote };
		});
		track(run, outcome);
		if (spec.for && outcome.status === "done") runs.get(spec.for)!.answers.push(run.report);
		recordChanges(run, outcome);
		return outcome;
	}

	const agentsAtLoad = discoverAgents(process.cwd(), "user").agents;

	pi.registerTool({
		name: "subagent",
		label: "Subagent",
		description: [
			"Delegate a narrow task to a subagent with a fresh context. Results are written to handoff files; you get a status, the report path and a one-line summary.",
			`Agents: ${agentsAtLoad.map((a) => `${a.name} (${a.description})`).join("; ")}.`,
			"Modes: single (goal, optional agent), tasks (parallel, independent), chain (sequential, each step gets the previous report), resume (continue a subagent that stopped with NEEDS).",
		].join(" "),
		parameters: Params,
		promptGuidelines: [
			"Your context is small and compaction loses detail. Keep this conversation for decisions; subagents do the reading and the doing. Your own read and bash output is capped, and your read/bash/edit tools are withdrawn once you have used your share for this prompt: from then on use subagent (tasks get their check run automatically).",
			"Ask scouts questions, not for file contents: \"where is X handled and what does it take\", not \"return lines 365-410\".",
			"Work in small steps: each worker task changes at most 2 named files and must leave the project compiling; tests are a step of their own, and shared test setup is changed only as a named step. Run a plan's steps as a chain, one worker per step.",
			"Give each subagent ONE narrow goal with exact paths, a writable scope (only the files it should change; none for scouts/reviewers) and `check`: the command that proves the work (tests, build check). The extension runs it, retries the subagent on failure and tells you the exit code.",
			"Trust the check result, not a subagent's claims. Do not re-run checks yourself after a passing `check`.",
			...checkGuidelines(process.cwd()),
			"Pass information between subagents as run ids or report paths in inputs (inputs: [\"01-scout\"]); read a report yourself only when you must decide something from it (handoff reads always work, capped). Subagents also share facts through a notes file they read and write themselves: do not relay findings between them.",
			"A subagent that runs out of thinking budget or context is retried automatically with a reformulated task (and then another model). If it still reports `overflow: ... split this goal`, do not resend it: split it into smaller tasks (one question or one file each), or chain its REMAINING line.",
			"Images: the looker agent sees them (it runs on the vision model). Ask it specific questions about image files instead of guessing.",
			"If a subagent returns NEEDS, dispatch another subagent for that need with for set to the waiting run id, then call subagent with resume set to that id. Never do the out-of-scope work by stretching the first subagent's task.",
			"Do one-line or single-file edits yourself; delegate anything that needs reading more than ~2 files.",
		],

		async execute(_toolCallId, params, signal, _onUpdate, ctx) {
			const agents = discoverAgents(ctx.cwd, "user").agents;
			const model = ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : undefined;
			const thinking = ctx.thinkingLevel;
			const reply = (outs: Outcome[]) => ({
				content: [{ type: "text" as const, text: outs.map((o) => o.text).join("\n") }],
				details: { outcomes: outs },
				isError: outs.some((o) => o.status === "error"),
			});

			if (params.resume) {
				const run = runs.get(params.resume);
				if (planOnly && (run?.scope.length || params.scope?.length)) {
					return reply([{ id: params.resume, status: "error", text: PLAN_ONLY_REASON, contextTokens: 0 }]);
				}
				if (!run) return reply([{ id: params.resume, status: "error", text: `Unknown run id "${params.resume}"`, contextTokens: 0 }]);
				if (params.scope) {
					run.scope = params.scope.map((s) => path.resolve(run.cwd, s));
					fs.writeFileSync(run.promptFile, run.agent.systemPrompt + CONTRACT(run.id, run.scope), { mode: 0o600 });
				}
				const widened = params.scope ? ` Your writable scope is now: ${run.scope.join(", ")}.` : "";
				const answer = run.answers.length
					? `Your request was handled. Read: ${run.answers.join(", ")}.`
					: params.message || params.scope
						? "Your request was handled."
						: "Your request was not answered (no helper report). Continue without it if you can; otherwise finish with a report that says what blocks you. Do not ask again.";
				run.answers = [];
				run.resumed = true;
				waiting.delete(run.id);
				const settled = await serialWrite(async () => {
					const resumed = await launch(run, `${answer}${widened} ${params.message ?? ""}\nContinue your task.`, model, thinking, signal);
					return settle(run, resumed, run.check ?? projectCheck(run.cwd), model, thinking, signal);
				});
				track(run, settled);
				recordChanges(run, settled);
				// whatever the resume's outcome (done, failed, error, skipped): once no run waits, the checks deferred
				// while one did run now, and come back in this result
				const late = await serialWrite(() => runDeferred(signal));
				return reply([settled, ...late]);
			}

			if (params.chain?.length) {
				const outs: Outcome[] = [];
				let prev: string | undefined;
				const reports: string[] = [];
				for (const step of params.chain) {
					const spec = { ...step, inputs: [...(step.inputs ?? []), ...(prev ? [prev] : [])] };
					if (spec.scope?.includes("@previous")) {
						const files = filesFromReports(reports, path.resolve(ctx.cwd, step.cwd ?? "."));
						spec.scope = [...spec.scope.filter((s) => s !== "@previous"), ...files];
					}
					const out = await runSpec(spec, ctx.cwd, agents, model, thinking, signal);
					outs.push(out);
					if (out.status !== "done") {
						outs.push({ id: "-", status: out.status, text: `Chain stopped at ${out.id}; remaining steps not run.`, contextTokens: 0 });
						break;
					}
					prev = runs.get(out.id)?.report;
					if (prev) reports.push(prev);
				}
				return reply(outs);
			}

			if (params.tasks?.length) {
				if (params.tasks.length > MAX_PARALLEL_TASKS) {
					return reply([{ id: "-", status: "error", text: `At most ${MAX_PARALLEL_TASKS} parallel tasks`, contextTokens: 0 }]);
				}
				const outs: Outcome[] = new Array(params.tasks.length);
				let next = 0;
				await Promise.all(
					Array.from({ length: Math.min(MAX_CONCURRENCY, params.tasks.length) }, async () => {
						while (next < params.tasks!.length) {
							const i = next++;
							outs[i] = await runSpec(params.tasks![i], ctx.cwd, agents, model, thinking, signal);
						}
					}),
				);
				return reply(outs);
			}

			if (params.goal) {
				return reply([await runSpec(params as Spec, ctx.cwd, agents, model, thinking, signal)]);
			}
			return reply([{ id: "-", status: "error", text: "Provide goal (single), tasks, chain, or resume.", contextTokens: 0 }]);
		},
	});

	// Automatic review: before the main agent finishes a prompt that changed files, a read-only reviewer reads
	// the diff. Its one-line result comes back as a message with one more turn to act on it.
	// (event.context.canContinue describes the context before our entries; it is false whenever the assistant spoke
	// last, i.e. always here. pi re-checks after committing the custom_message, which makes continuing valid.)
	pi.on("agent_before_settle", async (event, ctx) => {
		if (process.env.PI_SUBAGENT_DEBUG) {
			fs.appendFileSync(
				process.env.PI_SUBAGENT_DEBUG,
				JSON.stringify({ t: Date.now(), canContinue: event.context?.canContinue, toolCalls, nudged, pathsChecked, msgs: event.context?.llmMessages?.length, roles: event.context?.llmMessages?.map((m) => m.role).slice(-4) }) + "\n",
			);
		}
		// a local model sometimes answers "- Read GAME.md" and stops: no tool call at all, a few words. Send it back once.
		if (toolCalls === 0 && !nudged) {
			const lastAssistant = [...event.context.llmMessages].reverse().find((m) => m.role === "assistant");
			const text = lastAssistant ? textOf(lastAssistant.content as any).trim() : "";
			// only when the answer announces an action instead of doing it; a short reply can be the whole task
			if (text.length < 300 && /^\W*(read|let me|i'?ll|i will|first|now i|next|step \d|checking|looking)\b/i.test(text)) {
				nudged = true;
				const content = "You stopped without doing the task: no tool was called and your answer is only a few words. Do the task now with your tools; do not describe the steps.";
				return { entries: [{ type: "custom_message", customType: "nudge", content, display: true }], continue: true };
			}
		}
		// a plan written without looking at any code (psx-iso: GAME.md and BACKLOG.md only, then new classes next
		// to the existing Health.cs) goes back once
		if (planOnly && !lookedAtCode && !groundNudged) {
			groundNudged = true;
			const content =
				"You planned without looking at the code. Read the files you plan to change (or ask a scout what exists, e.g. which classes already handle health or input), then redo the plan with full paths. Reuse what exists.";
			return { entries: [{ type: "custom_message", customType: "ground", content, display: true }], continue: true };
		}
		// a final answer that presents files as existing which do not exist was written without looking at the
		// code (psx-iso: a plan for "Scripts/Runtime/CrawlerAI.cs" and a new health class next to Health.cs)
		if (!pathsChecked) {
			pathsChecked = true;
			const lastAssistant = [...event.context.llmMessages].reverse().find((m) => m.role === "assistant");
			const text = lastAssistant ? textOf(lastAssistant.content as any) : "";
			const missing = new Set<string>();
			for (const line of text.split("\n")) {
				if (/\b(new|create[sd]?|creating|add(s|ed)? a)\b/i.test(line)) continue;
				if (/\b(not exist|doesn'?t exist|does not exist|missing|not found|no such)\b/i.test(line)) continue;
				for (const m of line.matchAll(/(?:~|\/)?[\w.-]+(?:\/[\w.-]+)+\.\w{1,8}\b/g)) {
					const f = m[0].replace(/^~(?=\/)/, os.homedir());
					if (f.startsWith(path.resolve(HANDOFF_ROOT))) continue;
					if (!fs.existsSync(path.resolve(ctx.cwd, f))) missing.add(m[0]);
				}
			}
			if (missing.size) {
				const content =
					`These files in your answer do not exist: ${[...missing].slice(0, 8).join(", ")}. ` +
					"Look at the actual code (read, or a scout with a question) before answering, and correct the paths and the plan. Mark files that are genuinely new as (new).";
				return { entries: [{ type: "custom_message", customType: "paths", content, display: true }], continue: true };
			}
		}
		if (waiting.size && !waitReminded) {
			waitReminded = true;
			const ids = [...waiting].join(", ");
			const content = `Subagents are still waiting for your answer and have unfinished work: ${ids}. Resume each with subagent({resume: "<id>"}) before you finish: their checks run then, and the checks deferred while they waited run once none is waiting.`;
			return { entries: [{ type: "custom_message", customType: "waiting", content, display: true }], continue: true };
		}
		// full check (tests) once the steps are done: per-step checks were the fast ones
		const full = projectFullCheck(ctx.cwd);
		let fullMoved = ""; // HEAD moved during a passing full check: goes into the review message, else its own
		if (!planOnly && full && unverified.size && fullChecks < MAX_FULL_CHECKS) {
			fullChecks++;
			const log = path.join(ensureRunDir(), `full-check-${fullChecks}.log`);
			const covered = [...unverified];
			const head = headOf(ctx.cwd);
			const res = await runCheck(full, ctx.cwd, log);
			fullMoved = headMovedNote(head, ctx.cwd);
			// the files this check covered, pass or fail (a failure is reported below); files changed after it
			// started stay unverified, also into the next prompt
			for (const f of covered) unverified.delete(f);
			if (res.code !== 0) {
				const content =
					`[full check ${fullChecks}/${MAX_FULL_CHECKS}] \`${full}\` failed (${exitText(res)}) after your steps. Last lines:\n${res.tail}\nLog: ${log}${fullMoved}\n` +
					"Fix it with a worker step (scope = the 1-2 files at fault), then finish; the full check runs again.";
				return { entries: [{ type: "custom_message", customType: "full-check", content, display: true }], continue: true };
			}
		}
		const movedOnly = () => {
			if (!fullMoved) return;
			const content = `[full check ${fullChecks}/${MAX_FULL_CHECKS}] passed.${fullMoved}`;
			return { entries: [{ type: "custom_message" as const, customType: "full-check", content, display: true }], continue: true };
		};
		if (planOnly || changed.size === 0 || reviews >= MAX_REVIEWS) return movedOnly();
		const agents = discoverAgents(ctx.cwd, "user").agents;
		if (!agents.some((a) => a.name === "reviewer")) return movedOnly();
		const files = [...changed].map((f) => path.relative(ctx.cwd, f) || f);
		changed.clear();
		reviews++;
		const model = ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : undefined;
		const out = await runSpec(
			{
				agent: "reviewer",
				goal:
					`Review the uncommitted changes to: ${files.join(", ")}. Read them with \`git diff -- <file>\` (new files: read them whole). ` +
					"List concrete defects only: bugs, ignored parameters, dead or duplicate code, behavior nobody asked for, missing edge cases, tests that do not test what they claim. " +
					"No style nits. Start with `SUMMARY: <N> issues: <the worst one>` (N = 0 when there are none).",
			},
			ctx.cwd,
			agents,
			model,
			pi.getThinkingLevel?.(),
		);
		const clean = out.status === "done" && /SUMMARY:\W*(0|no|zero)\s+(issues|problems|defects)/i.test(out.text);
		const content =
			`[auto-review ${reviews}/${MAX_REVIEWS} of ${files.join(", ")}]\n${out.text}\n` +
			(fullMoved ? `[full check ${fullChecks}/${MAX_FULL_CHECKS}] passed.${fullMoved}\n` : "") +
			(clean
				? "No issues: finish your answer to the user."
				: "Fix the real issues with a worker (scope = the affected files; read the report path for details) or tell the user why not, then finish.");
		return { entries: [{ type: "custom_message", customType: "auto-review", content, display: true }], continue: !clean || !!fullMoved };
	});

	// Context guard: the main agent cannot pull large outputs into its own context. Per call, output is capped;
	// per user prompt, direct output is budgeted, after which reads are blocked and bash output shrinks.
	const DIRECT = ["read", "bash", "grep", "find", "ls"];
	const isSkillRead = (tool: string, input: unknown) => tool === "read" && path.basename((input as { path?: string }).path ?? "") === "SKILL.md";
	const isHandoffRead = (tool: string, input: unknown, cwd: string) =>
		tool === "read" && path.resolve(cwd, (input as { path?: string }).path ?? "").startsWith(path.resolve(HANDOFF_ROOT) + path.sep);
	let spent = 0;
	let toolCalls = 0; // in this user prompt
	const bashSnaps = new Map<string, { snap: Snapshot; startSeq: number }>();
	let nudged = false;
	let pathsChecked = false;
	let lookedAtCode = false; // read a non-Markdown file, searched with bash, or ran a subagent
	let groundNudged = false;
	let waitReminded = false;
	const edited = new Set<string>();
	const nextStep = (tool: string, input: unknown, cwd: string) => {
		const p = (input as { path?: string }).path;
		const target = p ? ` in ${path.relative(cwd, path.resolve(cwd, p))}` : tool === "bash" ? ` (you tried: ${(input as { command?: string }).command?.slice(0, 80)})` : "";
		return `subagent({agent: "scout", goal: "<the question you wanted answered${target}>"}) for information, or subagent({agent: "worker", goal: "<the change>", scope: ["<files>"]}) to change files.`;
	};
	// Tools the model cannot use are not offered at all: a blocked call reads to a local model as "try a variation"
	// (29 blocked calls in one psx-iso run), a missing tool does not. The guards below stay as a fallback.
	let fullTools: string[] | undefined;
	let lastSet = "";
	const applyTools = () => {
		fullTools ??= pi.getActiveTools();
		const off = new Set<string>(["write"]); // the main agent never creates or overwrites files
		if (planOnly) off.add("edit");
		if (edited.size >= PARENT_MAX_EDIT_FILES) off.add("edit");
		if (spent >= PARENT_TURN_BUDGET) for (const t of DIRECT) off.add(t);
		const next = fullTools.filter((t) => !off.has(t));
		if (!next.includes("subagent")) next.push("subagent");
		const key = next.join(",");
		if (key !== lastSet) {
			pi.setActiveTools(next);
			lastSet = key;
		}
	};

	// Images on a prompt to a text-only session model (Flint) would be dropped: save them to the handoff area and point
	// the main agent at the looker agent (vision model). The session keeps its model; only the looker swaps.
	let images = 0;
	pi.on("input", async (event, ctx) => {
		if (!event.images?.length || ctx.model?.input.includes("image")) return { action: "continue" as const };
		const dir = ensureRunDir();
		const files = event.images.map((img) => {
			const f = path.join(dir, `image-${++images}.${(img.mimeType.split("/")[1] ?? "png").replace("jpeg", "jpg")}`);
			fs.writeFileSync(f, Buffer.from(img.data, "base64"), { mode: 0o600 });
			return f;
		});
		const text =
			`${event.text}\n\n[${files.length} image(s) attached, saved as ${files.join(", ")}. Your model cannot see images: ask the looker ` +
			`subagent what you need to know, e.g. subagent({agent: "looker", goal: "<your question about the image>", inputs: ["${files[0]}"]}).]`;
		return { action: "transform" as const, text, images: [] };
	});

	pi.on("before_agent_start", async (event) => {
		if ((event as { messageType?: string }).messageType !== "custom") {
			promptNo++;
			toolCalls = 0;
			nudged = false;
			pathsChecked = false;
			lookedAtCode = false;
			groundNudged = false;
			waitReminded = false;
			// per user prompt only: a settle hook's continuation must not reset the caps (reviews, full checks) or it loops
			spent = 0;
			edited.clear();
			changed.clear();
			reviews = 0;
			// `unverified` is kept: a full check removes the files it covered (its failure was reported), so a
			// question-only prompt does not run it again, and files no full check has seen yet are checked once
			fullChecks = 0;
			baselines.clear();
		}
		if ((event as { messageType?: string }).messageType !== "custom") planOnly = PLAN_ONLY.test((event as { prompt?: string }).prompt ?? "");
		applyTools();
	});

	pi.on("tool_call", async (event, ctx) => {
		toolCalls++;
		if (event.toolName === "subagent" || event.toolName === "grep" || event.toolName === "find") lookedAtCode = true;
		if (event.toolName === "read" && !/\.(md|txt|toml)$/i.test((event.input as { path?: string }).path ?? "")) lookedAtCode = true;
		if (event.toolName === "bash" && /\b(grep|rg|find|ls|cat|sed|head)\b/.test((event.input as { command?: string }).command ?? "")) lookedAtCode = true;
		if (event.toolName === "bash") {
			const cmd = (event.input as { command: string }).command;
			if (GIT_WRITE.test(cmd)) {
				return { block: true, reason: "You do not change git state (commit, add, reset, checkout, stash, ...); the user owns git." };
			}
		}
		if (planOnly && (event.toolName === "edit" || event.toolName === "write")) return { block: true, reason: PLAN_ONLY_REASON };
		if (planOnly && event.toolName === "bash" && !isCheckCommand((event.input as { command: string }).command, checks)) {
			// quoted text is data, not shell syntax: grep 'GetComponent<Health>' is not a redirect
			const cmd = (event.input as { command: string }).command.replace(/'[^']*'|"(?:\\.|[^"\\])*"/g, "''");
			if (/(^|[^<>2&])>[^&]|\b(sed\s+-i|tee|rm|mv|cp|mkdir|touch|chmod|ln|truncate|patch|git\s+(commit|reset|checkout|stash|add|apply))\b/.test(cmd)) {
				return { block: true, reason: PLAN_ONLY_REASON };
			}
		}
		if (event.toolName === "write" && !isHandoffRead("read", event.input, ctx.cwd)) {
			return {
				block: true,
				reason: "You do not create or overwrite files. Delegate it to a worker subagent whose scope lists the new file; you may make small edits to existing files.",
			};
		}
		if (event.toolName === "edit" || event.toolName === "write") {
			const file = path.resolve(ctx.cwd, (event.input as { path: string }).path);
			if (!edited.has(file) && edited.size >= PARENT_MAX_EDIT_FILES) {
				return {
					block: true,
					reason:
						`You already edited ${PARENT_MAX_EDIT_FILES} files in this prompt. Next step, exactly: ` +
						`subagent({agent: "worker", goal: "<the change you were making, self-contained>", scope: ["${path.relative(ctx.cwd, file)}"]})`,
				};
			}
			edited.add(file);
			changed.add(file);
			unverified.add(file);
			applyTools();
			return;
		}
		if (!DIRECT.includes(event.toolName)) return;
		const isCheck = event.toolName === "bash" && isCheckCommand((event.input as { command: string }).command, checks);
		if (spent >= PARENT_TURN_BUDGET && !isCheck && !isHandoffRead(event.toolName, event.input, ctx.cwd) && !isSkillRead(event.toolName, event.input)) {
			return {
				block: true,
				reason:
					`Context budget for this prompt is used up (${PARENT_TURN_BUDGET} bytes of direct output); only check commands still run. Next step, exactly: ` +
					nextStep(event.toolName, event.input, ctx.cwd),
			};
		}
		if (event.toolName === "read" && !isSkillRead(event.toolName, event.input)) {
			const input = event.input as { limit?: number };
			if (!input.limit || input.limit > PARENT_READ_MAX_LINES) input.limit = PARENT_READ_MAX_LINES;
		}
		// the main agent's bash may run things, not change files: snapshot now (the call is allowed), undo any file
		// change after
		if (event.toolName === "bash") {
			const snap = snapshot(ctx.cwd);
			if (snap) bashSnaps.set(event.toolCallId, { snap, startSeq: seq });
		}
	});

	pi.on("tool_result", async (event, ctx) => {
		let undoNote = "";
		const entry = event.toolName === "bash" ? bashSnaps.get(event.toolCallId) : undefined;
		if (entry) {
			bashSnaps.delete(event.toolCallId);
			// a subagent running (or finished) alongside this call changes its scope meanwhile: that is not this bash's
			// doing, so those files are left to the subagent's own scope check
			try {
				const { fixed: undone, headMoved } = enforceScope(entry.snap, concurrentScopes(entry.startSeq), ctx.cwd, path.join(ensureRunDir(), "quarantine", "main-bash"));
				if (undone.length) {
					undoNote = `\n[undone: your bash changed ${undone.join(", ")}. Change existing files with edit (at most ${PARENT_MAX_EDIT_FILES}) or delegate to a worker with a scope.]`;
				}
				if (headMoved) undoNote += `\n[HEAD moved during this call (${headMoved}); not undone. Tell the user.]`;
			} catch (e) {
				undoNote = `\n[file-change check failed: ${(e as Error).message.slice(0, 120)}]`;
			}
		}
		if (!DIRECT.includes(event.toolName)) return undoNote ? { content: [...(event.content as any), { type: "text" as const, text: undoNote }] } : undefined;
		const text = textOf(event.content as any);
		const bytes = Buffer.byteLength(text, "utf8");
		const handoff = isHandoffRead(event.toolName, event.input, ctx.cwd);
		const skill = isSkillRead(event.toolName, event.input);
		const cap = handoff
			? PARENT_HANDOFF_READ_CAP
			: skill
				? PARENT_SKILL_READ_CAP
				: spent >= PARENT_TURN_BUDGET
					? PARENT_BASH_AFTER_BUDGET
					: PARENT_RESULT_CAP;
		const keptBytes = Math.min(bytes, cap);
		if (!handoff && !skill) spent += keptBytes; // reports and skills never use up the budget
		applyTools();
		if (bytes <= cap) return undoNote ? { content: [{ type: "text" as const, text: text + undoNote }] } : undefined;
		const kept = Buffer.from(text, "utf8").subarray(0, cap).toString("utf8");
		return {
			content: [
				{
					type: "text" as const,
					text: `${kept}\n[context guard: ${bytes - cap} bytes omitted. Delegate broad reading to a scout subagent.]${undoNote}`,
				},
			],
		};
	});
}

export default function (pi: ExtensionAPI) {
	if (!HARNESS_ON) return; // PI_HARNESS=off or .pi/harness.json {"enabled": false}: plain pi
	if (IS_CHILD) childSide(pi);
	else parentSide(pi);
}
