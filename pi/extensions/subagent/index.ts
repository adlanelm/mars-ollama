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

const IS_CHILD = process.env.PI_SUBAGENT === "1";
const HANDOFF_ROOT = process.env.PI_HANDOFF_ROOT || "/tmp/pi-handoff";
const MAX_CONCURRENCY = 2; // llama-swap serves -np 2 slots per model
const MAX_PARALLEL_TASKS = 4;
const SUMMARY_CAP = 600; // chars of the report returned inline to the main agent
const PARENT_READ_MAX_LINES = 200; // main agent: read calls are clamped to this many lines
const PARENT_RESULT_CAP = 6 * 1024; // main agent: bytes per read/bash/grep/find/ls result
const PARENT_TURN_BUDGET = 48 * 1024; // main agent: direct output bytes per user prompt, then reads are blocked
const PARENT_BASH_AFTER_BUDGET = 1024; // check commands still work after the budget, with this much output
const PARENT_MAX_EDIT_FILES = 2; // main agent: distinct files it may edit/write per user prompt; more = delegate
const PARENT_SKILL_READ_CAP = 16 * 1024; // main agent: SKILL.md files are instructions, read whole, not budgeted
const PARENT_HANDOFF_READ_CAP = 4 * 1024; // main agent: handoff reports stay readable after the budget, capped
const MAX_REVIEWS = 2; // automatic reviews per user prompt (a second one after fixes)
const CHECK_RETRIES = 2; // a failed `check` resumes the subagent with the failure this many times
const CHECK_TIMEOUT_MS = Number(process.env.PI_SUBAGENT_CHECK_TIMEOUT_MS) || 30 * 60 * 1000;
const CHECK_TAIL_LINES = 15;

// after the budget, bash is limited to checks (no sed/cat paging through files)
const CHECK_CORE = /^(\.\/)?(Tools\/check|git (status|diff --stat|log --oneline)|python3? -m pytest|pytest|npm (test|run (test|check|lint))|cargo (test|check)|go test|dotnet test|make (test|check))\b[^;&|<>]*$/;

/** True for a check command, also when wrapped: `cd X &&`, `timeout N`, `bash`, `2>&1`, `> log`, `| tail -N`. */
function isCheckCommand(cmd: string, extra: Set<string>): boolean {
	if (extra.has(cmd.trim())) return true;
	let c = cmd.trim();
	c = c.replace(/^cd\s+\S+\s*&&\s*/, "");
	c = c.replace(/^timeout\s+\d+[smh]?\s+/, "");
	c = c.replace(/^(bash|sh)\s+/, "");
	for (let prev = ""; prev !== c; ) {
		prev = c;
		c = c.replace(/\s*(2>&1|>\s*\S+|\|\s*(tail|head)(\s+-n)?(\s+-?\d+)?)\s*$/, "");
	}
	return extra.has(c) || CHECK_CORE.test(c);
}
const ALL_TOOLS = ["read", "bash", "edit", "write", "grep", "find", "ls"];
const PLAN_ONLY = /\[plan-only\]/i;
const PLAN_ONLY_REASON =
	"This prompt is plan-only: no edits, no writing subagents. Present the plan (files and why) and stop; the user replies with OK to implement.";

// ---------- shared helpers ----------

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
	return "";
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

// untracked build artefacts a subagent's check run may create; never treated as scope violations
const ARTEFACT = /(^|\/)(__pycache__|\.pytest_cache|\.mypy_cache|node_modules|\.venv|target|build|dist|obj|Library|Temp|Logs|\.godot)(\/|$)|\.pyc$/;

/** `.pi/check` in the task cwd or a parent (up to the git root): the project's check, run as `bash <file>`. */
function projectCheck(cwd: string): string | undefined {
	let root = "/";
	try {
		root = git(cwd, ["rev-parse", "--show-toplevel"]).trim();
	} catch {}
	for (let d = path.resolve(cwd); ; d = path.dirname(d)) {
		const f = path.join(d, ".pi", "check");
		if (fs.existsSync(f) && fs.statSync(f).isFile()) return `bash ${f}`;
		if (d === root || d === path.dirname(d)) return undefined;
	}
}

function runCheck(cmd: string, cwd: string, log: string, signal?: AbortSignal): Promise<{ code: number; tail: string }> {
	return new Promise((resolve) => {
		const proc = spawn("bash", ["-c", cmd], { cwd, stdio: ["ignore", "pipe", "pipe"] });
		let out = "";
		proc.stdout.on("data", (d) => (out += d.toString()));
		proc.stderr.on("data", (d) => (out += d.toString()));
		const kill = () => proc.kill("SIGTERM");
		const timer = setTimeout(kill, CHECK_TIMEOUT_MS);
		signal?.addEventListener("abort", kill, { once: true });
		proc.on("close", (code) => {
			clearTimeout(timer);
			signal?.removeEventListener("abort", kill);
			fs.writeFileSync(log, out);
			const tail = out.trimEnd().split("\n").slice(-CHECK_TAIL_LINES).join("\n").slice(-1536);
			resolve({ code: code ?? 1, tail });
		});
	});
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

/** Restores files changed outside scope since the snapshot; new untracked files are moved to quarantine. */
function enforceScope(snap: Snapshot, scope: string[], cwd: string, quarantine: string): string[] {
	const fixed: string[] = [];
	let tracked: Set<string>;
	try {
		// commits would hide changes from `git status`: move HEAD back, keeping the changes in the worktree
		if (snap.head && git(snap.root, ["rev-parse", "HEAD"]).trim() !== snap.head) {
			git(snap.root, ["reset", "-q", "--soft", snap.head]);
			fixed.push(`commits since ${snap.head.slice(0, 8)} (undone with reset --soft; changes kept and checked below)`);
		}
		tracked = new Set(snap.head ? git(snap.root, ["ls-tree", "-r", "-z", "--name-only", "HEAD"]).split("\0").filter(Boolean) : []);
	} catch {
		return fixed;
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
	return fixed;
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

function childSide(pi: ExtensionAPI) {
	const id = process.env.PI_SUBAGENT_ID ?? "subagent";
	const dir = process.env.PI_HANDOFF_DIR ?? HANDOFF_ROOT;
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

	pi.on("tool_call", async (event, ctx) => {
		if (event.toolName === "bash") {
			const cmd = (event.input as { command: string }).command;
			if (GIT_WRITE.test(cmd)) {
				return {
					block: true,
					reason: "Subagents do not change git state (commit, reset, checkout, stash, ...). The main agent and the user own git; edit files in your scope instead.",
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
	touched?: Set<string>; // files this run changed (all launches); undefined outside git = unknown
}

interface Outcome {
	id: string;
	status: "done" | "needs" | "error" | "failed";
	text: string;
	contextTokens: number;
	out?: string; // done: the subagent's final message
	scopeNote?: string;
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

function parentSide(pi: ExtensionAPI) {
	const runs = new Map<string, Run>();
	// parallel runs share the worktree: a run's scope check must not undo a concurrent run's in-scope work
	const active = new Set<Run>();
	let seq = 0;
	const finished: { seq: number; scope: string[] }[] = [];
	let runDir: string | undefined;
	let counter = 0;
	// files changed in this prompt (worker scopes and the main agent's own edits) and not reviewed since
	const changed = new Set<string>();
	let reviews = 0;
	// a prompt containing [plan-only] (e.g. /plan, /backlog-item) may read and plan but not change anything
	let planOnly = false;
	const checks = new Set<string>(); // check commands given to subagents; the main agent may run them too

	const ensureRunDir = () => {
		if (!runDir) {
			const stamp = new Date().toISOString().slice(5, 16).replace(/[-:T]/g, "");
			runDir = path.join(HANDOFF_ROOT, `${stamp}-${process.pid}`);
			fs.mkdirSync(runDir, { recursive: true, mode: 0o700 });
		}
		return runDir;
	};

	function launch(run: Run, message: string, model: string | undefined, thinking: string | undefined, signal?: AbortSignal) {
		const tools = [...(run.agent.tools ?? ALL_TOOLS).filter((t) => t !== "request"), "request"];
		const args = ["--mode", "json", "-p", "--session", run.session, "--tools", tools.join(",")];
		const model_ = run.agent.model ?? model;
		if (model_) args.push("--model", model_);
		if (!run.agent.model && thinking) args.push("--thinking", thinking);
		args.push("--append-system-prompt", run.promptFile, message);

		const snap = snapshot(run.cwd);
		const startSeq = seq;
		active.add(run);
		return new Promise<Outcome>((resolve) => {
			const messages: Message[] = [];
			let contextTokens = 0;
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
				},
			});
			const onLine = (line: string) => {
				let ev: any;
				try {
					ev = JSON.parse(line);
				} catch {
					return;
				}
				if (ev.type === "message_end" && ev.message) {
					messages.push(ev.message);
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
				const allowed = [
					...run.scope,
					...[...active].flatMap((r) => r.scope),
					...finished.filter((f) => f.seq > startSeq).flatMap((f) => f.scope),
				];
				finished.push({ seq: ++seq, scope: run.scope });
				let violations: string[] = [];
				try {
					if (snap) violations = enforceScope(snap, allowed, run.cwd, path.join(path.dirname(run.report), "quarantine", run.id));
				} catch (e) {
					violations = [`scope check failed (${(e as Error).message.slice(0, 200)}); review git status`];
				}
				const shown = violations.slice(0, 5).join(", ") + (violations.length > 5 ? `, +${violations.length - 5} more` : "");
				const scopeNote = violations.length
					? `\n  SCOPE VIOLATION, undone: ${shown}. Widen the scope (resume with scope) or delegate those files.`
					: "";
				const reqFile = path.join(path.dirname(run.report), `${run.id}.request.json`);
				if (fs.existsSync(reqFile)) {
					let req = { need: "?", why: "?" };
					try {
						req = JSON.parse(fs.readFileSync(reqFile, "utf8"));
					} catch {}
					fs.renameSync(reqFile, reqFile.replace(/\.json$/, `.${Date.now()}.json`));
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
				if (code !== 0 || !out) {
					resolve({
						id: run.id,
						status: "error",
						contextTokens,
						text: `[${run.id}] ERROR (exit ${code}): ${(stderr || "no output").trim().slice(-SUMMARY_CAP)}${scopeNote}`,
					});
					return;
				}
				fs.writeFileSync(run.report, out + "\n");
				if (snap) {
					run.touched ??= new Set();
					try {
						for (const f of touchedFiles(snap)) if (!ARTEFACT.test(f) && !f.endsWith(".meta")) run.touched.add(f);
					} catch {}
				}
				resolve({ id: run.id, status: "done", contextTokens, out, scopeNote, text: doneText(run, out, scopeNote) });
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
	async function settle(run: Run, outcome: Outcome, check: string | undefined, model?: string, thinking?: string, signal?: AbortSignal) {
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
			if (!check) return outcome;
			// nothing changed (a read-only task given a scope): the check cannot have changed either
			if (run.touched && run.touched.size === 0) return { ...outcome, text: `${outcome.text}\n  check: skipped (no files changed)` };
			const log = path.join(path.dirname(run.report), `${run.id}.check${attempt ? `-${attempt}` : ""}.log`);
			const res = await runCheck(check, run.cwd, log, signal);
			if (res.code === 0) {
				const last = res.tail.split("\n").slice(-2).join(" | ");
				return { ...outcome, text: `${outcome.text}\n  check: exit 0 (${last.slice(0, 300)})` };
			}
			if (attempt >= CHECK_RETRIES) {
				const last = res.tail.split("\n").slice(-4).join("\n    ");
				return {
					...outcome,
					status: "failed",
					text: `${outcome.text}\n  check: FAILED (exit ${res.code}) after ${CHECK_RETRIES} retries, log: ${log}\n    ${last.slice(0, 600)}`,
				};
			}
			outcome = await launch(
				run,
				`The check \`${check}\` failed (exit ${res.code}). Its last lines:\n${res.tail}\nFull log: ${log}\nFix the cause within your scope, then report again.`,
				model,
				thinking,
				signal,
			);
		}
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
		const g = spec.goal.trim();
		if (g.length > 200 || !/\b(return|show|give|read|print|output|fetch|get)\b[^.]*\b(report|content|text)\b/i.test(g)) return undefined;
		// any file named in the goal must be a handoff report; "read the content of README.md" is real work
		const files = g.match(/[\w./~-]*\.\w{1,5}\b/g) ?? [];
		const handoffRoot = path.resolve(HANDOFF_ROOT) + path.sep;
		if (files.some((f) => !f.startsWith(handoffRoot))) return undefined;
		const named = files[0];
		const latest = [...runs.values()].reverse().find((r) => fs.existsSync(r.report))?.report;
		const file = named && named.startsWith(path.resolve(HANDOFF_ROOT) + path.sep) ? named : !named ? latest : undefined;
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
			const name = spec.agent ?? (spec.scope?.length ? "worker" : "scout");
			const a = agents.find((x) => x.name === name);
			const writes = !a?.tools || a.tools.some((t) => t === "edit" || t === "write");
			if (writes) return { id: "-", status: "error", text: PLAN_ONLY_REASON, contextTokens: 0 } as Outcome;
			spec = { ...spec, agent: name, scope: [] }; // read-only agent: a scope means nothing, drop it
		}
		const run = newRun(spec, defaultCwd, agents);
		if (typeof run === "string") return { id: "-", status: "error", text: run, contextTokens: 0 } as Outcome;
		// a writing task without a check gets the project's check (.pi/check), so verification never depends on
		// the main agent remembering to ask for it
		const canWrite = !run.agent.tools || run.agent.tools.some((t) => t === "edit" || t === "write");
		// the project check always runs for writing tasks; a task check adds to it, never replaces it (psx-iso: the
		// main agent passed check: "echo done" for a worker editing 3 files)
		const pc = run.scope.length && canWrite ? projectCheck(run.cwd) : undefined;
		if (pc && spec.check?.trim() !== pc) spec = { ...spec, check: spec.check ? `${pc} && { ${spec.check}; }` : pc };
		if (spec.check) checks.add(spec.check.trim());
		run.check = spec.check;
		const outcome = await settle(run, await launch(run, taskMessage(spec, run), model, thinking, signal), spec.check, model, thinking, signal);
		if (spec.for && outcome.status === "done") runs.get(spec.for)!.answers.push(run.report);
		if (outcome.status === "done" && canWrite) for (const f of run.touched ?? run.scope) changed.add(f);
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
			"Give each subagent ONE narrow goal with exact paths, a writable scope (only the files it should change; none for scouts/reviewers) and `check`: the command that proves the work (tests, build check). The extension runs it, retries the subagent on failure and tells you the exit code.",
			"Trust the check result, not a subagent's claims. Do not re-run checks yourself after a passing `check`.",
			...(projectCheck(process.cwd())
				? [
						"This project has .pi/check (its build check and tests). It runs automatically after every subagent that changes files and cannot be replaced; pass `check` only for something it does not cover. When the user asks for checks to pass, that is what verifies them.",
					]
				: []),
			"Pass information between subagents as handoff report paths in inputs; read a report yourself only when you must decide something from it (handoff reads always work, capped).",
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
				const answer = run.answers.length ? `Your request was handled. Read: ${run.answers.join(", ")}.` : "Your request was handled.";
				run.answers = [];
				const resumed = await launch(run, `${answer}${widened} ${params.message ?? ""}\nContinue your task.`, model, thinking, signal);
				return reply([await settle(run, resumed, run.check, model, thinking, signal)]);
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
		if (planOnly || changed.size === 0 || reviews >= MAX_REVIEWS) return;
		const agents = discoverAgents(ctx.cwd, "user").agents;
		if (!agents.some((a) => a.name === "reviewer")) return;
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
			(clean
				? "No issues: finish your answer to the user."
				: "Fix the real issues with a worker (scope = the affected files; read the report path for details) or tell the user why not, then finish.");
		return { entries: [{ type: "custom_message", customType: "auto-review", content, display: true }], continue: !clean };
	});

	// Context guard: the main agent cannot pull large outputs into its own context. Per call, output is capped;
	// per user prompt, direct output is budgeted, after which reads are blocked and bash output shrinks.
	const DIRECT = ["read", "bash", "grep", "find", "ls"];
	const isSkillRead = (tool: string, input: unknown) => tool === "read" && path.basename((input as { path?: string }).path ?? "") === "SKILL.md";
	const isHandoffRead = (tool: string, input: unknown, cwd: string) =>
		tool === "read" && path.resolve(cwd, (input as { path?: string }).path ?? "").startsWith(path.resolve(HANDOFF_ROOT) + path.sep);
	let spent = 0;
	let toolCalls = 0; // in this user prompt
	const bashSnaps = new Map<string, Snapshot>();
	let nudged = false;
	let pathsChecked = false;
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

	pi.on("before_agent_start", async (event) => {
		if ((event as { messageType?: string }).messageType !== "custom") {
			toolCalls = 0;
			nudged = false;
			pathsChecked = false;
		}
		spent = 0;
		edited.clear();
		changed.clear();
		reviews = 0;
		if ((event as { messageType?: string }).messageType !== "custom") planOnly = PLAN_ONLY.test((event as { prompt?: string }).prompt ?? "");
		applyTools();
	});

	pi.on("tool_call", async (event, ctx) => {
		toolCalls++;
		if (event.toolName === "bash") {
			const cmd = (event.input as { command: string }).command;
			if (GIT_WRITE.test(cmd)) {
				return { block: true, reason: "You do not change git state (commit, add, reset, checkout, stash, ...); the user owns git." };
			}
			// the main agent's bash may run things, not change files: snapshot now, undo any file change after
			const snap = snapshot(ctx.cwd);
			if (snap) bashSnaps.set(event.toolCallId, snap);
		}
		if (planOnly && (event.toolName === "edit" || event.toolName === "write")) return { block: true, reason: PLAN_ONLY_REASON };
		if (planOnly && event.toolName === "bash" && !isCheckCommand((event.input as { command: string }).command, checks)) {
			const cmd = (event.input as { command: string }).command;
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
	});

	pi.on("tool_result", async (event, ctx) => {
		let undoNote = "";
		const snap = event.toolName === "bash" ? bashSnaps.get(event.toolCallId) : undefined;
		if (snap) {
			bashSnaps.delete(event.toolCallId);
			try {
				const undone = enforceScope(snap, [], ctx.cwd, path.join(ensureRunDir(), "quarantine", "main-bash"));
				if (undone.length) {
					undoNote = `\n[undone: your bash changed ${undone.join(", ")}. Change existing files with edit (at most ${PARENT_MAX_EDIT_FILES}) or delegate to a worker with a scope.]`;
				}
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
	if (IS_CHILD) childSide(pi);
	else parentSide(pi);
}
