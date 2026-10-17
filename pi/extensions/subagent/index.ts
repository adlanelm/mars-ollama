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
const PARENT_TURN_BUDGET = 24 * 1024; // main agent: direct output bytes per user prompt, then reads are blocked
const PARENT_BASH_AFTER_BUDGET = 1024; // bash still works for checks, with this much output
const ALL_TOOLS = ["read", "bash", "edit", "write", "grep", "find", "ls"];

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

function inScope(file: string, cwd: string, scope: string[]): boolean {
	const abs = path.resolve(cwd, file);
	return scope.some((s) => abs === s || abs.startsWith(s.endsWith(path.sep) ? s : s + path.sep));
}

// ---------- scope check by git (catches bash writes too) ----------

interface Snapshot {
	root: string;
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
		const dirty = new Map<string, string | null>();
		for (const p of changedPaths(root)) {
			const abs = path.join(root, p);
			dirty.set(p, fs.existsSync(abs) && fs.statSync(abs).isFile() ? fs.readFileSync(abs, "latin1") : null);
		}
		return { root, dirty };
	} catch {
		return undefined; // not a git repo: only the edit/write guard applies
	}
}

const hash = (s: string | null) => (s === null ? "-" : crypto.createHash("sha1").update(s, "latin1").digest("hex"));

/** Restores files changed outside scope since the snapshot; new untracked files are moved to quarantine. */
function enforceScope(snap: Snapshot, scope: string[], cwd: string, quarantine: string): string[] {
	const fixed: string[] = [];
	let tracked: Set<string>;
	try {
		tracked = new Set(git(snap.root, ["ls-files", "-z"]).split("\0").filter(Boolean));
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
			git(snap.root, ["checkout", "--", p]);
			fixed.push(`${p} (restored from HEAD)`);
		} else if (now !== null && !ARTEFACT.test(p)) {
			const dest = path.join(quarantine, p);
			fs.mkdirSync(path.dirname(dest), { recursive: true });
			fs.renameSync(abs, dest);
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
}

interface Outcome {
	id: string;
	status: "done" | "needs" | "error";
	text: string;
	contextTokens: number;
}

const TaskSpec = Type.Object({
	agent: Type.String({ description: "Agent name" }),
	goal: Type.String({ description: "The narrow task, self-contained: the subagent has not seen this conversation" }),
	scope: Type.Optional(
		Type.Array(Type.String(), {
			description: 'Files or directories the subagent may edit/write. Omit for read-only tasks. In a chain, "@previous" means the FILES: line of the latest earlier report.',
		}),
	),
	inputs: Type.Optional(Type.Array(Type.String(), { description: "Handoff reports or files to read first" })),
	done_when: Type.Optional(Type.String({ description: "Acceptance check, e.g. a command that must pass" })),
	for: Type.Optional(Type.String({ description: "Run id whose request this task answers; its report is handed to that run on resume" })),
	cwd: Type.Optional(Type.String()),
});

const Params = Type.Object({
	agent: Type.Optional(Type.String()),
	goal: Type.Optional(Type.String()),
	scope: Type.Optional(Type.Array(Type.String())),
	inputs: Type.Optional(Type.Array(Type.String())),
	done_when: Type.Optional(Type.String()),
	for: Type.Optional(Type.String()),
	cwd: Type.Optional(Type.String()),
	tasks: Type.Optional(Type.Array(TaskSpec, { description: `Independent tasks, run ${MAX_CONCURRENCY} at a time` })),
	chain: Type.Optional(Type.Array(TaskSpec, { description: "Sequential tasks; each step gets the previous report as input" })),
	resume: Type.Optional(Type.String({ description: "Run id of a subagent that stopped with a request" })),
	// with resume, `scope` replaces the run's writable scope
	message: Type.Optional(Type.String({ description: "With resume: extra note for the subagent" })),
});

type Spec = { agent: string; goal: string; scope?: string[]; inputs?: string[]; done_when?: string; for?: string; cwd?: string };

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
				const violations = snap ? enforceScope(snap, allowed, run.cwd, path.join(path.dirname(run.report), "quarantine", run.id)) : [];
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
				// SUMMARY line if the agent wrote one, else the first line that is not a heading or blank
				const summary = (
					out.match(/^SUMMARY:.*$/m)?.[0] ??
					out.split("\n").find((l) => l.trim() && !/^\s*(#|---)/.test(l)) ??
					""
				).slice(0, SUMMARY_CAP);
				resolve({ id: run.id, status: "done", contextTokens, text: `[${run.id}] done, report: ${run.report}\n  ${summary}${scopeNote}` });
			});
		});
	}

	function newRun(spec: Spec, defaultCwd: string, agents: AgentConfig[]): Run | string {
		const agent = agents.find((a) => a.name === spec.agent);
		if (!agent) return `Unknown agent "${spec.agent}". Available: ${agents.map((a) => a.name).join(", ")}`;
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
		lines.push(`WRITABLE SCOPE: ${run.scope.length ? run.scope.join(", ") : "none (read-only)"}`);
		if (spec.inputs?.length) lines.push(`INPUTS (read these first): ${spec.inputs.join(", ")}`);
		lines.push(
			"REPORT FORMAT: your final message must start with the line `SUMMARY: <at most 25 words>`, then `FILES: <comma-separated paths you changed, or for a plan the paths the change must touch, or none>`, then details.",
		);
		return lines.join("\n");
	}

	async function runSpec(spec: Spec, defaultCwd: string, agents: AgentConfig[], model?: string, thinking?: string, signal?: AbortSignal) {
		const run = newRun(spec, defaultCwd, agents);
		if (typeof run === "string") return { id: "-", status: "error", text: run, contextTokens: 0 } as Outcome;
		const outcome = await launch(run, taskMessage(spec, run), model, thinking, signal);
		if (spec.for && outcome.status === "done") runs.get(spec.for)!.answers.push(run.report);
		return outcome;
	}

	const agentsAtLoad = discoverAgents(process.cwd(), "user").agents;

	pi.registerTool({
		name: "subagent",
		label: "Subagent",
		description: [
			"Delegate a narrow task to a subagent with a fresh context. Results are written to handoff files; you get a status, the report path and a one-line summary.",
			`Agents: ${agentsAtLoad.map((a) => `${a.name} (${a.description})`).join("; ")}.`,
			"Modes: single (agent+goal), tasks (parallel, independent), chain (sequential, each step gets the previous report), resume (continue a subagent that stopped with NEEDS).",
		].join(" "),
		parameters: Params,
		promptGuidelines: [
			"Your context is small and compaction loses detail. Keep this conversation for decisions; subagents do the reading and the doing. Your own read and bash output is capped.",
			"Give each subagent ONE narrow goal with exact paths, a writable scope (only the files it should change; none for scouts/reviewers) and done_when (the check command).",
			"Pass information between subagents as handoff report paths in inputs; do not read reports yourself unless you must decide something from them.",
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
				if (!run) return reply([{ id: params.resume, status: "error", text: `Unknown run id "${params.resume}"`, contextTokens: 0 }]);
				if (params.scope) {
					run.scope = params.scope.map((s) => path.resolve(run.cwd, s));
					fs.writeFileSync(run.promptFile, run.agent.systemPrompt + CONTRACT(run.id, run.scope), { mode: 0o600 });
				}
				const widened = params.scope ? ` Your writable scope is now: ${run.scope.join(", ")}.` : "";
				const answer = run.answers.length ? `Your request was handled. Read: ${run.answers.join(", ")}.` : "Your request was handled.";
				run.answers = [];
				return reply([await launch(run, `${answer}${widened} ${params.message ?? ""}\nContinue your task.`, model, thinking, signal)]);
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

			if (params.agent && params.goal) {
				return reply([await runSpec(params as Spec, ctx.cwd, agents, model, thinking, signal)]);
			}
			return reply([{ id: "-", status: "error", text: "Provide agent+goal, tasks, chain, or resume.", contextTokens: 0 }]);
		},
	});

	// Context guard: the main agent cannot pull large outputs into its own context. Per call, output is capped;
	// per user prompt, direct output is budgeted, after which reads are blocked and bash output shrinks.
	const DIRECT = ["read", "bash", "grep", "find", "ls"];
	let spent = 0;
	pi.on("before_agent_start", async () => {
		spent = 0;
	});

	pi.on("tool_call", async (event) => {
		if (!DIRECT.includes(event.toolName)) return;
		if (spent >= PARENT_TURN_BUDGET && event.toolName !== "bash") {
			return {
				block: true,
				reason: `Context budget for this prompt is used up (${PARENT_TURN_BUDGET} bytes of direct output). Delegate the reading to a scout subagent.`,
			};
		}
		if (event.toolName === "read") {
			const input = event.input as { limit?: number };
			if (!input.limit || input.limit > PARENT_READ_MAX_LINES) input.limit = PARENT_READ_MAX_LINES;
		}
	});

	pi.on("tool_result", async (event) => {
		if (!DIRECT.includes(event.toolName)) return;
		const text = textOf(event.content as any);
		const bytes = Buffer.byteLength(text, "utf8");
		const cap = spent >= PARENT_TURN_BUDGET ? PARENT_BASH_AFTER_BUDGET : PARENT_RESULT_CAP;
		const keptBytes = Math.min(bytes, cap);
		spent += keptBytes;
		if (bytes <= cap) return;
		const kept = Buffer.from(text, "utf8").subarray(0, cap).toString("utf8");
		return {
			content: [
				{
					type: "text" as const,
					text: `${kept}\n[context guard: ${bytes - cap} bytes omitted. Delegate broad reading to a scout subagent.]`,
				},
			],
		};
	});
}

export default function (pi: ExtensionAPI) {
	if (IS_CHILD) childSide(pi);
	else parentSide(pi);
}
