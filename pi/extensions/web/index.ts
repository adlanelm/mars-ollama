/**
 * Web access through the searxng instance on jupiter (homelab chart openwebui-searxng, Traefik entrypoint :8888).
 *
 *   web_search  searxng JSON API: titles, URLs and short snippets
 *   web_fetch   one page as plain text, capped, paged with offset
 *
 * jupiter's certificate comes from step-ca, which Node's bundled CA list does not know: requests trust the system
 * store as well (tls.getCACertificates("system")), without NODE_USE_SYSTEM_CA for the whole process.
 * Output is capped here: the main session's context budget only counts read/bash/grep/find/ls.
 * Agents get the tools through their `tools:` list (scout, planner); the main session has them by default.
 */

import * as http from "node:http";
import * as https from "node:https";
import * as tls from "node:tls";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const SEARXNG = (process.env.PI_SEARXNG_URL ?? "https://jupiter.internal:8888").replace(/\/+$/, "");
const TIMEOUT_MS = 15_000;
const MAX_REDIRECTS = 5;
const MAX_BODY = 4 * 1024 * 1024; // raw bytes read from a page; the text sent back is capped by FETCH_CHARS
const FETCH_CHARS = 12 * 1024;
const SNIPPET_CHARS = 300;
const DEFAULT_RESULTS = 8;
const MAX_RESULTS = 15;

const CA = [...tls.getCACertificates("default"), ...tls.getCACertificates("system")];

type Page = { url: string; status: number; type: string; body: string };

function get(url: string, redirects = 0): Promise<Page> {
	return new Promise((resolve, reject) => {
		const u = new URL(url);
		const mod = u.protocol === "https:" ? https : http;
		const req = mod.get(u, { ca: CA, timeout: TIMEOUT_MS, headers: { "user-agent": "pi-web/1", accept: "text/html,application/json,text/plain;q=0.9,*/*;q=0.5" } }, (res) => {
			const status = res.statusCode ?? 0;
			if (status >= 300 && status < 400 && res.headers.location) {
				res.resume();
				if (redirects >= MAX_REDIRECTS) return reject(new Error(`more than ${MAX_REDIRECTS} redirects`));
				return resolve(get(new URL(res.headers.location, u).toString(), redirects + 1));
			}
			const chunks: Buffer[] = [];
			let size = 0;
			res.on("data", (c: Buffer) => {
				if (size >= MAX_BODY) return;
				chunks.push(c);
				size += c.length;
				if (size >= MAX_BODY) res.destroy();
			});
			const done = () => resolve({ url: u.toString(), status, type: String(res.headers["content-type"] ?? ""), body: Buffer.concat(chunks).toString("utf8") });
			res.on("end", done);
			res.on("close", done);
			res.on("error", reject);
		});
		req.on("timeout", () => req.destroy(new Error(`no answer within ${TIMEOUT_MS / 1000} s`)));
		req.on("error", reject);
	});
}

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ", mdash: "—", ndash: "–", hellip: "…", copy: "©" };

function decode(s: string): string {
	return s.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (m, e: string) => {
		if (e[0] === "#") {
			const n = e[1] === "x" || e[1] === "X" ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10);
			return Number.isFinite(n) && n > 0 && n < 0x110000 ? String.fromCodePoint(n) : m;
		}
		return ENTITIES[e.toLowerCase()] ?? m;
	});
}

/** HTML to readable text: page chrome and code-free tags dropped, blocks on their own lines. */
function htmlText(html: string): string {
	const title = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1]?.trim();
	const body = html
		.replace(/<!--[\s\S]*?-->/g, "")
		.replace(/<(script|style|noscript|svg|nav|header|footer|aside|form|template|iframe)\b[\s\S]*?<\/\1>/gi, "")
		.replace(/<(br|hr)\b[^>]*>/gi, "\n")
		.replace(/<li\b[^>]*>/gi, "\n- ")
		.replace(/<h([1-6])\b[^>]*>/gi, (_m, n: string) => `\n\n${"#".repeat(Number(n))} `)
		.replace(/<\/?(p|div|section|article|main|ul|ol|table|tr|pre|blockquote|h[1-6]|dl|dt|dd)\b[^>]*>/gi, "\n")
		.replace(/<\/t[dh]>/gi, " | ")
		.replace(/<[^>]+>/g, "");
	const text = decode(body)
		.split("\n")
		.map((l) => l.replace(/[ \t ]+/g, " ").trim())
		.join("\n")
		.replace(/^-\n+/gm, "- ") // a list item whose content sits in a block element
		.replace(/\n{3,}/g, "\n\n")
		.trim();
	return title ? `# ${decode(title)}\n\n${text}` : text;
}

const oneLine = (s: unknown, max: number) => String(s ?? "").replace(/\s+/g, " ").trim().slice(0, max);

const errText = (e: unknown) => (e instanceof Error ? (e as NodeJS.ErrnoException).code ?? e.message : String(e));

const reply = (text: string) => ({ content: [{ type: "text" as const, text }], details: {} });

export default function (pi: ExtensionAPI) {
	pi.registerTool({
		name: "web_search",
		label: "Web search",
		description: `Search the web (searxng on jupiter). Returns titles, URLs and short snippets; read a page with web_fetch.`,
		parameters: Type.Object({
			query: Type.String({ description: "Search terms, e.g. `llama.cpp server --jinja flag`" }),
			max: Type.Optional(Type.Number({ description: `Results to return (default ${DEFAULT_RESULTS}, max ${MAX_RESULTS})` })),
		}),
		promptGuidelines: [
			"Use the web only for what the project and its installed libraries cannot answer (current versions, external docs, error messages). Search first, then web_fetch the one or two most relevant pages; snippets alone are often not enough.",
		],
		async execute(_id, params) {
			const max = Math.max(1, Math.min(MAX_RESULTS, Math.floor(params.max ?? DEFAULT_RESULTS)));
			let page: Page;
			try {
				page = await get(`${SEARXNG}/search?format=json&q=${encodeURIComponent(params.query)}`);
			} catch (e) {
				return reply(`Web search unavailable: searxng at ${SEARXNG} did not answer (${errText(e)}). Continue without the web.`);
			}
			if (page.status !== 200) return reply(`Web search failed: searxng at ${SEARXNG} answered HTTP ${page.status}.`);
			let data: { results?: { title?: string; url?: string; content?: string }[]; answers?: unknown[]; infoboxes?: { infobox?: string; content?: string }[] };
			try {
				data = JSON.parse(page.body);
			} catch {
				return reply(`Web search failed: searxng at ${SEARXNG} did not return JSON (is format=json enabled?).`);
			}
			const out: string[] = [];
			for (const a of data.answers ?? []) out.push(`Answer: ${oneLine(typeof a === "string" ? a : (a as { answer?: string }).answer, SNIPPET_CHARS)}`);
			for (const b of data.infoboxes ?? []) out.push(`Infobox ${oneLine(b.infobox, 80)}: ${oneLine(b.content, SNIPPET_CHARS)}`);
			(data.results ?? []).slice(0, max).forEach((r, i) => {
				out.push(`${i + 1}. ${oneLine(r.title, 120)} — ${r.url}`);
				const snippet = oneLine(r.content, SNIPPET_CHARS);
				if (snippet) out.push(`   ${snippet}`);
			});
			return reply(out.length ? out.join("\n") : `No results for "${params.query}". Try other terms.`);
		},
	});

	pi.registerTool({
		name: "web_fetch",
		label: "Web fetch",
		description: `Fetch a web page (http/https) as plain text, ${FETCH_CHARS / 1024} KB at a time; call again with offset for the rest.`,
		parameters: Type.Object({
			url: Type.String({ description: "Full http(s) URL, usually one from web_search" }),
			offset: Type.Optional(Type.Number({ description: "Character offset to continue from (given in the truncation note)" })),
		}),
		async execute(_id, params) {
			let u: URL;
			try {
				u = new URL(params.url);
			} catch {
				return reply(`Not a URL: ${params.url}`);
			}
			if (u.protocol !== "http:" && u.protocol !== "https:") return reply(`Only http and https URLs can be fetched, not ${u.protocol}`);
			let page: Page;
			try {
				page = await get(u.toString());
			} catch (e) {
				return reply(`Could not fetch ${u}: ${errText(e)}`);
			}
			if (page.status >= 400) return reply(`Could not fetch ${page.url}: HTTP ${page.status}`);
			const type = page.type.toLowerCase();
			const isHtml = type.includes("html") || (!type && /^\s*<(!doctype|html)/i.test(page.body));
			if (!isHtml && type && !/^text\/|json|xml|javascript|yaml|toml/.test(type)) return reply(`${page.url} is ${page.type}, not text; it cannot be read here.`);
			const text = isHtml ? htmlText(page.body) : page.body;
			const offset = Math.max(0, Math.floor(params.offset ?? 0));
			const part = text.slice(offset, offset + FETCH_CHARS);
			const left = text.length - offset - part.length;
			const head = page.url !== u.toString() ? `(redirected to ${page.url})\n\n` : "";
			const tail = left > 0 ? `\n\n[truncated: ${left} more chars; call web_fetch again with offset=${offset + part.length} if you need them]` : "";
			return reply(part ? head + part + tail : `Nothing to read at offset ${offset} (${text.length} chars in total).`);
		},
	});
}
