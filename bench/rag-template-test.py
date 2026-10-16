#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["reportlab>=4", "matplotlib>=3.8"]
# ///
"""RAG template A/B against a live Open WebUI (0.12): retrieval through Open WebUI, generation on llama-swap.

Builds 3 synthetic PDFs (fictional company "Kestrel Lighting", no real data), uploads them into a temporary knowledge base,
asks 8 questions with known answers. For each question the chunks come from Open WebUI's /api/v1/retrieval/query/collection
(its embedder, hybrid search and reranker settings), then the prompt is built as Open WebUI does it (template with the
<source> context, "\\n", the question, in the user message) once per setup and sent to the same llama-swap model.
  old: Open WebUI's default template, no chunk metadata in <source> (RAG_SOURCE_METADATA_KEYS unset)
  new: bench/rag-templates/homelab.txt, page/page_label shown (RAG_SOURCE_METADATA_KEYS=page,page_label)
The live config is not changed; the knowledge base and files are deleted at the end (--keep to keep them).
API key: read from ~/.config/openwebui/api-key, never printed.
Usage: bench/rag-template-test.py [--url https://jupiter.internal:3000] [--model qwen3.6-35b-a3b] [--runs 1] [--keep] OUT.md"""
import argparse, html, io, json, os, re, sys, tempfile, time, urllib.request, uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
LLM = "http://localhost:11434/v1/chat/completions"

QUESTIONS = [  # (id, question, checks) ; check = (label, regex that must match | None, regex that must NOT match | None)
    ("q1-number", "What was Region North's revenue in Q3 2026?",
     [("exact 12 438 650", r"12[ ,. ]?438[ ,. ]?650", None), ("cites a source", r"\[\d+\]", None)]),
    ("q2-conflict", "How many employees did Kestrel Lighting have at the end of Q3 2026?",
     [("mentions 214", r"\b214\b", None), ("mentions 209", r"\b209\b", None), ("cites a source", r"\[\d+\]", None)]),
    ("q3-absent", "What is Kestrel Lighting's dividend policy?",
     [("says not in documents", r"(?i)not (in|found in|mentioned in|contain|include)|no (information|mention)|does not (say|mention|contain|include|state)|don't (contain|mention|include)|isn't (in|mentioned)", None),
      ("no invented percentage", None, r"\d+(\.\d+)? ?%")]),
    ("q4-swedish", "När ska fläktarna bytas och vad är budgeten för underhållet?",
     [("vecka 47", r"(?i)vecka 47", None), ("385 000", r"385[  .]?000", None), ("answers in Swedish", r"(?i)\b(och|är|ska|för)\b", None)]),
    ("q5-crosslang", "Who is responsible for the maintenance plan?",
     [("keeps 'Driftteamet'", r"Driftteamet", None), ("answers in English", r"(?i)\b(the|is|responsible)\b", None)]),
    ("q6-date", "On what date does the warehouse move to Västerås take place?",
     [("exact 2026-11-03", r"2026-11-03|3 November 2026|November 3, 2026", None)]),
    ("q7-calc", "What was the combined Q3 2026 revenue of Region North and Region South?",
     [("correct sum 21 667 450", r"21[ ,. ]?667[ ,. ]?450", None), ("shows inputs", r"12[ ,. ]?438[ ,. ]?650", None)]),
    ("q8-page", "On which page of the Q3 report is the warehouse move described?",
     [("page 3", r"(?i)(page|p\.|sida) ?3\b", None), ("no wrong page", None, r"(?i)(page|p\.) ?[124]\b")]),
]
FACTS = {"north": 12438650, "south": 9228800, "east": 6104300, "west": 812400}


def make_pdfs(out: Path) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    ss = getSampleStyleSheet()
    sek = lambda v: f"{v:,}".replace(",", " ")
    grid = TableStyle([("GRID", (0, 0), (-1, -1), 0.5, colors.black), ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey)])

    fig, ax = plt.subplots(figsize=(6, 3.2))
    ax.bar([k.title() for k in FACTS], [v / 1e6 for v in FACTS.values()], color="slateblue")
    ax.set_title("Q3 2026 revenue by region (MSEK)"); ax.set_ylabel("MSEK")
    chart = io.BytesIO(); fig.savefig(chart, format="png", dpi=200, bbox_inches="tight"); plt.close(fig); chart.seek(0)

    report = out / "kestrel-q3-2026-report.pdf"
    SimpleDocTemplate(str(report), pagesize=A4).build([
        Paragraph("Kestrel Lighting: Q3 2026 quarterly report", ss["Title"]),
        Paragraph("Kestrel Lighting designs and sells LED lighting for warehouses. This report covers July to September 2026.", ss["BodyText"]),
        Paragraph("Revenue by region", ss["Heading2"]),
        Table([["Region", "Q3 2026 revenue (SEK)"]] + [[f"Region {k.title()}", sek(v)] for k, v in FACTS.items()], style=grid),
        Spacer(1, 12),
        Paragraph("Region West revenue was low because the Gothenburg showroom was closed for renovation.", ss["BodyText"]),
        PageBreak(),
        Paragraph("Regional overview", ss["Heading2"]), Image(chart, 14 * cm, 7.5 * cm),
        Paragraph("Staff", ss["Heading2"]),
        Paragraph("At the end of Q3 2026 Kestrel Lighting had 214 employees, up from 198 at the end of Q2.", ss["BodyText"]),
        PageBreak(),
        Paragraph("Logistics", ss["Heading2"]),
        Paragraph("The central warehouse moves from Eskilstuna to Västerås on 2026-11-03. Deliveries pause for two working days during the move.", ss["BodyText"]),
    ])
    memo = out / "kestrel-q3-2026-correction-memo.pdf"
    SimpleDocTemplate(str(memo), pagesize=A4).build([
        Paragraph("Correction memo: Q3 2026 report", ss["Title"]),
        Paragraph("HR has corrected the headcount in the Q3 2026 report. The number of employees at the end of Q3 2026 was 209, "
                  "not 214: five fixed-term contracts had already ended in September.", ss["BodyText"]),
    ])
    plan = out / "kestrel-underhallsplan-2026.pdf"
    SimpleDocTemplate(str(plan), pagesize=A4).build([
        Paragraph("Underhållsplan för lagret 2026", ss["Title"]),
        Paragraph("Ansvarig för underhållsplanen är Driftteamet. Planen gäller lagret i Västerås från flytten i november.", ss["BodyText"]),
        Table([["Åtgärd", "Tidpunkt"], ["Byte av takfläktar", "vecka 47"], ["Kontroll av nödbelysning", "vecka 49"],
               ["Service av truckladdare", "vecka 2, 2027"]], style=grid),
        Spacer(1, 12),
        Paragraph("Den totala budgeten för underhållet är 385 000 kr.", ss["BodyText"]),
    ])
    return [report, memo, plan]


class OpenWebUI:
    def __init__(self, url: str, key: str):
        self.url, self.key = url.rstrip("/"), key

    def req(self, method, path, body=None, files=None, timeout=600):
        headers = {"Authorization": f"Bearer {self.key}", "Accept": "application/json"}
        data = None
        if files:
            boundary = uuid.uuid4().hex
            name, content = files
            data = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{name}\"\r\n"
                    f"Content-Type: application/pdf\r\n\r\n").encode() + content + f"\r\n--{boundary}--\r\n".encode()
            headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        elif body is not None:
            data = json.dumps(body).encode(); headers["Content-Type"] = "application/json"
        r = urllib.request.Request(self.url + path, data, headers, method=method)
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw) if raw else None


def wait_processed(owui, file_id, limit=900):
    end = time.monotonic() + limit
    while time.monotonic() < end:
        st = owui.req("GET", f"/api/v1/files/{file_id}/process/status")
        if st.get("status") in ("completed", "failed"):
            return st
        time.sleep(3)
    raise TimeoutError(f"file {file_id} not processed in {limit} s")


def build_context(result, names, with_meta: bool) -> str:
    """Mirror of open_webui.utils.middleware.get_source_context for knowledge-base chunks."""
    out, ids = "", {}
    for doc, meta in zip(result["documents"][0], result["metadatas"][0]):
        sid = meta.get("source") or meta.get("file_id") or "N/A"
        ids.setdefault(sid, len(ids) + 1)
        name = meta.get("name") or names.get(meta.get("file_id"), "")
        extra = "".join(f' {k}="{html.escape(str(meta[k]))}"' for k in ("page", "page_label") if with_meta and meta.get(k) is not None)
        out += f'<source id="{ids[sid]}"' + (f' name="{name}"' if name else "") + extra + f">{doc}</source>\n"
    return out


def fill(template: str, context: str) -> str:
    return template.replace("[context]", context).replace("{{CONTEXT}}", context)


def ask(model, prompt):
    body = {"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4096}
    r = urllib.request.Request(LLM, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=1800) as resp:
        return json.load(resp)["choices"][0]["message"]["content"] or ""


def score(answer, checks):
    res = []
    for label, must, must_not in checks:
        ok = (must is None or re.search(must, answer) is not None) and (must_not is None or re.search(must_not, answer) is None)
        res.append((label, ok))
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out"); ap.add_argument("--url", default="https://jupiter.internal:3000")
    ap.add_argument("--model", default="qwen3.6-35b-a3b"); ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    key = Path(os.path.expanduser("~/.config/openwebui/api-key")).read_text().strip()
    owui = OpenWebUI(a.url, key)
    setups = {"old": ((HERE / "rag-templates/openwebui-default-0.12.0.txt").read_text(), False),
              "new": ((HERE / "rag-templates/homelab.txt").read_text(), True)}
    kb = None; file_ids = []
    try:
        with tempfile.TemporaryDirectory() as tmp:
            pdfs = make_pdfs(Path(tmp))
            names = {}
            for pdf in pdfs:
                f = owui.req("POST", "/api/v1/files/", files=(pdf.name, pdf.read_bytes()))
                file_ids.append(f["id"]); names[f["id"]] = pdf.name
                st = wait_processed(owui, f["id"])
                print(f"uploaded {pdf.name}: {st.get('status')}", file=sys.stderr, flush=True)
                if st.get("status") != "completed":
                    raise RuntimeError(f"processing failed for {pdf.name}: {st}")
        kb = owui.req("POST", "/api/v1/knowledge/create", {"name": "rag-template-test (synthetic, safe to delete)", "description": "bench/rag-template-test.py"})
        for fid in file_ids:
            owui.req("POST", f"/api/v1/knowledge/{kb['id']}/file/add", {"file_id": fid})
        print(f"knowledge base ready", file=sys.stderr, flush=True)

        rows, answers = [], []
        for qid, question, checks in QUESTIONS:
            result = owui.req("POST", "/api/v1/retrieval/query/collection", {"collection_names": [kb["id"]], "query": question})
            for setup, (template, with_meta) in setups.items():
                for run in range(a.runs):
                    t0 = time.monotonic()
                    answer = ask(a.model, fill(template, build_context(result, names, with_meta)) + "\n" + question)
                    res = score(answer, checks)
                    rows.append((qid, setup, run, res, time.monotonic() - t0))
                    answers.append((qid, setup, run, question, answer))
                    print(f"[{qid}] {setup} run {run}: " + ", ".join(f"{l} {'✓' if ok else '✗'}" for l, ok in res), file=sys.stderr, flush=True)
            chunks = len(result["documents"][0])
            pages = sorted({str(m.get("page")) for m in result["metadatas"][0]})
            rows.append((qid, "retrieval", 0, [(f"{chunks} chunks, page values {', '.join(pages)}", True)], 0))
    finally:
        if not a.keep:
            if kb:
                owui.req("DELETE", f"/api/v1/knowledge/{kb['id']}/delete")
            for fid in file_ids:
                owui.req("DELETE", f"/api/v1/files/{fid}")
            print("cleanup: knowledge base and files deleted", file=sys.stderr, flush=True)

    with open(a.out, "w") as f:
        f.write(f"model {a.model}, {a.runs} run(s) per question and setup, Open WebUI {a.url}\n\n")
        f.write("| Question | Setup | Checks passed | Details | Time (s) |\n|---|---|---|---|---|\n")
        for qid, setup, run, res, dt in rows:
            if setup == "retrieval":
                f.write(f"| {qid} | retrieval | | {res[0][0]} | |\n"); continue
            f.write(f"| {qid} | {setup} | {sum(ok for _, ok in res)}/{len(res)} | " + "; ".join(f"{l} {'✓' if ok else '✗'}" for l, ok in res) + f" | {dt:.0f} |\n")
        total = {s: (sum(sum(ok for _, ok in r) for q, st, _, r, _ in rows if st == s), sum(len(r) for q, st, _, r, _ in rows if st == s)) for s in setups}
        f.write("\n" + " · ".join(f"**{s}: {p}/{n} checks**" for s, (p, n) in total.items()) + "\n\n## Answers\n")
        for qid, setup, run, question, answer in answers:
            f.write(f"\n### {qid} · {setup} · run {run}\n\n**Q:** {question}\n\n{answer.strip()}\n")
    print(f"wrote {a.out}", file=sys.stderr)


main()
