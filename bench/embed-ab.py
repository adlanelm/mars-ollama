#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["numpy>=1.26", "pyarrow>=15"]
# ///
"""Embedder A/B on public MTEB retrieval sets via llama-swap /v1/embeddings (+ /v1/rerank on the top 20).
Each variant is something Open WebUI can express: one global query prefix (RAG_EMBEDDING_QUERY_PREFIX), no content prefix.
Datasets (download into DATA first, see bench/embed-ab-2026-10-16.md): SweFaqRetrieval, SwednRetrieval (Swedish), scifact (English).
Usage: bench/embed-ab.py DATA [QUERIES_PER_SET=300]   -> markdown tables on stdout"""
import json, random, sys, time, urllib.request
import numpy as np
import pyarrow.parquet as pq

URL = "http://localhost:11434/v1"
DATA, NQ = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 300
DOC_CHARS = 2000  # about an Open WebUI chunk
QWEN_TASK = "Given a web search query, retrieve relevant passages that answer the query"
VARIANTS = [  # (label, model, query prefix)
    ("snowflake, no prefix (Open WebUI default)", "snowflake-arctic-embed2", ""),
    ("snowflake, `query: `", "snowflake-arctic-embed2", "query: "),
    ("qwen3-0.6b, no instruction", "qwen3-embedding-0.6b", ""),
    ("qwen3-0.6b, instruction", "qwen3-embedding-0.6b", f"Instruct: {QWEN_TASK}\nQuery:"),
]


def post(path, body):
    req = urllib.request.Request(URL + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.load(r)


def embed(model, texts, bs=16):
    out, tokens, t0 = [], 0, time.monotonic()
    for i in range(0, len(texts), bs):
        r = post("/embeddings", {"model": model, "input": texts[i:i + bs]})
        out += [d["embedding"] for d in sorted(r["data"], key=lambda d: d["index"])]
        tokens += r.get("usage", {}).get("prompt_tokens", 0)
    v = np.asarray(out, dtype=np.float32)
    return v / np.linalg.norm(v, axis=1, keepdims=True), tokens / (time.monotonic() - t0)


def load_parquet(name):
    t = lambda part: pq.read_table(f"{DATA}/{name}/{part}/test-00000-of-00001.parquet").to_pylist()
    corpus = {r["_id"]: (r.get("title") or "") + "\n" + r["text"] for r in t("corpus")}
    queries = {r["_id"]: r["text"] for r in t("queries")}
    qrels = {}
    for r in t("qrels"):
        if float(r["score"]) > 0:
            qrels.setdefault(r["query-id"], set()).add(r["corpus-id"])
    return corpus, queries, qrels


def load_scifact():
    corpus = {}
    for l in open(f"{DATA}/scifact/corpus.jsonl"):
        d = json.loads(l); corpus[d["_id"]] = d["title"] + "\n" + d["text"]
    queries = {(d := json.loads(l))["_id"]: d["text"] for l in open(f"{DATA}/scifact/queries.jsonl")}
    qrels = {}
    for l in open(f"{DATA}/scifact/qrels/test.tsv").read().splitlines()[1:]:
        q, c, s = l.split("\t")
        if float(s) > 0:
            qrels.setdefault(q, set()).add(c)
    return corpus, queries, qrels


def ndcg10(ranked, rel):
    dcg = sum(1 / np.log2(i + 2) for i, d in enumerate(ranked[:10]) if d in rel)
    return dcg / sum(1 / np.log2(i + 2) for i in range(min(len(rel), 10)))


def main():
    sets = {"SweFaq (sv)": load_parquet("SweFaqRetrieval"), "Swedn (sv)": load_parquet("SwednRetrieval"), "SciFact (en)": load_scifact()}
    rows, speed = [], {}
    for sname, (corpus, queries, qrels) in sets.items():
        ids = list(corpus); docs = [corpus[i].strip()[:DOC_CHARS] for i in ids]
        qids = sorted(q for q in qrels if q in queries)
        random.Random(0).shuffle(qids); qids = qids[:NQ]
        doc_vecs = {}
        for label, model, prefix in VARIANTS:
            if model not in doc_vecs:
                doc_vecs[model], tps = embed(model, docs)
                speed.setdefault(model, []).append(tps)
            qv, _ = embed(model, [prefix + queries[q] for q in qids])
            sims = qv @ doc_vecs[model].T
            top = np.argsort(-sims, axis=1)[:, :20]
            nd, rec, ndr = [], [], []
            for qi, q in enumerate(qids):
                ranked = [ids[j] for j in top[qi]]
                nd.append(ndcg10(ranked, qrels[q])); rec.append(len(set(ranked) & qrels[q]) / len(qrels[q]))
                rr = post("/rerank", {"model": "bge-reranker-v2-m3", "query": queries[q],
                                      "documents": [docs[j] for j in top[qi]], "top_n": 20})
                order = [ranked[x["index"]] for x in sorted(rr["results"], key=lambda x: -x["relevance_score"])]
                ndr.append(ndcg10(order, qrels[q]))
            rows.append((sname, label, np.mean(nd), np.mean(rec), np.mean(ndr)))
            print(f"[{sname}] {label}: nDCG@10 {np.mean(nd):.3f}, R@20 {np.mean(rec):.3f}, +rerank {np.mean(ndr):.3f}", file=sys.stderr, flush=True)
    print(f"{NQ} queries per set (fewer if the set is smaller), docs cut to {DOC_CHARS} chars, rerank = bge-reranker-v2-m3 over the top 20\n")
    print("| Set | Variant | nDCG@10 | Recall@20 | nDCG@10 after rerank |\n|---|---|---|---|---|")
    for r in rows:
        print(f"| {r[0]} | {r[1]} | {r[2]:.3f} | {r[3]:.3f} | {r[4]:.3f} |")
    print("\n| Model | Document embedding, tokens/s (CPU, per set) |\n|---|---|")
    for m, v in speed.items():
        print(f"| {m} | {', '.join(f'{x:.0f}' for x in v)} |")


main()
