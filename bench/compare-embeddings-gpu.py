"""Cosine similarity between the CPU and GPU llama-swap entries for snowflake-arctic-embed2, plus throughput.

The GPU entry (snowflake-arctic-embed2-gpu) is only usable for indexing if its vectors match the CPU entry that
built the existing Open WebUI index. Texts: the 5 fixed texts from embed-texts.json, their "query: " variants
(Open WebUI's RAG_EMBEDDING_QUERY_PREFIX), and synthetic ~1800-character chunks (Open WebUI's CHUNK_SIZE) in
English and Swedish.

Run: uv run --with numpy==2.5.4 python -I bench/compare-embeddings-gpu.py [base_url]
Pass threshold: >= 0.999 for every text.
"""
import json, random, sys, time, urllib.request
from pathlib import Path
import numpy as np

base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:11434"
here = Path(__file__).resolve().parent
CPU, GPU = "snowflake-arctic-embed2", "snowflake-arctic-embed2-gpu"

fixed = json.loads((here / "embed-texts.json").read_text())
rng = random.Random(7)
en = ("the ledger records each balance change while the team reviews the interest schedule and the manager "
      "negotiates a new contract with the vendor before the quarterly report is published").split()
sv = ("föreningen beslutade att årsredovisningen ska granskas innan stämman och styrelsen föreslår att avgiften "
      "höjs för att täcka räntekostnaderna under nästa räkenskapsår").split()


def chunk(words, n=1800):
    out = []
    while len(" ".join(out)) < n:
        out.append(rng.choice(words))
    return " ".join(out)[:n]


texts = fixed + ["query: " + t for t in fixed] + [chunk(en) for _ in range(6)] + [chunk(sv) for _ in range(6)]


def embed(model, inputs):
    req = urllib.request.Request(f"{base}/v1/embeddings", data=json.dumps({"model": model, "input": inputs}).encode(),
                                 headers={"Content-Type": "application/json"})
    data = json.load(urllib.request.urlopen(req, timeout=1200))["data"]
    return np.array([d["embedding"] for d in sorted(data, key=lambda d: d["index"])])


embed(GPU, ["warm-up"])  # first call loads the GPU entry
cpu, gpu = embed(CPU, texts), embed(GPU, texts)
cos = (cpu * gpu).sum(1) / (np.linalg.norm(cpu, axis=1) * np.linalg.norm(gpu, axis=1))
passed = bool((cos >= 0.999).all())
groups = {"fixed": cos[:5], "query-prefixed": cos[5:10], "chunk-en": cos[10:16], "chunk-sv": cos[16:]}
for name, c in groups.items():
    print(f"{name:15s} n={len(c):2d} min_cos={c.min():.6f} mean_cos={c.mean():.6f}")
print(f"dim={gpu.shape[1]} overall min_cos={cos.min():.6f} {'PASS' if passed else 'FAIL'}")

batch = [chunk(en if i % 2 else sv) for i in range(64)]
for model in (CPU, GPU):
    t = time.time()
    embed(model, batch)
    dt = time.time() - t
    print(f"{model:28s} 64 chunks of 1800 chars: {dt:6.2f} s ({1000 * dt / 64:.0f} ms/chunk)")
sys.exit(0 if passed else 1)
