"""Phase 3: cosine similarity between Ollama baseline vectors and llama-swap /v1/embeddings.

Run: uv run --with numpy==2.5.4 python -I bench/compare-embeddings.py [base_url]
Pass threshold: >= 0.999 for every text.
"""
import json, sys, urllib.request
from pathlib import Path
import numpy as np

base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:11435"
here = Path(__file__).resolve().parent
texts = json.loads((here / "embed-texts.json").read_text())
ok = True
for model, name in [("bge-m3", "bge-m3"), ("snowflake-arctic-embed2", "snowflake")]:
    ref = np.array(json.loads((here / f"embed-baseline-{name}.json").read_text())["embeddings"])
    req = urllib.request.Request(f"{base}/v1/embeddings", data=json.dumps({"model": model, "input": texts}).encode(),
                                 headers={"Content-Type": "application/json"})
    data = json.load(urllib.request.urlopen(req, timeout=300))["data"]
    new = np.array([d["embedding"] for d in sorted(data, key=lambda d: d["index"])])
    cos = (ref * new).sum(1) / (np.linalg.norm(ref, axis=1) * np.linalg.norm(new, axis=1))
    passed = bool((cos >= 0.999).all())
    ok &= passed
    print(f"{model:26s} dim={new.shape[1]} min_cos={cos.min():.6f} {'PASS' if passed else 'FAIL'}")
sys.exit(0 if ok else 1)
