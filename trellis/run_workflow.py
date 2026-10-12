#!/usr/bin/env python3
"""Run trellis/workflows/*_api.json against the comfyui-trellis service, stage by stage, and print timings.

Stdlib only (runs with the host python3, nothing to install). Each stage is submitted as a cumulative prefix of
the graph: ComfyUI caches the upstream nodes, so a prompt's execution time is the time of the newly added nodes.
  trellis/run_workflow.py [--url http://localhost:8189] [--image NAME] [--seed N] [--prefix trellis2/NAME] [workflow.json]
"""
import argparse, copy, json, pathlib, time, urllib.request

# Only output nodes (and their inputs) execute, so the model load, rembg and generation share one stage.
# Sub-stage times (sparse structure, shape, texture, remesh, unwrap, bake): `docker logs -t comfyui-trellis`.
STAGES = [("load + rembg + generate", ["1", "2", "3", "4"]), ("remesh + UV + bake", ["5"]), ("export glb", ["6"])]

def call(url, path, body=None):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)

def run(url, graph, timeout=3600):
    pid = call(url, "/prompt", {"prompt": graph})["prompt_id"]
    t0 = time.time()
    while time.time() - t0 < timeout:
        h = call(url, f"/history/{pid}").get(pid)
        if h and h.get("status", {}).get("completed") is not None:
            msgs = {m[0]: m[1] for m in h["status"].get("messages", [])}
            if h["status"].get("status_str") != "success":
                raise SystemExit(f"prompt failed: {json.dumps(msgs.get('execution_error', h['status']))[:2000]}")
            ms = msgs["execution_success"]["timestamp"] - msgs["execution_start"]["timestamp"]
            return ms / 1000, h.get("outputs", {})
        time.sleep(1)
    raise SystemExit("timeout")

ap = argparse.ArgumentParser()
ap.add_argument("workflow", nargs="?", default=str(pathlib.Path(__file__).parent / "workflows/trellis2_512_api.json"))
ap.add_argument("--url", default="http://localhost:8189")
ap.add_argument("--image"); ap.add_argument("--seed", type=int)
ap.add_argument("--prefix", help="ExportMesh filename_prefix (default from the workflow: trellis2/prop-terminal)")
a = ap.parse_args()
wf = json.load(open(a.workflow))
if a.image: wf["2"]["inputs"]["image"] = a.image
if a.seed is not None: wf["4"]["inputs"]["seed"] = a.seed
if a.prefix: wf["6"]["inputs"]["filename_prefix"] = a.prefix
keep, total = [], 0.0
for name, ids in STAGES:
    keep += ids
    secs, out = run(a.url, {k: copy.deepcopy(v) for k, v in wf.items() if k in keep})
    total += secs
    print(f"{name:28s} {secs:8.1f} s", flush=True)
print(f"{'total':28s} {total:8.1f} s")
print(json.dumps(out, indent=1)[:1500])
