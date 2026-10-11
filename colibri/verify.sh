#!/bin/bash
# Verify the colibri model download against colibri/model.lock.yaml (pinned revision, no network needed).
# Checks: file count + every file size + total bytes; sha256 of 3 random shards + all non-shard LFS files;
# git blob sha1 for non-LFS files. --full hashes every shard (185 GB, slow).
# Usage: colibri/verify.sh [--full] [model dir, default ~/llm/colibri/models/qwen38-flash-next]
# Exit codes: 0 ok, 1 error, 3 mismatch/missing
set -euo pipefail
ROOT=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)
MF=$ROOT/colibri/model.lock.yaml
FULL=0; [[ ${1:-} == --full ]] && { FULL=1; shift; }
DIR=${1:-$HOME/llm/colibri/models/qwen38-flash-next}
PYYAML=6.0.3   # exact pin; the only external input of this script
if command -v uv >/dev/null 2>&1; then UV=(uv); else UV=(mise x uv@0.12.24 -- uv); fi
"${UV[@]}" run -q --with "pyyaml==$PYYAML" --python 3.12 python -I - "$MF" "$DIR" "$FULL" <<'PY'
import sys, os, hashlib, random, yaml
m = yaml.safe_load(open(sys.argv[1])); d = sys.argv[2]; full = sys.argv[3] == "1"
files = m["files"]
bad = 0; total = 0
if len(files) != m["file_count"]:
    print("manifest file_count mismatch"); bad += 1
for x in files:
    p = os.path.join(d, x["path"])
    if not os.path.exists(p) or os.path.getsize(p) != x["size"]:
        print("SIZE MISMATCH/MISSING", x["path"]); bad += 1
    else:
        total += x["size"]
print("files", len(files), "bytes", total, "expected", m["total_bytes"])
def sha256(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 24), b""): h.update(b)
    return h.hexdigest()
def gitsha(p):
    data = open(p, "rb").read()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
shards = [x for x in files if x["path"].endswith(".safetensors")]
random.seed(1)
sample = (shards if full else random.sample(shards, 3)) + [x for x in files if x not in shards]
for x in sample:
    p = os.path.join(d, x["path"])
    if not os.path.exists(p):
        continue  # already reported
    if "sha256" in x:
        ok = sha256(p) == x["sha256"]; kind = "sha256"
    else:
        ok = gitsha(p) == x["git_sha1"]; kind = "git-blob-sha1"
    print("OK " if ok else "BAD", kind, x["path"]); bad += 0 if ok else 1
sys.exit(3 if bad else 0)
PY
