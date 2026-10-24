# Model downloads: replacing `ollama pull`

Status: **implemented** (Phase 1, done 2026-10-11). Spec and notes for `bin/llm-pull`; the script and `llama-swap/models.lock.yaml` are the source of truth. Items marked **[verify]** are still unconfirmed.

Related docs: [models.md](models.md) (what to download and why), [llama-swap-config.md](llama-swap-config.md) (how entries use the files).

## Purpose

Neither llama-swap nor llama-server has a pull or registry workflow like Ollama's. There are two tiers instead.

| Tier | Use | Mechanism |
|---|---|---|
| **1. Permanent set** | Everything that stays | `models.lock.yaml` manifest + `bin/llm-pull` (bash + `uvx hf` + `jq`) |
| **2. Quick experiments** | Closest to `ollama run` | `llama-server -hf <repo>:<QUANT>` auto-download into a cache volume |

Why Tier 1 beats download-on-load:
- Updates are explicit and pinned. That's reproducible and can be rolled back, unlike Ollama's moving tags.
- A first chat request never triggers a 20 GB download that blocks Open WebUI past `healthCheckTimeout`.
- It works offline.
- Disk use is visible before you commit to a model.

## Prerequisites

```bash
sudo pacman -S uv
uvx --from "huggingface_hub[hf_xet]==2.2.0" hf version
```

- The `hf` CLI is pinned to `huggingface_hub[hf_xet]==2.2.0`. The pin lives in `HF_HUB` at the top of `bin/llm-pull`; change it there, not in these examples.
- `llm-pull` uses the system `uv`/`uvx` when both exist, and falls back to `mise x uv@0.12.24 -- uv`.
- uv provides its own Python 3.12, so the host's Python 3.14 doesn't matter.
- Xet makes resumable, deduplicated downloads. Resolved: `hf download --revision` works with Xet, and the HF tree API returns `.lfs.oid` for the files used here.
- `jq` and `curl` are needed for the HF API calls.

## Directory layout

```
~/llm/models/<org>/<repo>/        # Tier 1, mounted read-only at /models in the container
  Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf
  mmproj-F16.gguf
~/llm/llama-cache/                # Tier 2, mounted read-write at /cache
~/devel/mars-ollama/llama-swap/models.lock.yaml   # committed to git
```

Example: `~/llm/models/unsloth/Qwen3.6-35B-A3B-GGUF/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf`.

The model mount stays **read-only** in the container.

## Manifest: `llama-swap/models.lock.yaml`

Single source of truth, committed next to `config.yaml`. Each entry lists `id`, `repo`, `files` (quant + mmproj), a **pinned `revision` (commit SHA)**, and `aliases`. The per-file `size` and `sha256` fields are filled in by `llm-pull add` from the HF API. The file is written by `llm-pull`; don't edit it by hand.

### Schema

| Field | Type | Meaning |
|---|---|---|
| `id` | string | llama-swap model ID |
| `source` | `hf` (default) or `ollama` | Where the files come from |
| `repo` | string | HF repo (`hf` only) |
| `revision` | string | Pinned commit SHA (`hf` only). Placeholder `<pinned-sha>` below |
| `files[].name` | string | File name inside `~/llm/models/<org>/<repo>/` |
| `files[].role` | `model` or `mmproj` | Used when printing the llama-swap snippet |
| `files[].size` | int | Bytes, from the HF tree API |
| `files[].sha256` | string | LFS oid from the HF tree API. For `ollama`: the sha of the **file on disk**, which differs from `ollama.blob_sha256` when a fixup was applied |
| `ollama.ref` | string | `<model>:<tag>` (`ollama` only) |
| `ollama.blob` | string | Blob digest prefix (`ollama` only) |
| `ollama.blob_sha256` | string | Full digest of the original Ollama blob (`ollama` only) |
| `ollama.fixup` | string, optional | Fixup applied to the copy. Currently `charsmap-b64-to-uint8` (snowflake) |
| `aliases` | list | Old Ollama names that keep working |

### Entries

One entry per model in the [models.md](models.md) picks table; see [`llama-swap/models.lock.yaml`](../llama-swap/models.lock.yaml) for the real ones. Two shapes (revision and sha values below are placeholders):

```yaml
- id: qwen3.8-27b
  source: hf
  repo: unsloth/Qwen3.8-27B-GGUF
  revision: <pinned-sha>
  files:
  - {name: Qwen3.8-27B-UD-Q3_K_XL.gguf, role: model,  size: <bytes>, sha256: <lfs-oid>}
  - {name: mmproj-F16.gguf,             role: mmproj, size: <bytes>, sha256: <lfs-oid>}
  aliases: [qwen3.8, qwen3.8:128k, qwen3.8:64k, qwen3.8:latest]

- id: snowflake-arctic-embed2
  source: ollama
  ollama:
    ref: snowflake-arctic-embed2:latest
    blob: sha256-8c625c95
    blob_sha256: <full-blob-digest>         # the original Ollama blob
    fixup: charsmap-b64-to-uint8            # applied automatically by import-ollama
  files:
  - {name: snowflake-arctic-embed2-F16.gguf, role: model, size: <bytes>, sha256: <sha-of-fixed-file>}
  aliases: [snowflake-arctic-embed2:latest, snowflake-arctic-embed2:latest_8192]
```

Entries in the manifest: qwen3.8-27b, qwen3.8-27b-gsq, flint-40b-a2.5b, qwen3.6-35b-a3b, gemma4-26b, granite-vision, granite-docling, bge-reranker-v2-m3 (all `hf`), bge-m3, snowflake-arctic-embed2 (`ollama`). `bge-m3` has no fixup, so its `files[].sha256` equals `blob_sha256`.

Ollama-imported files live under `~/llm/models/ollama/<model>/<model>-F16.gguf`.

## Commands

```
llm-pull add <repo> <QUANT> [--mmproj <file>] [--id <id>] [--alias <name>]...   # resolve, pin, download, print snippet
llm-pull import-ollama <model:tag> [--id <id>] [--alias <name>]...               # copy the GGUF blob from ~/llm/ollama/data (that data was deleted 2026-10-13; only useful with a fresh Ollama store)
llm-pull sync                                                # download everything in the manifest
llm-pull outdated                                            # compare pinned SHA with repo HEAD
llm-pull update <id>                                         # re-pin to HEAD and download
llm-pull rm <id>                                             # remove an entry and its files
llm-pull gc [-y]                                             # delete unreferenced files (-y: no confirmation)
llm-pull du                                                  # disk use per entry
```

`--alias` may be repeated; aliases are merged with any already in the entry. `LLM_MODELS` (default `~/llm/models`) and `LLM_OLLAMA` (default `~/llm/ollama/data/models`) override the paths. All outputs below are synthetic examples.

### `add`

**Behaviour:** resolve the quant file and the mmproj, pin the current HEAD SHA, download, verify sha256, write the manifest entry, and print a ready llama-swap model snippet.

**Underlying commands:**
```bash
# 1. HEAD sha
curl -s https://huggingface.co/api/models/<repo> | jq -r .sha

# 2. file list with sizes and sha256 (LFS oid) at that revision
curl -s https://huggingface.co/api/models/<repo>/tree/<rev> \
  | jq '.[] | select(.path|test("<QUANT>|mmproj")) | {path, size, oid: .lfs.oid}'

# 3. download
uvx --from "huggingface_hub[hf_xet]==2.2.0" hf download <repo> <file> [<mmproj-file>] \
  --revision <rev> --local-dir ~/llm/models/<org>/<repo>
```

**Example output:**
```
$ llm-pull add unsloth/Qwen3.6-35B-A3B-GGUF UD-Q4_K_XL --mmproj mmproj-F16.gguf --id qwen3.6-35b-a3b
repo      unsloth/Qwen3.6-35B-A3B-GGUF @ <pinned-sha>
model     Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf   22.36 GB
mmproj    mmproj-F16.gguf                    0.90 GB
downloading ... done
sha256    ok (2 files)
manifest  llama-swap/models.lock.yaml updated
snippet   -m /models/unsloth/Qwen3.6-35B-A3B-GGUF/Qwen3.6-35B-A3B-UD-Q4_K_XL.gguf
          --mmproj /models/unsloth/Qwen3.6-35B-A3B-GGUF/mmproj-F16.gguf
```

### `import-ollama`

**Behaviour:** copy the GGUF blob of an installed Ollama model into `~/llm/models`, so the weights stay identical (needed so Qdrant vectors stay valid). **Always copy, never move**; Ollama keeps working and stays the rollback.

**How a blob is resolved:**
1. Read the Ollama manifest JSON `<namespace>/<model>/<tag>` (for library models the namespace is `library`). Two layouts exist: `manifests-v2/ollama.com/...` for newer pulls and `manifests/registry.ollama.ai/...` for older ones. `llm-pull` tries both, first on the host (`~/llm/ollama/data/models`), then through `docker exec ollama`.
2. Find the layer whose `mediaType` is `application/vnd.ollama.image.model`.
3. Take its `digest` (`sha256:<hex>`) and copy `blobs/sha256-<hex>` (from the host, or via `docker exec ollama cat` if the host blob is not readable).
4. Verify that the copy's sha256 equals `<hex>`.
5. **Charsmap fixup (automatic).** The snowflake blob stores `tokenizer.ggml.precompiled_charsmap` as a base64 char array, which llama.cpp rejects. `import-ollama` runs [`tools/fix-gguf-charsmap.py`](../tools/fix-gguf-charsmap.py) on a copy (metadata only, tensors identical) and records `ollama.fixup: charsmap-b64-to-uint8`. The manifest `files[].sha256` is then the sha of the **fixed** file, `ollama.blob_sha256` that of the original blob. When the key is absent or already numeric (bge-m3) the script exits 10 and the blob is used as is.

**Example output:**
```
$ llm-pull import-ollama bge-m3:latest --id bge-m3
manifest  manifests-v2/ollama.com/library/bge-m3/latest
layer     application/vnd.ollama.image.model  sha256-daec91ff…  1.16 GB
copy      -> ~/llm/models/ollama/bge-m3/bge-m3-F16.gguf
sha256    ok
```

Resolved: bge-m3 loads as is, snowflake loads after the fixup. Embedding parity vs Ollama on 5 texts: bge-m3 min cosine 0.999969, snowflake 0.999952 (PASS, no Qdrant re-index).

### `sync`

**Behaviour:** download everything in the manifest that is missing or fails its sha256 check. Used for a rebuild or a new machine. A fully present set is a no-op. `ollama` entries are only checked, since they can't be re-downloaded.

**Underlying commands:** the `hf download` call from `add`, with the pinned `--revision`, per entry.

**Example output:**
```
$ llm-pull sync
qwen3.8-27b            ok
qwen3.6-35b-a3b        ok
gemma4-26b             ok
granite-vision         ok
granite-docling        ok
bge-reranker-v2-m3     ok
bge-m3                 ok (ollama copy)
snowflake-arctic-embed2 ok (ollama copy)
nothing to download
```

### `outdated`

**Behaviour:** compare each pinned revision with the repo HEAD. Useful because unsloth often re-uploads fixed GGUFs. Read-only.

**Underlying command:**
```bash
curl -s https://huggingface.co/api/models/<repo> | jq -r .sha
```

**Example output:**
```
$ llm-pull outdated
qwen3.8-27b        <pinned-sha>  HEAD <newer-sha>   OUTDATED
qwen3.6-35b-a3b    <pinned-sha>  HEAD <pinned-sha>  current
...
```

### `update <id>`

**Behaviour:** re-pin to HEAD, download the new files, verify, update the manifest. **Old files are kept until `llm-pull gc`**, so a rollback is `git checkout models.lock.yaml` plus a config revert.

**Underlying commands:** HEAD sha and tree API as in `add`, then `hf download --revision <new-sha> --local-dir ...`.

**Example output:**
```
$ llm-pull update qwen3.8-27b
<pinned-sha> -> <newer-sha>
Qwen3.8-27B-UD-Q3_K_XL.gguf  changed  13.15 GB
mmproj-F16.gguf               unchanged
downloading ... done, sha256 ok
old files kept until `llm-pull gc`
```

### `rm <id>`

**Behaviour:** remove the manifest entry and its files, unless another entry references the same file.

```
$ llm-pull rm granite-vision
removed 2 files (3.58 GB), manifest updated
```

### `gc`

**Behaviour:** delete files under `~/llm/models` that no manifest entry references (for example old revisions kept by `update`). Lists them first and asks for confirmation; `gc -y` skips the prompt.

> **Warning:** `gc` deletes **anything** under `~/llm/models` that the manifest doesn't reference, including in-progress downloads or files you put there by hand outside `llm-pull` (hidden paths such as `.cache/` are skipped). Don't run it while another download is running, and don't use `-y` unless you have read the list.

```
$ llm-pull gc
unreferenced: unsloth/Qwen3.8-27B-GGUF/Qwen3.8-27B-UD-Q3_K_XL.gguf (13.15 GB, old revision)
delete? [y/N]
```

### `du`

**Behaviour:** show disk use per entry and the total. Run it before committing to a new model.

```
$ llm-pull du
qwen3.8-27b             14.1 GB
qwen3.6-35b-a3b         23.3 GB
gemma4-26b              15.4 GB
granite-vision + docling 4.1 GB
bge-reranker-v2-m3       0.6 GB
embedders                2.4 GB
total                   ~60 GB   (/home: 324 GB free)
```

## Verification and exit codes

Every download is verified against the sha256 (LFS oid) from the HF API tree listing. `import-ollama` verifies against the blob digest, and the fixed file against its own sha. Verified files are cached in `~/llm/models/.llm-pull-verified` (path, size, mtime, sha). A file that fails is deleted and reported.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Success, or nothing to do |
| 1 | Generic error (bad arguments, missing manifest entry) |
| 2 | Network or HF API error |
| 3 | sha256 mismatch |
| 4 | `outdated` found updates (for use in scripts) |
| 5 | Gated repo, authentication needed |

## Gated repos and tokens

No token is needed for the public repos in the manifest. For gated repos:

```bash
uvx --from "huggingface_hub[hf_xet]==2.2.0" hf auth login
```

The token is stored in `~/.cache/huggingface`. **Never put tokens in files in the repo** (manifest, scripts, compose, `.env`).

## Tier 2: `-hf` experiments

Add a llama-swap entry whose `cmd` is `llama-server --port ${PORT} -hf unsloth/<Model>-GGUF:<QUANT> ...`.

- llama-server downloads the GGUF **and its mmproj automatically** on first load.
- It writes to a rw cache volume: `~/llm/llama-cache:/cache` with env `LLAMA_CACHE=/cache` (env name confirmed).
- Set that entry's `healthCheckTimeout` high, or send a warm-up request once.
- After the first load, add `--offline` so it never re-downloads silently.
- `llama-server --cache-list` shows what is in the cache.
- **Promotion to Tier 1:** if you keep the model, run `llm-pull add` and switch the entry to `-m /models/...`. If you don't, delete it from the cache.

## Comparison with `ollama pull`

| | `ollama pull` | `llm-pull` (Tier 1) | `-hf` (Tier 2) |
|---|---|---|---|
| Version pinning | Moving tags | Pinned commit SHA | Not pinned |
| Reproducible rebuild | No | `llm-pull sync` | No |
| Rollback of an update | No | Old files kept until `gc`; manifest in git | No |
| Integrity check | Ollama's own | sha256 (LFS oid) from HF API | llama.cpp's own |
| Offline | Yes | Yes | Yes with `--offline` after first load |
| Download timing | Explicit | Explicit | On first request, can exceed `healthCheckTimeout` |
| Disk visibility | `ollama list` | `llm-pull du` | `--cache-list` |
| Update detection | None | `llm-pull outdated` | None |
| Mount in container | rw | **read-only** | rw cache volume |
| mmproj handling | Bundled | Explicit file in manifest | Automatic |
