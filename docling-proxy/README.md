# Docling Split Proxy

This service sits in front of `docling-serve` and adds automatic split-processing for PDFs based only on page count. It keeps docling-style endpoints and request shapes, while adding optional proxy controls for chunking, orchestration, and debug metadata.

The proxy now runs conversions through short-lived `docling-serve` processes inside the `docling-proxy` container. For split-processing, each chunk gets its own child process, request, result retrieval, and shutdown cycle. For non-split requests, the proxy still uses isolated child lifecycles, and multi-file uploads are fanned out into one isolated child conversion per file before being reassembled into one final ZIP response. This keeps memory from accumulating inside one long-lived shared `docling-serve` instance and avoids sending a whole large batch through one child request.

All child `DOCLING_SERVE_*` settings are inherited from the proxy container environment, so `docker-compose.yaml` is the source of truth for child runtime configuration. The only exception is bind host and port, which the proxy forces to a localhost address and a per-child ephemeral port. `DOCLING_SERVE_SCRATCH_PATH`, if set, is treated as a base directory and each child gets its own unique subdirectory underneath it.

`DOCLING_PROXY_WORK_CONCURRENCY` controls fan-out within the currently active file, but files themselves are admitted through one process-local FIFO scheduler. Within a single split request, chunks are processed sequentially in page order, and across requests only one file is active at a time per proxy process. `DOCLING_PROXY_ACTIVE_CHILD_LIMIT` controls how many one-shot child `docling-serve` processes may be actively handling work at once. `DOCLING_PROXY_CHILD_LAUNCH_CONCURRENCY` separately controls how many child startups or warm-pool replacements may happen at the same time. `DOCLING_PROXY_WARM_CHILD_POOL_SIZE` controls how many idle ready child instances the proxy keeps prewarmed, and `DOCLING_PROXY_WARM_CHILD_IDLE_TIMEOUT_SEC` controls how long that warm pool may stay idle before those ready instances are shut down. `DOCLING_PROXY_ARCHIVE_DIR` enables persistence of final successful conversion outputs to disk. `DOCLING_PROXY_STATE_DIR` optionally controls where the proxy stores durable task and split-checkpoint state; if unset and `DOCLING_PROXY_ARCHIVE_DIR` is configured, the proxy stores that state under a hidden `.proxy-state/` directory inside the archive root so it survives container restarts on the same mounted volume. `DOCLING_PROXY_UVICORN_WORKERS` controls proxy worker count explicitly; strict global file ordering only holds when this is set to `1`, because each proxy worker process maintains its own FIFO scheduler. Child `docling-serve` instances always run with one HTTP worker and handle exactly one request before shutdown. Advanced conversion parallelism inside a child request remains controlled by passthrough `DOCLING_SERVE_*` settings such as `DOCLING_SERVE_ENG_LOC_NUM_WORKERS`.

## Supported endpoints

- `POST /v1/convert/file`
- `POST /v1/convert/file/async`
- `POST /v1/convert/source`
- `POST /v1/convert/source/async`
- `GET /v1/status/poll/{task_id}`
- `GET /v1/result/{task_id}`

## Optional proxy controls

- Multipart form fields: `proxy_force_split=true`, `proxy_max_pages_per_part=10`, `proxy_work_concurrency=2`, `proxy_include_proxy_meta=true`
- JSON payload field: `"proxy_options": {"force_split": true, "max_pages_per_part": 10, "work_concurrency": 2}`

If `proxy_force_split` is not set, the proxy splits only when the PDF page count is greater than `max_pages_per_part`.

`proxy_max_concurrency` is still accepted as a deprecated alias for `proxy_work_concurrency`.

## Run locally

```bash
cd docling-proxy
docker build -f Dockerfile.docling-proxy -t docling-proxy .
docker run --rm -p 8080:8080 -e DOCLING_SERVE_SCRATCH_PATH=/tmp/docling-serve -e DOCLING_PROXY_ARCHIVE_DIR=/var/lib/docling-output -v "$HOME/llm/docling:/var/lib/docling-output" docling-proxy
```

## Architecture

The proxy is a FastAPI application with a small set of focused modules:

- `src/docling_proxy/app.py` exposes the HTTP API and maps docling-compatible routes to proxy service methods.
- `src/docling_proxy/archive.py` persists final successful conversion outputs and metadata to the configured archive directory.
- `src/docling_proxy/state.py` persists async task metadata, split plans, and per-chunk checkpoint payloads so long-running jobs can resume after proxy restarts.
- `src/docling_proxy/service.py` decides whether to split, starts one-shot local docling instances for isolated conversions, merges split results, and stores async task state.
- `src/docling_proxy/pdf_tools.py` detects PDF inputs, counts pages, computes split plans, and materializes per-chunk PDF byte streams lazily.
- `src/docling_proxy/upstream.py` is now only used for helper downloads such as resolving HTTP PDF sources before split evaluation.
- `src/docling_proxy/local_docling.py` manages the short-lived child `docling-serve` processes used for both split and non-split conversions, maintains an optional warm pool of ready child instances, inherits child `DOCLING_SERVE_*` settings from the container environment, streams child logs into the proxy logs, and keeps a stderr tail for startup failures.
- `src/docling_proxy/merge.py` merges chunk results into a single `DoclingDocument` and exports the final formats.
- `src/docling_proxy/store.py` holds in-memory async proxy jobs.

The proxy never modifies docling’s internal models. Instead, it works at the API layer and decides whether the request can stay as one isolated conversion or must be split into several isolated conversions.

## Request handling model

There are two broad paths through the service:

1. Isolated single-request path: used for non-PDF files, unsupported source shapes, and PDFs that do not exceed `max_pages_per_part`.
2. Split-processing path: used for single PDFs that exceed the page threshold, or when `proxy_force_split=true`.
3. Multi-file batch path: used for `POST /v1/convert/file` requests with more than one uploaded file; each file is converted in its own isolated child request and the proxy merges those per-file outputs into a final `converted_docs.zip`.

In isolated single-request mode, the proxy acquires one ready local `docling-serve` child from its warm pool when available, forwards the original request to it, waits for the response, relays the response back to the caller, and then retires that child process. If warm pooling is disabled or depleted, the proxy starts a new child on demand. After the last in-flight request finishes, the warm pool can remain available for a configurable idle timeout and then drain itself back to zero ready children.

In multi-file batch mode, the proxy stores uploaded files in proxy-managed staging storage and processes files through one process-local FIFO file scheduler. That means one file completes before the next queued file starts, even across separate requests handled by the same proxy process. If a file needs proxy-managed splitting, the proxy completes all of that file's chunks from first to last before moving to the next queued file. Each per-file result is requested or assembled as a ZIP so any docling-generated assets stay together, and the proxy prefixes each file's ZIP contents before merging them into the final batch ZIP.

In split mode, the proxy takes over the conversion workflow and performs chunk orchestration itself.

## Data flow for a large PDF

For a large PDF sent to `POST /v1/convert/file`, the proxy processes it in this order:

1. FastAPI receives the multipart request and `parse_multipart_form()` spools uploaded files into proxy-managed staging storage while separating docling fields from optional proxy fields.
2. For persisted jobs, the proxy adopts that staged file into durable task state instead of copying the same upload a second time.
3. `service.py` checks whether the request is eligible for splitting:
   - exactly one file
   - detected as a PDF
   - page count exceeds `max_pages_per_part`, or `proxy_force_split=true`
4. `pdf_tools.py` opens the PDF with `pypdf.PdfReader`, counts pages once, and computes page ranges for each chunk without materializing every chunk PDF up front.
5. The proxy normalizes output options, forces chunk `target_type` to `inbody`, and ensures `json` is included for chunk processing, because merge happens through `DoclingDocument` JSON payloads.
6. The proxy enqueues chunk work from those planned page ranges immediately, so `queued chunk ...` logs appear before every chunk PDF has been written.
7. For each chunk that actually begins work, the proxy materializes that chunk's PDF bytes on demand, acquires a ready local `docling-serve` child from the warm pool when available, and starts a replacement child in the background to keep the configured idle pool filled.
8. If no ready child is available, the proxy waits for one to finish prewarming.
9. The proxy forwards the chunk to the child server with `POST /v1/convert/file` and waits for the chunk response.
10. The proxy reads `document.json_content` from the chunk response.
11. The proxy shuts down the used child `docling-serve` instance and cleans its unique scratch directory.
12. After each chunk succeeds, the proxy writes that chunk's JSON payload to durable task state on disk.
13. After every chunk succeeds, `merge.py` reconstructs `DoclingDocument` instances from the persisted chunk JSON files and merges them in page order with `DoclingDocument.concatenate(...)`.
14. The merged document is exported again into the formats originally requested by the caller, such as Markdown, JSON, HTML, or text.
15. The proxy returns one final response that looks like a normal docling conversion result.

The same split workflow is used for `source` requests after the proxy resolves the source into PDF bytes. For base64 file sources it decodes the payload; for HTTP sources it downloads the PDF first.

## Detailed large-PDF example

Assume a 73-page PDF arrives with `max_pages_per_part=25`.

- The proxy counts 73 pages.
- It creates 3 chunks: pages `1-25`, `26-50`, and `51-73`.
- For each chunk, it acquires a ready child `docling-serve` from the warm pool when available.
- It starts a replacement child in the background as soon as that ready child is leased.
- It sends the chunk to the leased child server and waits for the response.
- It retires that used child server before moving on to the merged result.
- It merges them in the same order the pages originally appeared.
- It exports one combined Markdown file and any other requested formats.

This preserves document order even though chunks may finish in a different order.

## Sync flow

For sync endpoints like `POST /v1/convert/file`:

- If the request does not split, the proxy runs one isolated local `docling-serve` request and returns that response directly.
- If the request is split, the proxy performs the full split-run-merge workflow inside the request lifecycle and returns the merged final result.

This means the caller still experiences a single synchronous conversion call.

## Async flow

For async endpoints like `POST /v1/convert/file/async`:

1. The proxy creates its own proxy task id.
2. It stores a `ProxyJob` in the in-memory job store and also writes durable task state to disk.
3. It starts a background coroutine to run the conversion workflow.
4. The caller polls the proxy task id, not any internal child process state.

Internally, the proxy may either:

- run one isolated local `docling-serve` request for non-split work, or
- manage many isolated local chunk conversions when split-processing is active.

`GET /v1/status/poll/{task_id}` returns proxy task status plus optional task metadata, including chunk progress. `GET /v1/result/{task_id}` returns the final merged payload after the background workflow finishes. If the proxy restarts while an async task is in progress, the proxy reloads the task from durable state on startup and resumes any missing work from persisted split checkpoints.

## Result archiving

If `DOCLING_PROXY_ARCHIVE_DIR` is set, the proxy writes a copy of each final successful or partial-success conversion result to disk.

- Only final outputs are archived.
- Intermediate split chunks are checkpointed separately in durable task state and are not treated as final archives.
- Failed conversions are not archived.
- Each archived conversion gets its own timestamped directory with a `meta.json` sidecar.
- JSON or in-body responses are written as exported files such as `.md`, `.json`, `.html`, `.txt`, `.yaml`, `.vtt`, or `.doctags` when present.
- ZIP responses are written as a single `.zip` artifact.

With the current `docker-compose.yaml`, archived outputs are stored on the host at `$HOME/llm/docling` through a bind mount to `/var/lib/docling-output` inside the container.

## Merge mechanics

The merge step is intentionally done at the `DoclingDocument` level rather than by concatenating Markdown strings.

That matters because:

- page order stays explicit
- document structure is preserved
- final Markdown, HTML, and text can be re-rendered from one combined document
- downstream consumers can still use combined `json_content`

Each chunk result must therefore contain `document.json_content`. The proxy adds `json` to per-part conversion requests if the caller did not ask for it explicitly, but the final response only includes the formats the caller requested.

## ZIP handling

ZIP output matters when the caller wants downloadable artifacts instead of an in-body JSON response.

There are two ZIP-related scenarios:

### 1. Isolated single-request ZIP

If the proxy does not split the request and the caller asks for ZIP output, the proxy relays the ZIP returned by the isolated local `docling-serve` child process.

### 2. Proxy-assembled ZIP after split-processing

If the proxy does split the PDF and the caller asks for ZIP output:

1. The proxy converts each chunk through its own isolated local `docling-serve` child process.
2. It merges the chunk results into one combined `DoclingDocument`.
3. It renders the final formats from the merged document.
4. It builds a new ZIP archive locally.
5. It writes one file per rendered output into the ZIP, such as:
   - `converted.md`
   - `converted.json`
   - `converted.html`
   - `converted.txt`
6. It returns that ZIP to the caller.

This is different from simply zipping individual chunk outputs. The proxy always returns artifacts from the merged document, not per-part outputs, so the caller receives one coherent final conversion.

## Failure handling

If any chunk fails during split-processing:

- the proxy marks the overall split conversion as failed
- async task status includes the chunk that failed when proxy metadata is present
- no partial merged document is returned

If a child `docling-serve` process fails during startup, the proxy keeps the recent stderr tail and includes it in the raised error so the failure is visible in `docker logs`.

For async requests, the final `GET /v1/result/{task_id}` will not succeed until the proxy task completes successfully.

## Current split eligibility rules

The proxy currently splits only when all of the following are true:

- the request contains exactly one logical document
- that document is a PDF
- the PDF exceeds the page threshold or split is forced

Requests outside those rules still go through docling-compatible processing, but they use one isolated local child server instead of an external long-lived upstream instance.

## Notes on behavior

- Splitting is based only on page count.
- All conversions run through short-lived local `docling-serve` processes inside the proxy container.
- Child `docling-serve` stdout and stderr are streamed into the proxy container logs.
- Child `DOCLING_SERVE_*` configuration comes from the container environment; `DOCLING_SERVE_SCRATCH_PATH` is interpreted as a base path for unique per-child scratch directories.
- `DOCLING_PROXY_WORK_CONCURRENCY` limits concurrent work within the currently active file; files themselves are processed one at a time per proxy process.
- `DOCLING_PROXY_ACTIVE_CHILD_LIMIT` limits how many one-shot child `docling-serve` processes may be busy at once.
- `DOCLING_PROXY_CHILD_LAUNCH_CONCURRENCY` limits simultaneous child startups and warm-pool replacements.
- `DOCLING_PROXY_WARM_CHILD_POOL_SIZE` keeps that many idle ready child instances prelaunched when warm pooling is enabled.
- `DOCLING_PROXY_WARM_CHILD_IDLE_TIMEOUT_SEC` drains those idle ready children after the configured period since the last active request finished.
- Split chunks are processed sequentially in part order.
- Multi-file batch requests process file 1 completely before file 2 begins, including ordered split chunks within each file.
- The same file-by-file ordering also applies across separate requests handled by the same proxy process.
- To preserve that ordering globally in deployment, keep `DOCLING_PROXY_UVICORN_WORKERS=1`.
- Per-request `proxy_work_concurrency` can lower fan-out for one batch request (and concurrent split requests), but it cannot exceed the global proxy work limit.
- Child `docling-serve` HTTP worker count is fixed at `1`; if you need more parallelism inside a single conversion, tune advanced passthrough settings such as `DOCLING_SERVE_ENG_LOC_NUM_WORKERS` instead.
- `proxy_include_proxy_meta=true` can be used to expose split details in the final response.
- Async task state is persisted to disk, so split progress, final async results, and resumable in-flight work survive proxy restarts as long as the configured state directory survives.
