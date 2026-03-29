# Docling Split Proxy

This service sits in front of `docling-serve` and adds automatic split-processing for PDFs based only on page count. It keeps docling-style endpoints and request shapes, while adding optional proxy controls for chunking, orchestration, and debug metadata.

The proxy now runs conversions through short-lived `docling-serve` processes inside the `docling-proxy` container. For split-processing, each chunk gets its own child process, request, result retrieval, and shutdown cycle. For non-split requests, the whole request goes through the same isolated lifecycle. This keeps memory from accumulating inside one long-lived shared `docling-serve` instance.

All child `DOCLING_SERVE_*` settings are inherited from the proxy container environment, so `docker-compose.yaml` is the source of truth for child runtime configuration. The only exception is bind host and port, which the proxy forces to a localhost address and a per-child ephemeral port. `DOCLING_SERVE_SCRATCH_PATH`, if set, is treated as a base directory and each child gets its own unique subdirectory underneath it.

`DOCLING_PROXY_MAX_CONCURRENCY` is enforced process-wide in the proxy and limits the number of active child `docling-serve` lifecycles across all requests handled by one proxy process. `DOCLING_PROXY_UVICORN_WORKERS` controls proxy worker count explicitly, and `DOCLING_PROXY_LOCAL_DOCLING_WORKERS` controls the uvicorn worker count for each child `docling-serve` instance.

## Supported endpoints

- `POST /v1/convert/file`
- `POST /v1/convert/file/async`
- `POST /v1/convert/source`
- `POST /v1/convert/source/async`
- `GET /v1/status/poll/{task_id}`
- `GET /v1/result/{task_id}`

## Optional proxy controls

- Multipart form fields: `proxy_force_split=true`, `proxy_max_pages_per_part=10`, `proxy_include_proxy_meta=true`
- JSON payload field: `"proxy_options": {"force_split": true, "max_pages_per_part": 10}`

If `proxy_force_split` is not set, the proxy splits only when the PDF page count is greater than `max_pages_per_part`.

## Run locally

```bash
cd docling-proxy
docker build -f Dockerfile.docling-proxy -t docling-proxy .
docker run --rm -p 8080:8080 -e DOCLING_SERVE_SCRATCH_PATH=/tmp/docling-serve docling-proxy
```

## Architecture

The proxy is a FastAPI application with a small set of focused modules:

- `src/docling_proxy/app.py` exposes the HTTP API and maps docling-compatible routes to proxy service methods.
- `src/docling_proxy/service.py` decides whether to split, starts one-shot local docling instances for isolated conversions, merges split results, and stores async task state.
- `src/docling_proxy/pdf_tools.py` detects PDF inputs, counts pages, computes chunk ranges, and writes per-chunk PDF byte streams.
- `src/docling_proxy/upstream.py` is now only used for helper downloads such as resolving HTTP PDF sources before split evaluation.
- `src/docling_proxy/local_docling.py` manages the short-lived child `docling-serve` processes used for both split and non-split conversions, inherits child `DOCLING_SERVE_*` settings from the container environment, streams child logs into the proxy logs, and keeps a stderr tail for startup failures.
- `src/docling_proxy/merge.py` merges chunk results into a single `DoclingDocument` and exports the final formats.
- `src/docling_proxy/store.py` holds in-memory async proxy jobs.

The proxy never modifies docling’s internal models. Instead, it works at the API layer and decides whether the request can stay as one isolated conversion or must be split into several isolated conversions.

## Request handling model

There are two broad paths through the service:

1. Isolated single-request path: used for non-PDF files, multi-file requests, unsupported source shapes, and PDFs that do not exceed `max_pages_per_part`.
2. Split-processing path: used for single PDFs that exceed the page threshold, or when `proxy_force_split=true`.

In isolated single-request mode, the proxy starts one short-lived local `docling-serve`, forwards the original request to it, waits for the response, relays the response back to the caller, and then shuts that child process down.

In split mode, the proxy takes over the conversion workflow and performs chunk orchestration itself.

## Data flow for a large PDF

For a large PDF sent to `POST /v1/convert/file`, the proxy processes it in this order:

1. FastAPI receives the multipart request and reads the uploaded file into memory.
2. `parse_multipart_form()` separates docling fields from optional proxy fields.
3. `service.py` checks whether the request is eligible for splitting:
   - exactly one file
   - detected as a PDF
   - page count exceeds `max_pages_per_part`, or `proxy_force_split=true`
4. `pdf_tools.py` opens the PDF with `pypdf.PdfReader`, counts pages, and slices it into smaller PDF parts with `PdfWriter`.
5. The proxy normalizes output options, forces chunk `target_type` to `inbody`, and ensures `json` is included for chunk processing, because merge happens through `DoclingDocument` JSON payloads.
6. For each chunk, the proxy starts a fresh local `docling-serve` process inside the proxy container.
7. The proxy waits for the child server readiness endpoint to return success.
8. The proxy forwards the chunk to the child server with `POST /v1/convert/file` and waits for the chunk response.
9. The proxy reads `document.json_content` from the chunk response.
10. The proxy shuts down that child `docling-serve` instance and cleans its unique scratch directory.
11. After every chunk succeeds, `merge.py` reconstructs `DoclingDocument` instances from chunk JSON and merges them in page order with `DoclingDocument.concatenate(...)`.
12. The merged document is exported again into the formats originally requested by the caller, such as Markdown, JSON, HTML, or text.
13. The proxy returns one final response that looks like a normal docling conversion result.

The same split workflow is used for `source` requests after the proxy resolves the source into PDF bytes. For base64 file sources it decodes the payload; for HTTP sources it downloads the PDF first.

## Detailed large-PDF example

Assume a 73-page PDF arrives with `max_pages_per_part=25`.

- The proxy counts 73 pages.
- It creates 3 chunks: pages `1-25`, `26-50`, and `51-73`.
- For each chunk, it starts a fresh local `docling-serve` process.
- It sends the chunk to that child server and waits for the response.
- It shuts that child server down before moving on to the merged result.
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
2. It stores a `ProxyJob` in the in-memory job store.
3. It starts a background coroutine to run the conversion workflow.
4. The caller polls the proxy task id, not any internal child process state.

Internally, the proxy may either:

- run one isolated local `docling-serve` request for non-split work, or
- manage many isolated local chunk conversions when split-processing is active.

`GET /v1/status/poll/{task_id}` returns proxy task status plus optional task metadata, including chunk progress. `GET /v1/result/{task_id}` returns the final merged payload after the background workflow finishes.

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
- Chunk completion order does not affect final merge order.
- The default split concurrency is `1` so only one local child server is active at a time unless you explicitly raise it.
- The concurrency cap is global per proxy process, not just per split request.
- `proxy_include_proxy_meta=true` can be used to expose split details in the final response.
- Async task state is currently in memory, so restarting the proxy clears in-flight proxy jobs.
