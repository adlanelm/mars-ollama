# Local colibri patches

Applied by `colibri/build.sh` on top of the v2.0.0 tag, in file-name order (`git apply -p1`).

| Patch | What | Why |
|---|---|---|
| `0001-progress-log.patch` | `c/openai_server.py`: `[progress]` lines on stderr per request: start, first token (prefill time), every `COLI_PROGRESS_SECS` (default 15, `0` = off) while decoding with ~tok and ~tok/s, and a finish summary | Upstream logs only when a request finishes; a 10-minute request was silent in `docker logs`. Token counts are approximate (one engine text callback ≈ one token; an MTP step can carry more) |

Checks: the upstream server suite passes with the patch (`cd c && python3 -B -m unittest tests.test_openai_server`, 425 tests OK). The progress clock is `time.perf_counter()` because those tests pin `time.monotonic()` call by call.

A Python-only patch can be applied to the existing tree without rebuilding the engine:
`cd ~/llm/colibri/bin && git apply -p1 ~/devel/mars-ollama/colibri/patches/<patch>`, then restart llama-swap
(or wait for `colibri-big` to unload). When upgrading colibri, check that each patch still applies; consider sending it upstream.
