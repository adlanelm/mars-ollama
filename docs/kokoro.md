# Kokoro TTS (CPU)

OpenAI-compatible text-to-speech for Open Notebook podcast generation. Runs on the CPU only: the RX 7800 XT stays with llama-swap and ComfyUI.

Back to [README.md](README.md) · [architecture.md](architecture.md) · [licenses.md](licenses.md)

## Service

| Item | Value |
|---|---|
| Compose service / container | `kokoro` |
| Image | `ghcr.io/remsky/kokoro-fastapi-cpu:v0.9.2@sha256:cbcc28802fb5d1eaaba8b91e38b0e872a669e85831c513cf6a29c709cbe25405` (Kokoro-FastAPI v0.9.2, multi-arch index digest) |
| Port | host `8880` on `0.0.0.0` (container `8880`), reached from the k3s cluster on jupiter |
| Limits | `mem_limit: 3g`, `cpus: "6"`, torch/OMP/MKL threads 6. Llama-swap keeps `cpus: "12.8"` and the RAM headroom for MoE expert offload (~12 GiB free with a MoE chat model loaded) |
| Restart | `always`. Healthcheck is a TCP connect to `:8880` (does not wake the model) |
| Idle stop | `kokoro/idle-proxy.py` (mounted, run as the container command) holds `:8880`, starts the image's `./entrypoint.sh` on `127.0.0.1:8881` at the first request and stops it after `IDLE_PROXY_IDLE_SEC=900` without connections. See On-demand start |
| Start | `docker compose up -d --no-deps kokoro` |
| Model weights | Kokoro-82M, baked into the image (no download at start) |

Firewall: if jupiter cannot reach `:8880`, open it the same way as `:11434`. The compose file does not change firewall rules.

## On-demand start

While the server runs, the model stays in RAM: 1.3 GiB right after load. After a few podcast batches it had grown to 2.4 GiB, mostly swapped to zram, while nobody was using it. The container now runs a small stdlib proxy as its main process (`kokoro/idle-proxy.py`, ~20 MiB). The image itself is unchanged.

| Event | What happens |
|---|---|
| First request that sends data | The proxy starts `./entrypoint.sh` with `HOST=127.0.0.1 PORT=8881`, waits for `/health`, then forwards. Adds about 7 s (measured: 7.3 s cold, 1.7 s warm for the same text) |
| Connect without data (healthcheck) | Closed at once. Does not start the model or reset the idle clock |
| No open connections for `IDLE_PROXY_IDLE_SEC` (900) | SIGTERM to the server (SIGKILL after 15 s). Container memory falls to about 10 MiB anon |

Logs: `docker logs kokoro | grep idle-proxy` shows each start and stop. To keep the model loaded all the time, remove the `command:` and `volumes:` lines from the service.

## Use by Open Notebook

| Setting | Value |
|---|---|
| Provider | OpenAI-compatible |
| Base URL | `http://mars.internal:8880/v1` |
| Model id | `kokoro` (any name is accepted by the server; use `kokoro`) |
| API key | any non-empty string (no auth) |
| Endpoints | `POST /v1/audio/speech` (`response_format` mp3, wav, opus, flac, pcm), `GET /v1/audio/voices` |
| Suggested speaker voices | `af_heart` (female, US, grade A) and `am_michael` (male, US). Other good pairs: `af_bella` + `am_adam`; British `bf_emma` + `bm_george` |

About 70 voices are listed. Voices can be mixed in the `voice` field (for example `af_heart+af_bella`).

## Measured on mars (2026-11-01, CPU, 6 threads, warm)

| Input | Audio | Wall | Speed |
|---|---|---|---|
| 57 words, `af_heart`, mp3 | 19.4 s | 3.1 s | 6.3x realtime (RTF 0.16) |
| 57 words, `am_michael`, mp3 | 20.9 s | 3.1 s | 6.8x realtime (RTF 0.15) |

Container RAM while loaded: 1.3 GiB after load (idle stop now unloads it, see On-demand start), 1.7 GiB while synthesising (limit 3 GiB). CPU use during a request was about 5.2 cores (of the 6 allowed). A 10-minute podcast takes about 1.5 minutes of synthesis. llama-swap's aux models (reranker, embeddings) stayed `ready` during the test.

## Licences

Kokoro-FastAPI: Apache-2.0. Kokoro-82M weights: Apache-2.0. See [licenses.md](licenses.md), section 6.
