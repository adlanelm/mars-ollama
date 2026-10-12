# SPDX-License-Identifier: MIT
"""gpu_arbiter: frees the GPU from llama-swap before ComfyUI queues a prompt. Spec: docs/gpu-switching.md.

Our own node package, bind-mounted read-only into the comfyui and comfyui-trellis containers (docker-compose.yaml).
It adds no nodes. It wraps POST /prompt and /api/prompt in an aiohttp middleware. Before the prompt is queued:
  1. wait while llama-swap serves or loads a GPU model: a model whose cmd uses gpu-guard is not 'ready', or an
     in-flight request (the inflight snapshot on /api/events) targets a model other than the unguarded aux models,
  2. announce the admission (GET /gpu-arbiter -> {"admitting": 1}, read by llama-swap/gpu-guard), check again,
  3. POST /api/models/unload/<id> for each running guarded model (the aux models stay loaded), then queue the prompt.
Steps 2-3 close the race with gpu-guard: an LLM request that arrives meanwhile is in flight before gpu-guard runs,
so one side always sees the other, and on a tie the LLM goes first. After GPU_ARBITER_MAX_WAIT seconds (default 600)
the prompt is refused with HTTP 503. If llama-swap is not reachable, prompts pass through unchanged.
"""
import asyncio
import glob
import json
import logging
import os
import time

import aiohttp
from aiohttp import web
from server import PromptServer

LLM = os.environ.get("GPU_ARBITER_LLM_URL", "http://llama-swap:11434").rstrip("/")
MAX_WAIT = float(os.environ.get("GPU_ARBITER_MAX_WAIT", "600"))
GUARD = "gpu-guard"  # marker in a llama-swap model cmd: the model uses the GPU and is arbitrated
PATHS = ("/prompt", "/api/prompt")

log = logging.getLogger("gpu_arbiter")
_lock = asyncio.Lock()
_state = {"admitting": 0, "waiting": 0}

NODE_CLASS_MAPPINGS = {}


async def _inflight(s):
    """Models of the requests llama-swap is handling right now (snapshot sent first on its event stream)."""
    async with s.get(f"{LLM}/api/events") as r:
        async for line in r.content:
            if line.startswith(b"data:"):
                ev = json.loads(line[5:])
                if ev.get("type") == "inflight":
                    return [q.get("model", "") for q in json.loads(ev["data"]).get("requests") or []]
    return []


async def _llm(s):
    """(running guarded model ids, reasons the LLM side is busy)."""
    async with s.get(f"{LLM}/running") as r:
        running = (await r.json())["running"]
    guarded = [m for m in running if GUARD in m.get("cmd", "")]
    aux = {m["model"] for m in running if GUARD not in m.get("cmd", "")}
    busy = [f"{m['model']} {m['state']}" for m in guarded if m.get("state") != "ready"]
    try:
        inflight = await asyncio.wait_for(_inflight(s), 5)
    except asyncio.TimeoutError:
        log.warning("gpu_arbiter: no inflight snapshot from llama-swap; judging by /running only")
        inflight = []
    busy += [f"request for {m}" for m in inflight if m and m not in aux]
    return [m["model"] for m in guarded], busy


async def _unload(s, models):
    t = time.monotonic()
    before = _vram_used()
    for m in models:
        async with s.post(f"{LLM}/api/models/unload/{m}") as r:
            if r.status >= 400:
                log.warning("gpu_arbiter: unloading %s returned HTTP %s", m, r.status)
    while time.monotonic() - t < 60:  # the unload call normally returns after the process exited
        async with s.get(f"{LLM}/running") as r:
            left = [x["model"] for x in (await r.json())["running"] if x["model"] in models]
        if not left:
            break
        await asyncio.sleep(0.25)
    # /running drops the model before the driver has released its VRAM (0.1-0.2 s for 14 GB). Wait until it has.
    # Read sysfs, not torch: HIP's free-memory figure does not count llama.cpp's Vulkan buffers.
    prev = cur = _vram_used()
    while cur is not None and time.monotonic() - t < 10:
        await asyncio.sleep(0.2)
        cur = _vram_used()
        settled = prev - cur < 32 * 2**20
        if settled and (cur < before - 256 * 2**20 or time.monotonic() - t > 2):
            break
        prev = cur
    log.info("gpu_arbiter: unloaded LLM %s in %.1f s (VRAM used %s)", ", ".join(models), time.monotonic() - t,
             f"{before // 2**20} -> {cur // 2**20} MiB" if cur is not None else "unknown")


def _card():
    """sysfs dir of the GPU with the most VRAM (the RX 7800 XT; card0 is the iGPU), or None."""
    cards = []
    for d in glob.glob("/sys/class/drm/card[0-9]*/device/mem_info_vram_total"):
        try:
            cards.append((int(open(d).read()), os.path.dirname(d)))
        except (OSError, ValueError):
            pass
    return max(cards)[1] if cards else None


CARD = _card()


def _vram_used():
    try:
        return int(open(f"{CARD}/mem_info_vram_used").read()) if CARD else None
    except (OSError, ValueError):
        return None


async def _admit():
    """Return None when the prompt may be queued (with _state['admitting'] set), else an error message."""
    t0 = time.monotonic()
    announced = False
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, sock_connect=3)) as s:
            while True:
                running, busy = await _llm(s)
                if not busy:
                    _state["admitting"] = 1
                    running, busy = await _llm(s)  # re-check after announcing (see module docstring)
                    if not busy:
                        if running:
                            await _unload(s, running)
                        if announced:
                            log.info("gpu_arbiter: waited %.1f s for the LLM", time.monotonic() - t0)
                        return None
                    _state["admitting"] = 0
                if time.monotonic() - t0 > MAX_WAIT:
                    return f"GPU busy: llama-swap still serving after {MAX_WAIT:.0f} s ({'; '.join(busy)})"
                if not announced:
                    log.info("gpu_arbiter: waiting for the LLM (%s)", "; ".join(busy))
                    announced = True
                _state["waiting"] = 1
                await asyncio.sleep(1)
    except (aiohttp.ClientError, OSError, asyncio.TimeoutError, ValueError, KeyError) as e:
        log.warning("gpu_arbiter: llama-swap at %s not usable (%s); queueing without arbitration", LLM, e)
        _state["admitting"] = 1
        return None
    finally:
        _state["waiting"] = 0


@web.middleware
async def gpu_arbiter_middleware(request, handler):
    if request.method != "POST" or request.path not in PATHS:
        return await handler(request)
    async with _lock:  # one admission at a time; the queue is filled in order
        try:
            err = await _admit()
            if err:
                log.warning("gpu_arbiter: refusing prompt: %s", err)
                return web.json_response({"error": {"type": "gpu_busy", "message": err, "details": err,
                                                    "extra_info": {}}, "node_errors": {}}, status=503)
            return await handler(request)  # queued when this returns
        finally:  # also when the client disconnects while waiting (task cancelled)
            _state["admitting"] = 0


@PromptServer.instance.routes.get("/gpu-arbiter")
async def gpu_arbiter_state(request):
    return web.json_response(dict(_state, llm=LLM, max_wait=MAX_WAIT))


PromptServer.instance.app.middlewares.append(gpu_arbiter_middleware)
log.info("gpu_arbiter: arbitrating POST /prompt against llama-swap at %s (max wait %.0f s)", LLM, MAX_WAIT)
