"""Idle-stop proxy for Kokoro-FastAPI, run as the container's main process (stdlib only).

Listens on LISTEN_PORT. The first connection that sends data starts the image's own ./entrypoint.sh on
127.0.0.1:BACKEND_PORT and waits for /health. After IDLE_SEC with no open connections the server is stopped,
so the idle container holds only this process (~20 MiB) instead of the loaded model (1.3-2.4 GiB).
A connect-and-close without data (the compose healthcheck) does not start the server.
"""

import asyncio
import os
import signal
import sys
import time

LISTEN_PORT = int(os.environ.get("IDLE_PROXY_LISTEN_PORT", "8880"))
BACKEND_PORT = int(os.environ.get("IDLE_PROXY_BACKEND_PORT", "8881"))
IDLE_SEC = float(os.environ.get("IDLE_PROXY_IDLE_SEC", "900"))
START_TIMEOUT_SEC = float(os.environ.get("IDLE_PROXY_START_TIMEOUT_SEC", "120"))
STOP_TIMEOUT_SEC = 15.0


def log(msg: str) -> None:
    print(f"idle-proxy: {msg}", file=sys.stderr, flush=True)


class Backend:
    def __init__(self) -> None:
        self.proc: asyncio.subprocess.Process | None = None
        self.lock = asyncio.Lock()
        self.active = 0
        self.last_activity = time.monotonic()

    def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def ensure_started(self) -> None:
        async with self.lock:
            if self.running():
                return
            env = dict(os.environ, HOST="127.0.0.1", PORT=str(BACKEND_PORT))
            started = time.monotonic()
            log(f"starting kokoro on 127.0.0.1:{BACKEND_PORT}")
            self.proc = await asyncio.create_subprocess_exec("./entrypoint.sh", env=env, start_new_session=True)
            while time.monotonic() - started < START_TIMEOUT_SEC:
                if self.proc.returncode is not None:
                    raise RuntimeError(f"kokoro exited during startup (code {self.proc.returncode})")
                if await self._healthy():
                    log(f"kokoro ready in {time.monotonic() - started:.1f}s")
                    return
                await asyncio.sleep(0.5)
            await self._stop_locked()
            raise RuntimeError("kokoro did not become healthy in time")

    async def _healthy(self) -> bool:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", BACKEND_PORT)
        except OSError:
            return False
        try:
            writer.write(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            status = await asyncio.wait_for(reader.readline(), 5)
            return b" 200 " in status
        except (OSError, asyncio.TimeoutError):
            return False
        finally:
            writer.close()

    async def stop(self) -> None:
        async with self.lock:
            await self._stop_locked()

    async def _stop_locked(self) -> None:
        proc = self.proc
        if proc is None or proc.returncode is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            await asyncio.wait_for(proc.wait(), STOP_TIMEOUT_SEC)
        except ProcessLookupError:
            pass
        except asyncio.TimeoutError:
            os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()
        log("kokoro stopped")


backend = Backend()


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            backend.last_activity = time.monotonic()
            writer.write(data)
            await writer.drain()
    except (OSError, asyncio.IncompleteReadError):
        pass
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
        except OSError:
            pass


async def handle(client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
    upstream_w = None
    try:
        first = await client_r.read(65536)
    except OSError:
        first = b""
    if not first:
        client_w.close()  # connect-only probe (healthcheck): neither wakes the model nor resets the idle clock
        return
    backend.active += 1
    backend.last_activity = time.monotonic()
    try:
        await backend.ensure_started()
        upstream_r, upstream_w = await asyncio.open_connection("127.0.0.1", BACKEND_PORT)
        upstream_w.write(first)
        await upstream_w.drain()
        await asyncio.gather(pipe(client_r, upstream_w), pipe(upstream_r, client_w))
    except Exception as exc:  # noqa: BLE001 - one bad connection must not kill the proxy
        log(f"connection failed: {exc}")
    finally:
        backend.active -= 1
        backend.last_activity = time.monotonic()
        for w in (upstream_w, client_w):
            if w is not None:
                w.close()


async def reaper() -> None:
    while True:
        await asyncio.sleep(15)
        if backend.running() and backend.active == 0 and time.monotonic() - backend.last_activity >= IDLE_SEC:
            log(f"idle for {IDLE_SEC:.0f}s, stopping kokoro")
            await backend.stop()


async def main() -> None:
    server = await asyncio.start_server(handle, "0.0.0.0", LISTEN_PORT)
    log(f"listening on :{LISTEN_PORT}, kokoro starts on first request, stops after {IDLE_SEC:.0f}s idle")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    reap = asyncio.create_task(reaper())
    async with server:
        await stop.wait()
    reap.cancel()
    await backend.stop()


if __name__ == "__main__":
    asyncio.run(main())
