# Known issue: ROCm runtime spins one CPU core while idle

Found 2026-10-12. Not reported upstream; the issue text below is ready to paste into
<https://github.com/ROCm/ROCR-Runtime/issues> if we decide to.

**Effect here:** while the Krita ComfyUI (`comfyui`) is running, one CPU core sits at 100% even with an empty queue.
`bin/gpu-mode llm` stops the container, which stops the spin. TRELLIS (`comfyui-trellis`) is built on the same
torch, so it presumably does the same while it is up (not measured).

**Not a ComfyUI bug.** Ruled out: ComfyUI's prompt worker, the custom nodes, the launch flags, and comfy-aimdo's VRAM
poller (`--disable-nvml-pressure` changed nothing).

**Re-test after every torch/ROCm update** (`bin/check-updates` torch row). Run it in a throwaway container with the
same device args as the `comfyui` service (group IDs are from
`docker-compose.yaml`), and look for one thread at about 500 ticks per 5 s:

```
docker run --rm --device /dev/kfd --device /dev/dri --group-add 983 --group-add 987 --ipc host \
  --security-opt seccomp=unconfined --entrypoint /opt/venv/bin/python comfyui-rocm:<tag> -c \
  "import torch,time; torch.zeros(1,device='cuda'); torch.cuda.synchronize(); time.sleep(60)"
# in another shell: docker top <container> -eLo tid,pcpu,comm    (or top -H)
```

If no thread is busy, the fix has landed. Delete this file and its line in `docs/README.md`.

---

## Issue text (copy from here)

**Title:** AsyncEventsLoop busy-spins in `AMDKFD_IOC_WAIT_EVENTS` at idle (100% of one core), ROCm 7.1/7.2, not 6.4

### Description

After the first GPU call, every process that uses PyTorch's bundled ROCm runtime has one native thread pegging a CPU
core at 100%, even though the GPU is completely idle. The thread is ROCr's `AsyncEventsLoop`. Each call to the KFD
wait-events ioctl returns immediately instead of blocking, so the loop never sleeps.

### Environment

| | |
|---|---|
| GPU | Radeon RX 7800 XT (gfx1101), 16 GB |
| Host kernel | 7.2.9-2-cachyos (in-tree amdgpu/KFD) |
| Userspace | Docker container; `/dev/kfd` and `/dev/dri` passed through, `--group-add video`, `--ipc host`, `seccomp=unconfined` |
| Runtime | `libhsa-runtime64.so` bundled in the PyTorch ROCm wheels (`torch/lib/`) |

### Minimal reproducer

```python
import torch, time
torch.zeros(1, device="cuda"); torch.cuda.synchronize()
time.sleep(120)   # watch with top -H: one thread at ~100%
```

`import torch` alone, with no GPU call, does not spin. The spin starts at runtime init.

### Version matrix (same host, same kernel, same container device args)

We sampled per-thread `utime+stime` from `/proc/<pid>/task/*/stat` twice, 5 s apart. 500 ticks means one full core.

| torch | `torch.version.hip` | Max per-thread ticks / 5 s |
|---|---|---|
| 2.14.1+rocm7.2 | 7.2.53211 | 498 (spins) |
| 2.14.1+rocm7.2, `import torch` only | 7.2.53211 | 0 |
| 2.13.0+rocm7.1 | 7.1.52802 | 499 (spins) |
| 2.9.1+rocm6.4 | 6.4.43484 | 0 (quiet) |

The ROCm 7.0 wheel was not tested, so the regression lies somewhere in 6.4 → 7.1.

### Evidence

gdb attached three times; the hot thread showed the identical stack each time:

```
#0  __GI___ioctl (fd=3, request=3222817548)        # 0xC0184B0C = AMDKFD_IOC_WAIT_EVENTS, fd 3 = /dev/kfd
#1  hsakmt_ioctl ()                                 torch/lib/libhsa-runtime64.so
#2  hsaKmtWaitOnMultipleEvents_ExtCtx ()            torch/lib/libhsa-runtime64.so
#3  rocr::core::Runtime::AsyncEventsLoop(void*) ()  torch/lib/libhsa-runtime64.so
#4  rocr::os::ThreadTrampoline(void*) ()            torch/lib/libhsa-runtime64.so
#5  start_thread
#6  clone3
```

- The thread is always in state R. It made 3 voluntary context switches in about 2000 scheduler switches, and spends
  about 2/3 of its time in kernel mode.
- `/proc/<pid>/task/<tid>/syscall` reads `running` and `wchan` is 0, so it was never caught blocked.
- It made no `read()` calls and its page-fault counters stayed flat.
- No other thread is busy: the main thread is in `epoll_wait` and the worker thread is waiting on a lock.

### Related

The same signature (`ioctl(..., AMDKFD_IOC_WAIT_EVENTS, ...) = 0` in a loop, one core at 100% while idle) was reported
with vLLM on ROCm 7.2.2 / Radeon AI Pro 9700 and on an HX 370 iGPU:
<https://discuss.vllm.ai/t/vllm-0-20-1-radeon-ai-9700-1-cpu-core-at-100/2621>

### Not yet checked

- The timeout argument passed to the ioctl, which would show whether it is 0 or the event is always already signalled.
- Whether `HSA_ENABLE_INTERRUPT=0` changes the behaviour.
- The ROCm 7.0 wheel.
- An older kernel with the 7.x runtime. The quiet 6.4 run on the same kernel argues against a pure kernel cause, but
  a 7.x runtime interacting with a newer KFD can't be excluded.
