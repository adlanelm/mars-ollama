#!/usr/bin/env python3
"""Text-to-music with ACE-Step 1.5, one generation per seed (runs inside the acestep image).

Settings default to upstream's 12-16 GB tier ("tier5" in acestep/gpu_config.py): turbo DiT on the GPU with
INT8 weight-only quantization (needs torch.compile), VAE + text encoder offloaded to the CPU, 1.7B LM on the
PyTorch backend (nano-vllm needs flash-attn, not available on ROCm). --lm 0.6B / --no-lm are the lower tiers.

Writes <out>/<name>_s<seed>.wav (48 kHz, as produced) and <name>_s<seed>.json (all params + LM CoT output +
timings). See docs/acestep.md.
"""
import argparse
import json
import os
import shutil
import time

import torch

from acestep.handler import AceStepHandler
from acestep.inference import GenerationConfig, GenerationParams, generate_music
from acestep.llm_inference import LLMHandler


def jsonable(v):
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items() if not isinstance(x, torch.Tensor)}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    return repr(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--caption", required=True)
    ap.add_argument("--duration", type=float, default=75)
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--bpm", type=int, default=None)
    ap.add_argument("--keyscale", default="")
    ap.add_argument("--timesignature", default="")
    ap.add_argument("--steps", type=int, default=8)
    ap.add_argument("--shift", type=float, default=3.0, help="3.0 recommended for turbo")
    ap.add_argument("--dit", default="acestep-v15-turbo")
    ap.add_argument("--lm", default="acestep-5Hz-lm-1.7B")
    ap.add_argument("--no-lm", action="store_true", help="DiT only (no LM planning / audio codes)")
    ap.add_argument("--no-quant", action="store_true")
    ap.add_argument("--out", default="/out")
    ap.add_argument("--name", default="gen")
    a = ap.parse_args()

    ckpt = os.environ.get("ACESTEP_CHECKPOINTS_DIR", "/ckpt")
    t0 = time.time()
    dit = AceStepHandler()
    msg, ok = dit.initialize_service(
        project_root=os.environ.get("ACESTEP_PROJECT_ROOT", "/opt/acestep"),
        config_path=a.dit,
        device="cuda",
        use_flash_attention=False,
        compile_model=not a.no_quant,
        offload_to_cpu=True,
        offload_dit_to_cpu=False,
        quantization=None if a.no_quant else "int8_weight_only",
    )
    print(f"[gen] DiT init ok={ok}: {msg}", flush=True)
    if not ok:
        raise SystemExit(1)
    llm = LLMHandler()
    if not a.no_lm:
        msg, ok = llm.initialize(checkpoint_dir=ckpt, lm_model_path=a.lm, backend="pt", device="cuda", offload_to_cpu=True)
        print(f"[gen] LM init ok={ok}: {msg}", flush=True)
        if not ok:
            raise SystemExit(1)
    t_init = time.time() - t0
    print(f"[gen] init {t_init:.1f}s, torch max allocated {torch.cuda.max_memory_allocated() / 2**30:.2f} GiB", flush=True)

    os.makedirs(a.out, exist_ok=True)
    for seed in [int(s) for s in a.seeds.split(",")]:
        torch.cuda.reset_peak_memory_stats()
        params = GenerationParams(
            task_type="text2music", caption=a.caption, lyrics="[Instrumental]", instrumental=True,
            bpm=a.bpm, keyscale=a.keyscale, timesignature=a.timesignature, duration=a.duration,
            inference_steps=a.steps, shift=a.shift, seed=seed, thinking=not a.no_lm,
        )
        cfg = GenerationConfig(batch_size=1, use_random_seed=False, seeds=[seed], audio_format="wav")
        t1 = time.time()
        tmp = os.path.join(a.out, f".tmp_{seed}")
        res = generate_music(dit, llm, params, cfg, save_dir=tmp)
        dt = time.time() - t1
        peak = torch.cuda.max_memory_allocated() / 2**30
        if not res.success:
            print(f"[gen] seed {seed} FAILED: {res.error}", flush=True)
            continue
        audio = res.audios[0]
        base = os.path.join(a.out, f"{a.name}_s{seed}")
        shutil.move(audio["path"], base + ".wav")
        shutil.rmtree(tmp, ignore_errors=True)
        meta = {
            "model": {"dit": a.dit, "lm": None if a.no_lm else a.lm, "quantization": None if a.no_quant else "int8_weight_only",
                      "offload_to_cpu": True, "offload_dit_to_cpu": False, "lm_backend": "pt",
                      "weights": "ACE-Step/Ace-Step1.5@19671f406d603126926c1b7e2adc169acbcade22",
                      "code": "ace-step/ACE-Step-1.5@" + os.environ.get("ACESTEP_COMMIT", "ca1e85fe9430179831e6bc6be790c332190a3866")},
            "request": vars(a) | {"seed": seed},
            "sample_rate": audio.get("sample_rate"),
            "params_used": jsonable(audio.get("params", {})),
            "lm_metadata": jsonable(res.extra_outputs.get("lm_metadata")),
            "time_costs": jsonable(res.extra_outputs.get("time_costs")),
            "seconds": round(dt, 1), "init_seconds": round(t_init, 1),
            "torch_max_allocated_gib": round(peak, 2),
        }
        meta["params_used"].pop("audio_codes", None)
        with open(base + ".json", "w") as f:
            json.dump(meta, f, indent=1)
        print(f"[gen] seed {seed}: {dt:.1f}s, torch peak {peak:.2f} GiB -> {base}.wav", flush=True)


if __name__ == "__main__":
    main()
