#!/usr/bin/env python3
"""Regenerates the API-format workflows here (hand-built from the official workflow_templates nodes/defaults). stdlib only."""
import json, pathlib
out = pathlib.Path(__file__).parent
PROMPT = "low-poly rusty maintenance robot, isometric view, 1999 video game render"
def save(name, g): (out / f"{name}.json").write_text(json.dumps(g, indent=1) + "\n")
def L(n, i=0): return [n, i]
# Krea 2 VAE decode: 0 = plain VAEDecode (current default); N = VAEDecodeTiled with tile_size N, overlap 64.
# Plain decode OOMs at 1024^2 on 16 GB (3.4 GiB alloc, ~2.1 GiB free) and ComfyUI retries tiled. Tiled 512 removes the OOM
# but measured ~1 s/image slower (31.2 vs 30.1 s) with identical output, so plain stays (bench/comfyui-results.md,
# "Tiled VAE decode"); 768 still OOMs. Per-run override without regenerating: comfy-sweep --variant NAME:tile=512.
KREA_VAE_TILE = 0
def tail(vae, samples, tile=0):
    dec = ({"class_type": "VAEDecodeTiled", "inputs": {"samples": samples, "vae": vae, "tile_size": tile, "overlap": 64,
            "temporal_size": 64, "temporal_overlap": 8}} if tile else
           {"class_type": "VAEDecode", "inputs": {"samples": samples, "vae": vae}})
    return {"dec": dec,
            "save": {"class_type": "SaveImage", "inputs": {"images": L("dec"), "filename_prefix": "bench"}}}

# Z-Image Turbo: 8 steps, res_multistep/simple, cfg 1, AuraFlow shift 3
save("z_image_turbo", {
 "unet": {"class_type": "UNETLoader", "inputs": {"unet_name": "z_image_turbo_bf16.safetensors", "weight_dtype": "default"}},
 "clip": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen_3_4b.safetensors", "type": "lumina2", "device": "default"}},
 "vae": {"class_type": "VAELoader", "inputs": {"vae_name": "ae.safetensors"}},
 "pos": {"class_type": "CLIPTextEncode", "inputs": {"clip": L("clip"), "text": PROMPT}},
 "neg": {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": L("pos")}},
 "lat": {"class_type": "EmptySD3LatentImage", "inputs": {"width": 1024, "height": 1024, "batch_size": 1}},
 "ms": {"class_type": "ModelSamplingAuraFlow", "inputs": {"model": L("unet"), "shift": 3}},
 "ks": {"class_type": "KSampler", "inputs": {"model": L("ms"), "positive": L("pos"), "negative": L("neg"), "latent_image": L("lat"),
        "seed": 0, "steps": 8, "cfg": 1, "sampler_name": "res_multistep", "scheduler": "simple", "denoise": 1}},
 **tail(L("vae"), L("ks"), KREA_VAE_TILE)})

# FLUX.2 Klein 4B distilled: 4 steps, euler, CFGGuider 1, Flux2Scheduler
save("flux2_klein_4b", {
 "unet": {"class_type": "UNETLoader", "inputs": {"unet_name": "flux-2-klein-4b.safetensors", "weight_dtype": "default"}},
 "clip": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen_3_4b.safetensors", "type": "flux2", "device": "default"}},
 "vae": {"class_type": "VAELoader", "inputs": {"vae_name": "flux2-vae.safetensors"}},
 "pos": {"class_type": "CLIPTextEncode", "inputs": {"clip": L("clip"), "text": PROMPT}},
 "neg": {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": L("pos")}},
 "guider": {"class_type": "CFGGuider", "inputs": {"model": L("unet"), "positive": L("pos"), "negative": L("neg"), "cfg": 1}},
 "noise": {"class_type": "RandomNoise", "inputs": {"noise_seed": 0}},
 "samp": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "euler"}},
 "sig": {"class_type": "Flux2Scheduler", "inputs": {"steps": 4, "width": 1024, "height": 1024}},
 "lat": {"class_type": "EmptyFlux2LatentImage", "inputs": {"width": 1024, "height": 1024, "batch_size": 1}},
 "sca": {"class_type": "SamplerCustomAdvanced", "inputs": {"noise": L("noise"), "guider": L("guider"), "sampler": L("samp"), "sigmas": L("sig"), "latent_image": L("lat")}},
 **tail(L("vae"), L("sca"))})

# Krea 2 Turbo fp8: 6 steps (blind sweep 2026-10-11: no visible loss vs 8, 29 s vs 39 s; use 8 for hero renders), euler/simple, cfg 1 (template without LoRA / prompt-expansion branch)
save("krea2_turbo", {
 "unet": {"class_type": "UNETLoader", "inputs": {"unet_name": "krea2_turbo_fp8_scaled.safetensors", "weight_dtype": "default"}},
 "clip": {"class_type": "CLIPLoader", "inputs": {"clip_name": "qwen3vl_4b_fp8_scaled.safetensors", "type": "krea2", "device": "default"}},
 "vae": {"class_type": "VAELoader", "inputs": {"vae_name": "qwen_image_vae.safetensors"}},
 "pos": {"class_type": "CLIPTextEncode", "inputs": {"clip": L("clip"), "text": PROMPT}},
 "neg": {"class_type": "ConditioningZeroOut", "inputs": {"conditioning": L("pos")}},
 "lat": {"class_type": "EmptyLatentImage", "inputs": {"width": 1024, "height": 1024, "batch_size": 1}},
 "ks": {"class_type": "KSampler", "inputs": {"model": L("unet"), "positive": L("pos"), "negative": L("neg"), "latent_image": L("lat"),
        "seed": 0, "steps": 6, "cfg": 1, "sampler_name": "euler", "scheduler": "simple", "denoise": 1}},
 **tail(L("vae"), L("ks"), KREA_VAE_TILE)})
