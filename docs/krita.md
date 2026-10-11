# Krita + krita-ai-diffusion on the local ComfyUI

How to paint with [krita-ai-diffusion](https://github.com/Acly/krita-ai-diffusion) **v1.53.0** on top of this repo's ComfyUI container (`comfyui` service, `bin/gpu-mode comfy`, port `127.0.0.1:8188`). Krita is a desktop app that you install yourself. The plugin connects to ComfyUI as a **Custom Server**, so it never installs or downloads anything on its own.

Server side status (checked 2026-10-11): all required custom nodes are present, and the plugin's workload check passes for **Flux 2 Klein 4B, Z-Image, Krea 2 and Qwen Edit Plus** (Qwen-Image-Edit-2511). A test graph with MAT inpaint, OmniSR upscale and `ETN_SaveImageCache` ran successfully on the GPU. **Changed 2026-10-12 (licence, [licenses.md](licenses.md) P1-c/P2-b):** MAT (CC BY-NC) and 4x NMKD (unclear licence) were removed; the inpaint pre-fill is LaMa `big-lama.pt` (Apache-2.0) and the default upscaler is OmniSR 4x. ComfyUI lists both.

User test in Krita (Flatpak 5.3.4.1, plugin 1.53.0, 2026-10-11): connected as custom server; generate with Klein, generate with Krea 2 (6 steps), inpaint with Klein, and Upscale workspace (4x NMKD Superscale, 2x, refine off) all worked. Times in the ComfyUI log: 24–54 s per generation, 123 s for the first Klein job (model load plus a VAE decode that asked for 31.6 GB and fell back to tiled decoding), 86 s for the upscale; both long ones point to a canvas well above 1024². Use a 1024×1024 document (or selection) for normal work.

## 1. Install Krita

The plugin needs Krita **5.2 or newer**. Plugin 1.x is the line for Krita 5.x; 2.x will target Krita 6 ([installation docs](https://github.com/Acly/krita-ai-diffusion/blob/v1.53.0/docs/src/content/docs/installation.mdx)). So Krita **5.3.x works with v1.53.0**.

Installed here: Flatpak `org.kde.krita` **5.3.4.1** (Flathub, system install). The alternative is `sudo pacman -S krita`; skip it if you already have the Flatpak.

Flatpak notes:
- `flatpak info --show-permissions org.kde.krita` shows `shared=ipc;network;`. The sandbox shares the host network, so `http://127.0.0.1:8188` reaches the container's localhost-only port binding. No `flatpak override` is needed.
- Plugins and plugin data live in `~/.var/app/org.kde.krita/data/krita/pykrita/` (`ai_diffusion/` plus `ai_diffusion.desktop`). The plugin's user data and logs go to `~/.var/app/org.kde.krita/data/krita/ai_diffusion/logs/client.log` (Qt AppDataLocation; `util.py`). The "View log files" link in the plugin settings opens that folder.

## 2. Install the plugin

1. Download `krita_ai_diffusion-1.53.0.zip` from <https://github.com/Acly/krita-ai-diffusion/releases/tag/v1.53.0>. Pick the plugin zip, not the source archive.
2. In Krita, go to **Tools > Scripts > Import Python Plugin from File...**, select the zip and answer **Yes** to enable it. This works the same in the Flatpak, which unpacks it into the `pykrita` folder above.
3. Restart Krita.
4. Open the docker with **Settings > Dockers > AI Image Generation**. Create or open a document first, because the docker is inactive without a canvas.

## 3. Connect to ComfyUI (every session)

```bash
bin/gpu-mode comfy     # unloads LLMs, re-warms aux models, starts ComfyUI on 127.0.0.1:8188
```

In the docker, click **Configure** (gear icon), then **Connection**:
- Select **Custom Server (local or remote)**.
- Enter the URL `http://127.0.0.1:8188` and click **Connect**.
- Expected result: "Connected". The plugin then reads the model list (`/api/etn/model_info`). On first connect it may create a default style.

When you're done:

```bash
bin/gpu-mode llm --stop   # stops ComfyUI, LLMs load on demand again
```

The plugin only talks to the server, so you can keep Krita open while ComfyUI is stopped. Click Connect again after the next `bin/gpu-mode comfy`.

## 4. Styles and models for the game

Styles are under **Configure > Styles**. The plugin ships styles whose model lists match our files: *Flux 2 Klein*, *General Editing* (Klein), *Z-Image Turbo* and *Krea 2 Turbo*. *Qwen Edit* needs the model picked by hand.

| Use | Style / model | Settings |
|---|---|---|
| Fast iteration, sketch-to-image, edits ("make it rusty", style or pose from a reference) | **Flux 2 Klein** / **General Editing** (`flux-2-klein-4b.safetensors`) | 4 steps (default). About 8 s per 1024² image. Fill/Expand uses the Klein outpaint LoRA automatically |
| Quality concepts, textures, turnarounds | **Krea 2 Turbo** (`krea2_turbo_fp8_scaled.safetensors`) | Change the style's steps from 8 to **6** (about 29 s at 1024², no visible loss in our blind test, see [../bench/comfyui-results.md](../bench/comfyui-results.md)). Use 8 for hero renders (about 39 s) |
| Alternative t2i with control layers (scribble, line, depth, pose) | **Z-Image Turbo** (`z_image_turbo_bf16.safetensors`) | 9 steps (default). About 26-32 s. Control layers use the Z-Image ControlNet union model patch |
| Instruction edits | **Qwen Edit**: create a new style and pick `qwen-image-edit-2511-Q4_K_M.gguf` | Experimental in the plugin. Not speed-tested here; expect slow runs (7B text encoder plus a 13 GB GGUF swap on 16 GB) |

**PS1 look:** there is no PS1 LoRA yet, so steer with the prompt, for example "low-poly 1999 PlayStation game render, low resolution texture, vertex lighting, dithered, isometric view". Add these terms to the style's *Style Prompt* (`{prompt}, low-poly PS1 ...`) so they stick. The real PS1 downscale and dither happens later in the pipeline ([game-asset-workflow.md](game-asset-workflow.md)), not in Krita. Work at about 1024², because these models do poorly at native 256 px.

Not installed on purpose: SD 1.5 and SDXL (with their IP-Adapter, CLIP vision and ControlNets), Flux 1, Illustrious, Chroma, Anima and ERNIE. The plugin connects without them because it only needs one supported workload. These styles show as unavailable.

## 5. Limitations on this card (RX 7800 XT, 16 GB)

- One model family at a time. Z-Image and Krea don't fit together with their text encoders on 16 GB, so ComfyUI swaps them per run. Switching styles costs a reload of about 20-60 s on the first image.
- Krea 2 fp8 is upcast on RDNA3 (no fp8 matmul) and uses the whole card. Close other GPU apps.
- Live painting mode: use Klein, since the others are too slow for it.
- Regions, attention guidance and LCM are SD/SDXL-only features and are not available with these models.
- ControlNet preprocessors (scribble, line art, depth, pose) download their annotator weights from Hugging Face into the container on first use. The container is not a volume, so they download again after the container is recreated.
- Upscale: the "Upscale" workspace works with the installed OmniSR 2x/3x/4x upscalers (OmniSR 4x is the default since NMKD was removed on 2026-10-12). "Refine after upscale" with Z-Image would use the tile model patch, which is not installed.

## 6. Troubleshooting

- **"Connection refused"**: ComfyUI is not running. Run `bin/gpu-mode comfy` and wait about 10 s (`bin/gpu-mode status` shows `COMFY running`).
- **"Missing custom nodes" or "No supported workload"**: the connection panel lists what is missing per base model. Server side, see the container logs: `docker logs comfyui | tail -50`. Client side, see `client.log` (link in Configure > Connection). It lists every search pattern and every file found. Model filenames must contain the plugin's search pattern, for example `big-lama`, `OmniSR_X4`, `OmniSR_X2`, `qwen_3_4b`, `flux2` (VAE) or `qwen_image` (VAE). Models come from `comfyui/models.txt` and `bin/comfy-pull`. Don't rename the files.
- **Style shows a warning or no model**: open the style and pick a checkpoint that exists on the server, or click Configure > Connection > refresh after adding models.
- **Out of memory or very slow**: check `bin/gpu-mode status` for leftover LLMs and switch to Klein. Generate at 1024² or below.
- **Flatpak can't connect but `curl localhost:8188/system_stats` works on the host**: run `flatpak info --show-permissions org.kde.krita`. Only if `network` is missing, run `flatpak override --user --share=network org.kde.krita`.

## Reference: what the plugin checks (v1.53.0)

Sources: `ai_diffusion/backend/resources.py` (architectures, `required_custom_nodes`, `search_paths`, `required_resource_ids`), `ai_diffusion/presets/models.json` (download URLs and folders), `ai_diffusion/backend/comfy_client.py` (`connect`, `discover_models`, `_check_workload`), [ComfyUI setup docs](https://github.com/Acly/krita-ai-diffusion/blob/v1.53.0/docs/src/content/docs/comfyui-setup.mdx).

- **Custom nodes (required):** comfyui_controlnet_aux, ComfyUI_IPAdapter_plus, comfyui-tooling-nodes and comfyui-inpaint-nodes. ComfyUI-GGUF is optional. All are baked into `comfyui/Dockerfile` at the plugin's commits.
- **ComfyUI version:** the plugin pins `4da9e2d` (= ComfyUI v0.33.3) only for its managed local install. For a custom server it checks node classes, not the version, so our v0.39.0 is fine.
- **Required for every workload:** the default upscaler (`4x_NMKD-Superscale-SP_178000_G.pth`, else OmniSR 4x; here OmniSR 4x, since NMKD is deliberately absent), `OmniSR_X2_DIV2K` and the inpaint pre-fill model (`MAT_Places512_G_fp16`, else LaMa `big-lama.pt`; here LaMa: MAT is CC BY-NC and must not be put back in `models/inpaint`, because the plugin would pick it first).
- **Per workload:** a diffusion model the tooling nodes identify as that architecture, plus its text encoder and VAE. Klein 4B needs `qwen_3_4b` and a `flux2` VAE. Z-Image needs `qwen_3_4b` and `ae.safetensors`. Krea 2 needs `qwen3vl_4b` and `qwen_image` VAE. Qwen Edit (Plus for 2509/2511) needs `qwen_2.5_vl_7b` and a `qwen` VAE.
- **Discovery:** model lists come from `/object_info` loader inputs (`VAELoader`, `DualCLIPLoader`, `UpscaleModelLoader`, `INPAINT_LoadInpaintModel`, `ModelPatchLoader`, `LoraLoader`, ...). Architectures come from `/api/etn/model_info/{checkpoints,diffusion_models,unet_gguf}`. Matching is by filename substring, and files in a `krita` subfolder win ties.
