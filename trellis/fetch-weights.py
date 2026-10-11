"""Fetch TRELLIS.2 GGUF weights at pinned HF revisions into /models/Trellis2 (= ~/llm/trellis/models/Trellis2).
Same layout Aero-Ex's model_manager expects, so it finds everything and downloads nothing at run time.
Also fetches the rembg BiRefNet ONNX (pinned url + sha256) into /cache/u2net (= ~/llm/trellis/cache/u2net, U2NET_HOME), so the
node never downloads at run time: rembg uses an existing file as is, without any checksum check.
Run (no host installs):
  docker run --rm -v ~/llm/trellis/models:/models -v ~/llm/trellis/cache:/cache -v $PWD/trellis:/w:ro --entrypoint python \
    comfyui-rocm:0.39.2 /w/fetch-weights.py [QUANT]
QUANT defaults to Q8_0 (~11 GB incl. DINOv3; Q4_K_M ~6 GB). The node verifies the 1024 models too, so they are included.
"""
import hashlib, os, sys, urllib.request
from huggingface_hub import hf_hub_download

GGUF_REPO, GGUF_REV = "Aero-Ex/Trellis2-GGUF", "edf81aef93af0823ac8721da3c9cd3c52d539f87"
DINO_REPO, DINO_REV = "Aero-Ex/Dinov3", "33aa61361f167fadf08fa88f312717ba5f482c42"
# rembg 2.0.85 rembg/sessions/birefnet_general.py: same URL (its pooch md5 7a35a0141cbbc80de11d9c9a28f52697), MIT code + weights
BIREFNET_URL = "https://github.com/danielgatis/rembg/releases/download/v0.0.0/BiRefNet-general-epoch_244.onnx"
BIREFNET_SHA256 = "58f621f00f5d756097615970a88a791584600dcf7c45b18a0a6267535a1ebd3c"
BIREFNET_DEST = "/cache/u2net/models/birefnet-general/birefnet-general.onnx"  # layout rembg looks up (U2NET_HOME/models/<name>/<name>.onnx)
q = sys.argv[1] if len(sys.argv) > 1 else "Q8_0"
root = "/models/Trellis2"

files = ["pipeline.json", "texturing_pipeline.json"]
for f in ["decoders/Stage1/ss_dec_conv3d_16l8_fp16", "decoders/Stage2/shape_dec_next_dc_f16c32_fp16",
          "decoders/Stage2/tex_dec_next_dc_f16c32_fp16", "encoders/shape_enc_next_dc_f16c32_fp16"]:
    files += [f + ".json", f + ".safetensors"]
for f in ["refiner/ss_flow_img_dit_1_3B_64_bf16",
          "shape/slat_flow_img2shape_dit_1_3B_512_bf16", "shape/slat_flow_img2shape_dit_1_3B_1024_bf16",
          "texture/slat_flow_imgshape2tex_dit_1_3B_512_bf16", "texture/slat_flow_imgshape2tex_dit_1_3B_1024_bf16"]:
    files += [f + ".json", f"{f}_{q}.gguf"]
for f in files:
    print(hf_hub_download(GGUF_REPO, f, revision=GGUF_REV, local_dir=root), flush=True)
for f in ["config.json", "model.safetensors", "preprocessor_config.json"]:
    print(hf_hub_download(DINO_REPO, f"facebook/dinov3-vitl16-pretrain-lvd1689m/{f}", revision=DINO_REV,
                          local_dir=f"{root}/dinov3"), flush=True)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 24), b""):
            h.update(b)
    return h.hexdigest()


if not (os.path.exists(BIREFNET_DEST) and sha256(BIREFNET_DEST) == BIREFNET_SHA256):
    os.makedirs(os.path.dirname(BIREFNET_DEST), exist_ok=True)
    part = BIREFNET_DEST + ".part"
    urllib.request.urlretrieve(BIREFNET_URL, part)
    if sha256(part) != BIREFNET_SHA256:
        os.remove(part)
        sys.exit(f"sha256 mismatch for {BIREFNET_URL}")
    os.replace(part, BIREFNET_DEST)
print(BIREFNET_DEST, flush=True)
