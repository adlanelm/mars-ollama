"""Fetch TRELLIS.2 GGUF weights at pinned HF revisions into /models/Trellis2 (= ~/llm/trellis/models/Trellis2).
Same layout Aero-Ex's model_manager expects, so it finds everything and downloads nothing at run time.
Run (no host installs):
  docker run --rm -v ~/llm/trellis/models:/models -v $PWD/trellis:/w:ro --entrypoint python \
    comfyui-rocm:0.39.0 /w/fetch-weights.py [QUANT]
QUANT defaults to Q8_0 (~11 GB incl. DINOv3; Q4_K_M ~6 GB). The node verifies the 1024 models too, so they are included.
"""
import sys
from huggingface_hub import hf_hub_download

GGUF_REPO, GGUF_REV = "Aero-Ex/Trellis2-GGUF", "edf81aef93af0823ac8721da3c9cd3c52d539f87"
DINO_REPO, DINO_REV = "Aero-Ex/Dinov3", "33aa61361f167fadf08fa88f312717ba5f482c42"
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
