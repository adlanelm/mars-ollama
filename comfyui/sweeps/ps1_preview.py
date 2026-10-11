#!/usr/bin/env python3
"""ps1_preview: PS1-style preview for every <name>.png under a directory -> <name>_ps1.png next to it.
Box-downscale to 256x256, 4x4 ordered (Bayer) dither to 15-bit RGB555 like the PS1 GPU, quantize to 64 colours
(no further dither), upscale x2 nearest. Needs Pillow; run without host installs:
  uvx --with pillow python comfyui/sweeps/ps1_preview.py ~/llm/comfyui/output/krea-sweep [--size 256 --colors 64 --scale 2]"""
import argparse, pathlib
from PIL import Image

BAYER = [[0, 8, 2, 10], [12, 4, 14, 6], [3, 11, 1, 9], [15, 7, 13, 5]]


def ps1(src, size, colors, scale):
    im = Image.open(src).convert("RGB").resize((size, size), Image.BOX)
    px = im.load()
    for y in range(size):
        for x in range(size):
            off = BAYER[y & 3][x & 3] / 2  # 0..7.5 before truncating to RGB555 steps of 8
            px[x, y] = tuple(min(255, max(0, int(c + off))) & 0xF8 for c in px[x, y])
    im = im.quantize(colors=colors, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).convert("RGB")
    return im.resize((size * scale, size * scale), Image.NEAREST)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("dir"); ap.add_argument("--size", type=int, default=256)
    ap.add_argument("--colors", type=int, default=64); ap.add_argument("--scale", type=int, default=2)
    ap.add_argument("--force", action="store_true"); a = ap.parse_args(); n = 0
    for p in sorted(pathlib.Path(a.dir).expanduser().rglob("*.png")):
        if p.stem.endswith("_ps1") or p.parent.name == "warmup": continue
        out = p.with_name(p.stem + "_ps1.png")
        if out.exists() and not a.force: continue
        ps1(p, a.size, a.colors, a.scale).save(out); n += 1
    print(f"{n} previews written")


if __name__ == "__main__":
    main()
