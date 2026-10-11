"""Print mesh stats of a .glb and render a 4-view textured preview PNG (nvdiffrast GL, inside the trellis image).
  docker run --rm --device /dev/kfd --device /dev/dri --group-add 983 --group-add 987 \
    -v ~/llm/trellis/out:/out -v $PWD/trellis:/w:ro --entrypoint python \
    comfyui-trellis2-rocm:0.39.0-b3b9e95 /w/inspect_glb.py /out/trellis2/prop-terminal_00001_.glb
Writes <file>.preview.png next to the input. Run it while comfyui-trellis is stopped (one GPU user at a time).
"""
import math, sys
import numpy as np, torch, trimesh
from PIL import Image
import nvdiffrast.torch as dr

path = sys.argv[1]
scene = trimesh.load(path)
meshes = list(scene.geometry.values()) if isinstance(scene, trimesh.Scene) else [scene]
m = meshes[0] if len(meshes) == 1 else trimesh.util.concatenate(meshes)
print(f"geometries={len(meshes)} vertices={len(m.vertices)} triangles={len(m.faces)} "
      f"watertight={m.is_watertight} extents={np.round(m.extents, 3).tolist()}")
mat = getattr(m.visual, "material", None)
tex = None
for attr in ("baseColorTexture", "image"):
    img = getattr(mat, attr, None) if mat is not None else None
    if img is not None:
        tex = img; print(f"material={type(mat).__name__} {attr}={img.size} mode={img.mode}")
        for other in ("metallicRoughnessTexture", "normalTexture", "emissiveTexture"):
            o = getattr(mat, other, None)
            if o is not None: print(f"  {other}={o.size}")
        break
uv = getattr(m.visual, "uv", None)
print(f"uv={'yes' if uv is not None else 'no'}")

dev = "cuda"
v = torch.tensor(m.vertices - m.bounds.mean(0), dtype=torch.float32, device=dev)
v = v / v.abs().max()
f = torch.tensor(m.faces, dtype=torch.int32, device=dev)
ctx = dr.RasterizeGLContext()
R = 512

def view(az, el):
    a, e = math.radians(az), math.radians(el)
    ry = torch.tensor([[math.cos(a), 0, math.sin(a)], [0, 1, 0], [-math.sin(a), 0, math.cos(a)]])
    rx = torch.tensor([[1, 0, 0], [0, math.cos(e), -math.sin(e)], [0, math.sin(e), math.cos(e)]])
    p = v @ (rx @ ry).T.to(dev)
    clip = torch.cat([p[:, :2] * 0.75, (p[:, 2:3] * -0.5), torch.ones_like(p[:, :1])], 1)[None]
    rast, _ = dr.rasterize(ctx, clip.contiguous(), f, resolution=[R, R])
    mask = rast[..., 3:4] > 0
    if tex is not None and uv is not None:
        t = torch.tensor(np.asarray(tex.convert("RGB")), dtype=torch.float32, device=dev)[None] / 255
        uvt = torch.tensor(np.stack([uv[:, 0], 1 - uv[:, 1]], 1), dtype=torch.float32, device=dev)[None].contiguous()
        uvi, _ = dr.interpolate(uvt, rast, f)
        col = dr.texture(t.contiguous(), uvi, filter_mode="linear")
    else:
        col = torch.ones(1, R, R, 3, device=dev) * 0.7
    col = torch.where(mask, col, torch.full_like(col, 0.85))
    return (col[0].flip(0).clamp(0, 1).cpu().numpy() * 255).astype(np.uint8)

tiles = [view(az, el) for az, el in [(45, 25), (135, 25), (225, 25), (315, 25)]]
out = path.rsplit(".", 1)[0] + ".preview.png"
Image.fromarray(np.concatenate(tiles, 1)).save(out)
print("preview:", out)
