"""Print mesh stats of a .glb and render a 4-view unlit preview PNG (CPU painter's algorithm: numpy + PIL, no GPU).
  docker run --rm -v ~/llm/trellis/out:/out -v $PWD/trellis:/w:ro --entrypoint python \
    comfyui-trellis2-rocm:0.39.0-b3b9e95-clean /w/inspect_glb.py /out/trellis2/prop-terminal_00005_.glb
Writes <file>.preview.png next to the input. Colour per face: mean vertex colour, or the UV texture sampled at the face
centroid. Licence-clean: the former nvdiffrast GL renderer is gone (docs/licenses.md).
"""
import math, sys
import numpy as np, trimesh
from PIL import Image, ImageDraw

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
vc = m.visual.vertex_colors if m.visual.kind == "vertex" else None
print(f"uv={'yes' if uv is not None else 'no'} vertex_colors={'yes' if vc is not None else 'no'}")

faces = np.asarray(m.faces)
if vc is not None:
    fcol = np.asarray(vc)[faces, :3].mean(1)
elif tex is not None and uv is not None:
    t = np.asarray(tex.convert("RGB"))
    c = np.asarray(uv)[faces].mean(1)
    x = np.clip((c[:, 0] % 1) * (t.shape[1] - 1), 0, t.shape[1] - 1).astype(int)
    y = np.clip((1 - c[:, 1] % 1) * (t.shape[0] - 1), 0, t.shape[0] - 1).astype(int)
    fcol = t[y, x].astype(float)
else:
    fcol = np.full((len(faces), 3), 178.0)
fcol = fcol.astype(np.uint8)

v = m.vertices - m.bounds.mean(0)
v = v / np.abs(v).max()
R = 512

def view(az, el):
    a, e = math.radians(az), math.radians(el)
    ry = np.array([[math.cos(a), 0, math.sin(a)], [0, 1, 0], [-math.sin(a), 0, math.cos(a)]])
    rx = np.array([[1, 0, 0], [0, math.cos(e), -math.sin(e)], [0, math.sin(e), math.cos(e)]])
    p = v @ (rx @ ry).T
    xy = np.stack([(p[:, 0] * 0.75 + 1) * R / 2, (1 - p[:, 1] * 0.75) * R / 2], 1)
    tri = xy[faces]
    order = np.argsort(p[faces, 2].mean(1))  # far to near: larger z is nearer, as in the old nvdiffrast view
    im = Image.new("RGB", (R, R), (217, 217, 217))
    d = ImageDraw.Draw(im)
    for i in order:
        d.polygon([tuple(q) for q in tri[i]], fill=tuple(int(c) for c in fcol[i]))
    return np.asarray(im)

tiles = [view(az, el) for az, el in [(45, 25), (135, 25), (225, 25), (315, 25)]]
out = path.rsplit(".", 1)[0] + ".preview.png"
Image.fromarray(np.concatenate(tiles, 1)).save(out)
print("preview:", out)
