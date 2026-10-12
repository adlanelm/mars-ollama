"""Workbench (software GL) views of a rigged .glb: skeleton over the mesh, and the mesh coloured by dominant bone.
    LIBGL_ALWAYS_SOFTWARE=1 blender -b --factory-startup -P skintokens/render_rig.py -- IN.glb OUT.png
(host Blender 5.2, Mesa llvmpipe = CPU only). Top row: x-ray mesh + bones as red octahedra (armatures are overlays and
do not render). Bottom row: each vertex coloured by its dominant bone. Columns: front, side, top, 3/4.
Prints rig stats as JSON on a line starting with RIGSTATS. Leaf-bone tails come from the glTF importer's heuristic,
so the long red spikes at limb ends are display artefacts, not predicted joints.
"""
import bpy, sys, os, json, colorsys, math
from mathutils import Vector

argv = sys.argv[sys.argv.index("--") + 1:]
src, out = argv[0], argv[1]
bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.gltf(filepath=src)
arms = [o for o in bpy.data.objects if o.type == "ARMATURE"]
meshes = [o for o in bpy.data.objects if o.type == "MESH"]
arm = arms[0] if arms else None

stats = {"file": os.path.basename(src), "armatures": len(arms), "meshes": len(meshes)}
if arm:
    bones = arm.data.bones
    def depth(b):
        d = 0
        while b.parent:
            b = b.parent; d += 1
        return d
    stats.update(bones=len(bones), roots=sum(1 for b in bones if not b.parent), max_depth=max(depth(b) for b in bones),
                 bone_names=[b.name for b in bones][:80])
nv = nt = 0; unweighted = 0; infl = []
bone_color = {}
if arm:
    for i, b in enumerate(arm.data.bones):
        h = (i * 0.618034) % 1.0
        bone_color[b.name] = colorsys.hsv_to_rgb(h, 0.75, 0.95)
for m in meshes:
    me = m.data
    nv += len(me.vertices); nt += sum(len(p.vertices) - 2 for p in me.polygons)
    names = {g.index: g.name for g in m.vertex_groups}
    col = me.color_attributes.new("dominant", "FLOAT_COLOR", "POINT")
    for v in me.vertices:
        gs = [(g.weight, names[g.group]) for g in v.groups if g.weight > 1e-4]
        infl.append(len(gs))
        if not gs:
            unweighted += 1; col.data[v.index].color = (0.2, 0.2, 0.2, 1); continue
        w, n = max(gs)
        c = bone_color.get(n, (1, 1, 1))
        col.data[v.index].color = (*c, 1)
    g = me.color_attributes.new("grey", "FLOAT_COLOR", "POINT")
    for c in g.data: c.color = (0.7, 0.7, 0.75, 1)
stats.update(vertices=nv, triangles=nt, unweighted_vertices=unweighted,
             max_influences=max(infl) if infl else 0, mean_influences=round(sum(infl) / max(len(infl), 1), 2))
if arm:
    arm.data.display_type = "OCTAHEDRAL"
    arm.show_in_front = True
    try:
        arm.data.show_bone_colors = False
    except AttributeError:
        pass

# extra meshes without vertex groups (SkinTokens' glTF_not_exported helper) are hidden and ignored
skinned = [m for m in meshes if m.vertex_groups]
for m in meshes:
    if m not in skinned:
        m.hide_render = True
stats["skinned_meshes"] = len(skinned)
# bones as render geometry (armatures are overlays and do not appear in renders): thin octahedra, red
bone_obj = None
if arm:
    import bmesh
    bm = bmesh.new()
    mw = arm.matrix_world
    for b in arm.data.bones:
        h, t = mw @ b.head_local, mw @ b.tail_local
        d = t - h; L = d.length
        if L < 1e-6: continue
        r = L * 0.08
        a = d.normalized(); ref = Vector((0, 0, 1)) if abs(a.z) < 0.9 else Vector((1, 0, 0))
        u = a.cross(ref).normalized() * r; w = a.cross(u).normalized() * r
        m0 = h + d * 0.2
        vs = [bm.verts.new(p) for p in (h, m0 + u, m0 + w, m0 - u, m0 - w, t)]
        for i in range(4):
            bm.faces.new((vs[0], vs[1 + i], vs[1 + (i + 1) % 4])); bm.faces.new((vs[5], vs[1 + (i + 1) % 4], vs[1 + i]))
    me_b = bpy.data.meshes.new("bones"); bm.to_mesh(me_b); bm.free()
    bone_obj = bpy.data.objects.new("bones", me_b); sc0 = bpy.context.scene; sc0.collection.objects.link(bone_obj)
    ca = me_b.color_attributes.new("dominant", "FLOAT_COLOR", "POINT")
    for c in ca.data: c.color = (1, 0.1, 0.05, 1)
    arm.hide_render = True
meshes = skinned
# bounds
pts = [m.matrix_world @ Vector(c) for m in meshes for c in m.bound_box]
lo = Vector((min(p.x for p in pts), min(p.y for p in pts), min(p.z for p in pts)))
hi = Vector((max(p.x for p in pts), max(p.y for p in pts), max(p.z for p in pts)))
ctr = (lo + hi) / 2; size = max(hi - lo)
stats["bbox_m"] = [round(v, 3) for v in (hi - lo)]

sc = bpy.context.scene
sc.render.engine = "BLENDER_WORKBENCH"
sc.render.resolution_x = sc.render.resolution_y = 640
sh = sc.display.shading
sh.light = "STUDIO"
sc.world = bpy.data.worlds.new("w") if not sc.world else sc.world
cam_d = bpy.data.cameras.new("cam"); cam_d.type = "ORTHO"; cam_d.ortho_scale = size * 1.15
cam = bpy.data.objects.new("cam", cam_d); sc.collection.objects.link(cam); sc.camera = cam
views = {"front": (0, -1, 0), "side": (1, 0, 0), "top": (0, -0.001, 1), "persp": (0.8, -1, 0.6)}
tiles = []
tmp = os.path.dirname(os.path.abspath(out))
for mode in ("skeleton", "weights"):
    if mode == "skeleton":
        sh.color_type = "VERTEX"; sh.show_xray = True; sh.xray_alpha = 0.45
        for m in meshes:
            m.data.color_attributes.active_color = m.data.color_attributes["grey"]
        if bone_obj: bone_obj.hide_render = False
    else:
        sh.color_type = "VERTEX"; sh.show_xray = False
        for m in meshes:
            m.data.color_attributes.active_color = m.data.color_attributes["dominant"]
        if bone_obj: bone_obj.hide_render = True
    for vn, d in views.items():
        dv = Vector(d).normalized()
        cam.location = ctr + dv * size * 3
        cam.rotation_euler = (-dv).to_track_quat("-Z", "Y").to_euler()
        p = os.path.join(tmp, f"_{mode}_{vn}.png")
        sc.render.filepath = p
        bpy.ops.render.render(write_still=True)
        tiles.append(p)

# compose 4x2 grid
imgs = [bpy.data.images.load(p) for p in tiles]
W = H = 640
grid = bpy.data.images.new("grid", W * 4, H * 2, alpha=False)
px = [0.0] * (W * 4 * H * 2 * 4)
for k, im in enumerate(imgs):
    col, row = k % 4, 1 - k // 4
    src_px = list(im.pixels)
    for y in range(H):
        o = ((row * H + y) * W * 4 + col * W) * 4
        px[o:o + W * 4] = src_px[y * W * 4:(y + 1) * W * 4]
grid.pixels = px
grid.filepath_raw = out; grid.file_format = "PNG"; grid.save()
for p in tiles:
    os.remove(p)
print("RIGSTATS " + json.dumps(stats))
