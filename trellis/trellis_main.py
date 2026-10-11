"""ComfyUI launcher for the TRELLIS.2 image: create an nvdiffrast GL (EGL surfaceless) context BEFORE ComfyUI starts.

Without this, the first RasterizeGLContext() created inside ComfyUI's prompt worker segfaults (texture bake step),
while the same call in a plain script works: toastmanAu/trellis-2-rocm-comfyui issue #5. A context created early
keeps later ones working. Root cause unknown upstream.
"""
import os
import runpy
import sys

os.environ.setdefault("EGL_PLATFORM", "surfaceless")
try:
    import nvdiffrast.torch as _dr
    _EARLY_GLCTX = _dr.RasterizeGLContext()  # keep a reference for the life of the process
    print("[trellis] early GL context created", flush=True)
except Exception as e:  # ComfyUI still starts; texture baking will fail later
    print(f"[trellis] early GL context failed: {e}", flush=True)

sys.argv[0] = "main.py"
runpy.run_path("main.py", run_name="__main__")
