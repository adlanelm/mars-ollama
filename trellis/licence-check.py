"""Build-time and post-build proof that the TRELLIS.2 image carries no non-commercial package (docs/licenses.md).
Fails (exit 1) if nvdiffrast, nvdiffrec or MeshLib is installed or present as a file anywhere in the image, or if
the licence stubs don't raise. Run: docker run --rm --entrypoint python <image> /opt/licence-check.py
"""
import os, pathlib, site, subprocess, sys

NC = ("nvdiffrast", "nvdiffrec", "meshlib", "mrmesh")
STUBS = "/opt/licence-stubs"
errors = []

freeze = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True, check=True).stdout
for line in freeze.lower().splitlines():
    if line.split("==")[0].split(" @")[0] in ("nvdiffrast", "nvdiffrec-render", "nvdiffrec_render", "meshlib", "meshlib-core"):
        errors.append(f"pip: {line}")

for root, dirs, files in os.walk("/", topdown=True):
    dirs[:] = [d for d in dirs if os.path.join(root, d) not in ("/proc", "/sys", "/dev", STUBS, "/cache")]
    for name in dirs + files:
        n = name.lower()
        if any(k in n for k in NC) and "pymeshlab" not in n:
            errors.append(f"file: {os.path.join(root, name)}")

sys.path.insert(0, STUBS)
import _licence_stub
import nvdiffrast.torch as dr
import meshlib.mrmeshpy as mrmeshpy
for mod, attr in ((dr, "RasterizeGLContext"), (mrmeshpy, "fillHole")):
    try:
        getattr(mod, attr)
        errors.append(f"stub {mod.__name__}.{attr} did not raise")
    except _licence_stub.LicenceRemovedError:
        pass
    if not mod.__file__.startswith(STUBS):
        errors.append(f"{mod.__name__} resolves to {mod.__file__}, not the stub")

if errors:
    print("licence-check FAILED:\n  " + "\n  ".join(errors[:50]))
    sys.exit(1)
print("licence-check ok: no nvdiffrast / nvdiffrec / MeshLib in the image; stubs raise LicenceRemovedError")
