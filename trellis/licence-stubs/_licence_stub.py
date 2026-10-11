"""Stand-ins for non-commercial libraries removed from the TRELLIS.2 image (see docs/licenses.md).

The Aero-Ex node pack and o_voxel import nvdiffrast and MeshLib at module load, even though the clean workflow
(bake_on_vertices, fill_holes off, Cumesh simplify) never calls them. These modules let the imports succeed and make
any real use fail loudly instead of silently producing an asset that a non-commercial licence touched.
"""


class LicenceRemovedError(RuntimeError):
    pass


def make_getattr(module, what):
    def __getattr__(name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)  # keep import machinery, pickle, inspect etc. working
        raise LicenceRemovedError(
            f"{module}.{name}: {what} was removed from this image for licence reasons (non-commercial), "
            "see docs/licenses.md in mars-ollama. Use bake_on_vertices=true, fill_holes=false, simplify_method=Cumesh."
        )
    return __getattr__
