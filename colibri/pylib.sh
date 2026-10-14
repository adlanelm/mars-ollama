# Python packages colibri's server imports lazily for image input (c/tools/qwen38_image.py: Pillow + numpy).
# The llama-swap image has neither, nor pip; without them an image request drops the connection (502 EOF).
# Installed into /out/pylib (= ~/llm/colibri/bin/pylib, mounted at /colibri/pylib, on colibri-big's PYTHONPATH).
# Run inside the llama-swap image so the wheels match its Python. Called by build.sh; alone:
#   docker run --rm --entrypoint bash -v $PWD/colibri:/src:ro,Z -v ~/llm/colibri/bin:/out:Z \
#     ghcr.io/mostlygeek/llama-swap:v264-vulkan-b11580 -c 'bash /src/pylib.sh'
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq python3-pip >/dev/null 2>&1
rm -rf /out/pylib
pip3 install -q --break-system-packages --no-compile --only-binary=:all: --target /out/pylib \
  numpy==2.5.4 pillow==12.3.0
chown -R 1000:1000 /out/pylib
python3 -c 'import sys; sys.path.insert(0, "/out/pylib"); import numpy, PIL; print("pylib: numpy", numpy.__version__, "pillow", PIL.__version__)'
