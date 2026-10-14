# Build colibri v2.0.0 (qwen38 engine, Vulkan) inside the llama-swap image, so glibc/libvulkan match the runtime.
# Run from the repo root (nothing is installed on the host):
#   docker run --rm --entrypoint bash -v $PWD/colibri:/src:ro,Z -v ~/llm/colibri/bin:/out:Z \
#     ghcr.io/mostlygeek/llama-swap:v263-vulkan-b11541 -c 'mkdir -p /work; bash /src/build.sh'
# Local patches in colibri/patches/*.patch are applied before the build (see colibri/patches/README.md).
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq git build-essential libvulkan-dev glslc libomp-dev ca-certificates >/dev/null 2>&1
git clone -q --branch v2.0.0 https://github.com/JustVugg/colibri /work/colibri 2>/dev/null
cd /work/colibri; git rev-parse HEAD; git describe --tags
for p in /src/patches/*.patch; do [ -e "$p" ] || continue; echo "applying $p"; git apply -p1 "$p"; done
cd c
make qwen38 VK=1 -j8 2>&1 | tail -15
ls -la qwen38 shaders | head -50
ldd qwen38
# Keep the model's runtime files (.coli_*) out of /out: they live in the models dir, not here.
rm -rf /out/*; mkdir -p /out
cd /work/colibri; rm -rf .git
cp -a /work/colibri/. /out/
chown -R 1000:1000 /out
bash /src/pylib.sh # Pillow + numpy for image input
