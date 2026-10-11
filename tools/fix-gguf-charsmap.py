"""Rewrite tokenizer.ggml.precompiled_charsmap from Ollama's base64 char-array form to a UINT8 array.

Ollama's converter stored snowflake-arctic-embed2's charsmap as an array of 1-char strings (base64 text);
llama.cpp requires an INT8/UINT8 array. Tensors and all other metadata are copied unchanged.

Exit 10 (nothing written) when the key is absent or already numeric.

Usage: uv run --with gguf==0.19.0 --python 3.12 python -I tools/fix-gguf-charsmap.py <in.gguf> <out.gguf>
"""
import base64, sys
import numpy as np
from gguf import GGUFReader, GGUFWriter, GGUFValueType

KEY = "tokenizer.ggml.precompiled_charsmap"
src, dst = sys.argv[1], sys.argv[2]
r = GGUFReader(src)
f0 = r.fields.get(KEY)
if f0 is None or f0.types[-1] != GGUFValueType.STRING:
    print("no fix needed"); sys.exit(10)  # 10 = nothing written
arch = bytes(r.fields["general.architecture"].parts[-1]).decode()
w = GGUFWriter(dst, arch=arch, endianess=r.endianess)
w.data_alignment = r.alignment if hasattr(r, "alignment") else w.data_alignment

for k, f in r.fields.items():
    if k.startswith("GGUF.") or k == "general.architecture":
        continue
    if k == KEY:
        if f.types[-1] == GGUFValueType.STRING:
            b64 = b"".join(bytes(f.parts[i]) for i in f.data)
            raw = base64.b64decode(b64)
        else:
            raw = bytes(np.concatenate([f.parts[i] for i in f.data]).astype(np.uint8))
        w.add_key_value(KEY, list(raw), GGUFValueType.ARRAY, sub_type=GGUFValueType.UINT8)
        print(f"{KEY}: {len(f.data)} elems -> {len(raw)} bytes")
        continue
    t = f.types[0]
    if t == GGUFValueType.ARRAY:
        sub = f.types[-1]
        if sub == GGUFValueType.STRING:
            w.add_key_value(k, [bytes(f.parts[i]).decode("utf-8", "surrogateescape") for i in f.data], t, sub_type=sub)
        else:
            w.add_key_value(k, [f.parts[i].tolist()[0] for i in f.data], t, sub_type=sub)
    elif t == GGUFValueType.STRING:
        w.add_key_value(k, bytes(f.parts[-1]).decode("utf-8", "surrogateescape"), t)
    else:
        w.add_key_value(k, f.parts[-1].tolist()[0], t)

for t in r.tensors:
    w.add_tensor_info(t.name, t.data.shape, t.data.dtype, t.data.nbytes, t.tensor_type)
w.write_header_to_file(); w.write_kv_data_to_file(); w.write_ti_data_to_file()
for t in r.tensors:
    w.write_tensor_data(t.data)
w.close()
print("wrote", dst)
