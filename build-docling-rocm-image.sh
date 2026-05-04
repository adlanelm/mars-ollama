#!/bin/bash
git clone --branch main git@github.com:docling-project/docling-serve.git >/dev/null
cd docling-serve
git pull
make docling-serve-rocm-image
