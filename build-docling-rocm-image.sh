#!/bin/bash
git clone --branch main git@github.com:docling-project/docling-serve.git >/dev/null
git pull
cd docling-serve
make docling-serve-rocm-image
