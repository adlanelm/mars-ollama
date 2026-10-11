| model                          |       size |     params | backend    | ngl |  n_cpu_moe | threads | n_ubatch | type_k | type_v |  fa |            test |                  t/s |
| ------------------------------ | ---------: | ---------: | ---------- | --: | ---------: | ------: | -------: | -----: | -----: | --: | --------------: | -------------------: |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |        480.04 ± 4.43 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         32.13 ± 0.20 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |       461.81 ± 13.68 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         31.63 ± 0.13 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |        478.13 ± 2.28 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         31.82 ± 0.14 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |        734.65 ± 0.32 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         31.73 ± 0.40 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |        750.64 ± 8.39 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         31.53 ± 0.03 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |        747.89 ± 3.35 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         32.05 ± 0.44 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |      1031.09 ± 13.74 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       6 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         31.90 ± 0.34 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |       1043.01 ± 8.28 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |       8 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         31.75 ± 0.13 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |      1036.10 ± 13.33 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         20 |      12 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         31.89 ± 0.26 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |       193.32 ± 64.26 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         32.28 ± 0.29 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |       405.59 ± 32.07 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         32.19 ± 0.36 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |      512 |   q8_0 |   q8_0 |   1 |          pp2048 |       451.62 ± 22.77 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |      512 |   q8_0 |   q8_0 |   1 |           tg128 |         32.39 ± 0.19 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |       587.95 ± 12.84 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         33.89 ± 0.28 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |       692.21 ± 19.20 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         32.57 ± 0.78 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |     1024 |   q8_0 |   q8_0 |   1 |          pp2048 |       667.72 ± 55.54 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |     1024 |   q8_0 |   q8_0 |   1 |           tg128 |         33.19 ± 0.20 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |       962.12 ± 13.84 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       6 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         32.55 ± 0.02 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |       928.34 ± 25.15 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |       8 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         32.50 ± 0.24 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |     2048 |   q8_0 |   q8_0 |   1 |          pp2048 |       927.19 ± 16.49 |
| qwen35moe 35B.A3B Q4_K - Medium |  20.81 GiB |    34.66 B | Vulkan     |  99 |         18 |      12 |     2048 |   q8_0 |   q8_0 |   1 |           tg128 |         32.79 ± 0.05 |

build: f2918cabb (11541)
