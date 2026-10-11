# Compose patch for colibri (not applied)

Add two volume lines to the `llama-swap` service in `docker-compose.yaml`, next to the existing ones:

```yaml
    volumes:
      - $HOME/llm/models:/models:ro,Z
      - $HOME/llm/llama-cache:/cache:Z
      - ./llama-swap:/config:ro,Z
      - $HOME/llm/colibri/bin:/colibri:ro,Z             # NEW: colibri source tree + qwen38 engine + shaders
      - $HOME/llm/colibri/models:/colibri-models:Z      # NEW: read-write, colibri writes .coli_kv/.coli_usage/.coli_ssd
```

Notes:
- The task brief gave `~/llm/colibri/bin:/colibri:ro` and `~/llm/colibri/models:/colibri-models`. The `Z` flag (SELinux relabel) matches the other mounts. Drop it if the host has no SELinux. Compose does not expand `~`, so use `$HOME`.
- `/colibri/c/coli` is the launcher, `/colibri/c/qwen38` the engine, `/colibri/c/shaders/` the SPIR-V (found beside the binary). `ldd` and `coli --help` were verified inside the llama-swap image.
- `mem_limit: 28g` already applies to the service. With `--ram 18` plus the aux models that fits, but page cache for streamed experts is charged to the cgroup, so watch `docker stats` and `swapon --show` on the first run.
- Also add `colibri-big` to `routing.router.settings.groups.chat.members` in `llama-swap/config.yaml` (it is in `llama-swap/colibri.snippet.yaml`). Without it, loading `colibri-big` would not unload the chat model.
- Applying needs `docker compose up -d llama-swap` (recreate for new volumes), not just `restart`.
