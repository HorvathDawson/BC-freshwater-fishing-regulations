# Scripts

## dev.mjs

Launches the full local dev stack in one command:

1. Seeds local R2 storage from pipeline deploy output
2. Starts the R2 Worker via `wrangler dev` (port 8787)
3. Starts Vite dev server (port 5173) with proxy to the worker

```sh
node scripts/dev.mjs
```

## seed.mjs

Populates the local Miniflare R2 bucket with files from `output/pipeline/deploy/` so `wrangler dev` can serve them identically to production. Uses content hashing to skip re-seeding when nothing has changed.

```sh
node scripts/seed.mjs          # incremental (skips if unchanged)
node scripts/seed.mjs --force  # always re-seed
```
