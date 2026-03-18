# Staging & Release Guide

## Architecture

Three pieces to deploy:

| Component | Host | Config |
|-----------|------|--------|
| **R2 Worker** | Cloudflare Workers | `r2-worker/wrangler.toml` |
| **R2 Data** | Cloudflare R2 bucket | `output/pipeline/deploy/` |
| **Frontend** | Cloudflare Pages (or static host) | `webapp/` |

## One-Time Staging Setup

### 1. Create staging R2 bucket

```sh
wrangler r2 bucket create bc-fishing-regulations-staging
```

### 2. Add staging environment to `r2-worker/wrangler.toml`

```toml
[env.staging]
name = "bc-fishing-r2-staging"

[[env.staging.r2_buckets]]
binding = "BUCKET"
bucket_name = "bc-fishing-regulations-staging"

[env.staging.vars]
SHARD_VERSION = "v8"
```

### 3. Deploy staging worker

```sh
cd r2-worker
npm install
wrangler deploy --env staging
```

This gives you a URL like `https://bc-fishing-r2-staging.<account>.workers.dev`.

### 4. Seed staging R2

```sh
node scripts/seed.mjs --env staging
```

> Note: `seed.mjs` needs a `--env` flag added to target the staging bucket.
> Alternatively, upload directly:
> ```sh
> cd output/pipeline/deploy
> for f in $(find . -type f); do
>   wrangler r2 object put "bc-fishing-regulations-staging/${f#./}" --file "$f"
> done
> ```

### 5. Build & preview frontend against staging worker

```sh
cd webapp
VITE_TILE_BASE_URL=https://bc-fishing-r2-staging.<account>.workers.dev npm run build
npx vite preview
```

The frontend reads `VITE_TILE_BASE_URL` to set both data and API origins. In dev mode, Vite proxies `/data/*` → worker, but in production builds this env var points directly at the worker URL.

## Release to Production

Once staging is verified:

```sh
# 1. Deploy worker
cd r2-worker
wrangler deploy

# 2. Upload data to prod R2
cd output/pipeline/deploy
wrangler r2 object put bc-fishing-regulations/<key> --file <file>
# or use deploy-data.sh / seed.mjs against prod

# 3. Deploy frontend
cd webapp
npm run build
# push to Pages (git push main, or wrangler pages deploy dist/)
```

## Rollback

- **Worker**: `wrangler rollback` reverts to previous worker version
- **Data**: Old shards remain in R2 (keyed by shard_version). Revert `SHARD_VERSION` in wrangler.toml and redeploy worker.
- **Frontend**: Revert git commit on main, Pages auto-redeploys.

## Key Env Vars

| Var | Where | Purpose |
|-----|-------|---------|
| `VITE_TILE_BASE_URL` | Frontend build | Points at worker URL (empty = same origin) |
| `SHARD_VERSION` | Worker `wrangler.toml` | Which shard prefix to read (`v8`) |
