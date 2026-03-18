# Staging & Release Guide

## Architecture

| Component | Staging | Production |
|-----------|---------|------------|
| **R2 Worker** | `bc-fishing-r2-staging` | `bc-fishing-r2` |
| **R2 Bucket** | `bc-fishing-regulations-staging` | `bc-fishing-regulations` |
| **Branch** | `staging` | `main` |
| **In-Season Cron** | GHA on `staging` → staging bucket | GHA on `main` → prod bucket |
| **Worker Deploy** | Cloudflare git integration → `staging` branch | Cloudflare git integration → `main` branch |

Both environments use identical code — the only difference is the R2 bucket binding in `r2-worker/wrangler.toml` (`[env.staging]` vs `[env.production]`).

## One-Time Setup

### 1. Create staging R2 bucket

```sh
wrangler r2 bucket create bc-fishing-regulations-staging
```

### 2. Deploy staging worker

The `[env.staging]` section is already in `r2-worker/wrangler.toml`.

In Cloudflare dashboard → Workers & Pages → Create → Connect to Git:
- Repository: this repo
- Production branch: `staging`
- Build command: `cd r2-worker && npm install`
- Deploy command: `wrangler deploy --env staging`

This gives you `https://bc-fishing-r2-staging.<account>.workers.dev`.
Every push to `staging` auto-deploys the worker.

### 3. Seed staging R2 with pipeline data

```sh
./scripts/seed-r2.sh    # defaults to DEPLOY_ENV=staging
```

### 4. Add GitHub secrets

| Secret | Purpose |
|--------|---------|
| `CLOUDFLARE_ACCOUNT_ID` | Wrangler auth for R2 uploads |
| `CLOUDFLARE_API_TOKEN` | Wrangler auth (needs R2 write + Workers read) |

### 5. Preview frontend against staging

```sh
cd webapp
VITE_TILE_BASE_URL=https://bc-fishing-r2-staging.<account>.workers.dev npm run build
npx vite preview
```

## Daily In-Season Updates

The `update-in-season.yml` workflow runs daily at 6 AM PST on whichever branch it's configured for. It:

1. Fetches `tier0.json` + `match_table.json` from the environment's R2 worker
2. Scrapes BC gov in-season changes page
3. Resolves water names → reach IDs (Python pipeline)
4. Uploads `in_season.json` to the environment's R2 bucket

Branch determines environment automatically:
- `staging` branch → `DEPLOY_ENV=staging` → staging bucket
- `main` branch → `DEPLOY_ENV=production` → prod bucket

### Local testing (same code path):

```sh
./scripts/update-in-season.sh              # scrape + resolve only
./scripts/update-in-season.sh --seed        # + refresh local R2
DEPLOY_ENV=staging ./scripts/update-in-season.sh --upload  # upload to staging R2
```

## Promoting Staging → Production

Once staging is verified:

```sh
# 1. Merge staging → main
git checkout main
git merge staging
git push origin main

# 2. Cloudflare auto-deploys production worker via git integration

# 3. Seed production R2 (if pipeline data changed)
DEPLOY_ENV=production ./scripts/seed-r2.sh

# 4. Frontend auto-deploys via Pages on main push
```

## Rollback

- **Worker**: `wrangler rollback` reverts to previous worker version
- **Data**: Old shards remain in R2 (keyed by `SHARD_VERSION`). Revert the var in wrangler.toml and redeploy.
- **Frontend**: Revert git commit on main, Pages auto-redeploys.
- **In-season**: Re-run workflow manually, or upload a known-good `in_season.json`.

## Key Config

| Var | Where | Purpose |
|-----|-------|---------|
| `VITE_TILE_BASE_URL` | Frontend build | Points at worker URL (empty = same origin) |
| `SHARD_VERSION` | `r2-worker/wrangler.toml` | Which shard prefix to read (`v8`) |
| `DEPLOY_ENV` | `update-in-season.sh` / GHA | `staging` or `production` — controls bucket + origin |
