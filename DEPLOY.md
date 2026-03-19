# Deployment Quick Reference

How to deploy this project to staging and production. Read this if it's been 6 months.

---

## Architecture Overview

```
┌─────────────┐     ┌───────────────────┐     ┌──────────────┐
│  Pipeline    │────▶│  R2 Bucket        │◀────│  R2 Worker   │
│  (Python)    │     │  (data shards)    │     │  (API + CORS)│
└─────────────┘     └───────────────────┘     └──────┬───────┘
                                                      │
                    ┌───────────────────┐              │
                    │  Webapp Worker    │──── fetches ──┘
                    │  (SPA + static)   │
                    └───────────────────┘
```

Two Cloudflare Workers per environment, one R2 bucket per environment:

| | Staging | Production |
|---|---|---|
| **Branch** | `staging` | `main` |
| **R2 Bucket** | `bc-fishing-regulations-staging` | `bc-fishing-regulations` |
| **R2 Worker** | `bc-fishing-r2-staging` | `bc-fishing-r2` |
| **Webapp Worker** | `bc-fishing-regulations-staging` | `bc-fishing-regulations` |
| **R2 Data URL** | `data-staging.canifishthis.ca` | `data.canifishthis.ca` |
| **Frontend URL** | `staging.canifishthis.ca` | `canifishthis.ca` |

Workers auto-deploy via Cloudflare git integration when you push to the corresponding branch.

---

## Prerequisites

- **rclone** configured with an `r2` remote (S3-compatible, Cloudflare R2 credentials)
- **wrangler** installed globally (`npm i -g wrangler`) and authenticated
- **conda env `fish`** activated for pipeline runs
- GitHub secrets: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`

---

## Deploy to Staging

### 1. Run the pipeline (if data changed)

```bash
conda activate fish
python -m pipeline --step all          # full: atlas → tiles → enrich
python -m pipeline --step tiles enrich # skip atlas if only regs changed
pytest pipeline/tests/ -q             # verify tests pass
```

Output lands in `output/pipeline/deploy/`.

### 2. Upload data to staging R2

```bash
./scripts/seed-r2.sh                  # defaults to DEPLOY_ENV=staging
./scripts/seed-r2.sh --dry-run        # preview first
./scripts/seed-r2.sh --file output/pipeline/deploy/in_season.json  # single file
```

This uses rclone to sync `output/pipeline/deploy/` → `bc-fishing-regulations-staging` bucket. Checksum-based — only changed files upload.

### 3. Deploy workers (push to staging branch)

```bash
git checkout staging
git merge feature/your-branch
git push origin staging
```

Cloudflare auto-deploys both workers (R2 + Webapp) on push. No manual wrangler commands needed.

### 4. Build + deploy frontend

The webapp worker serves `webapp/dist/`. The Cloudflare git integration runs:
```
cd webapp && npm install && npm run build
```
This builds with `.env.staging` (Vite `--mode staging`), pointing `VITE_TILE_BASE_URL` at the staging R2 worker.

### 5. Verify staging

Visit `https://staging.canifishthis.ca` and test.

---

## Deploy to Production

Production is **different from staging** in these ways:

1. **Custom domain** — `canifishthis.ca` routes to the production webapp worker
2. **Prerender/SEO** — production build generates 19,000+ prerendered HTML pages + sitemap.xml
3. **Different R2 bucket** — `bc-fishing-regulations` (no `-staging` suffix)

### 1. Merge staging → main

```bash
git checkout main
git merge staging
git push origin main
```

Cloudflare auto-deploys both production workers on push to `main`.

### 2. Promote data: staging R2 → production R2

Instead of re-uploading from local, copy directly from the staging bucket:

```bash
./scripts/promote-r2.sh              # copy staging → production (additive)
./scripts/promote-r2.sh --dry-run    # preview what would change
./scripts/promote-r2.sh --clean      # sync with delete (removes stale prod files)
```

Or upload from local if you prefer:
```bash
DEPLOY_ENV=production ./scripts/seed-r2.sh
```

### 3. Verify production

Visit `https://canifishthis.ca` and test. Check:
- Search works (tier0.json loaded)
- PMTiles render (map tiles appear)
- Deep links work (`/waterbody/<wbg>/`)
- Prerendered pages: `curl -s https://canifishthis.ca/waterbody/<some-wbg>/ | grep '<title>'`

### 4. Rollback (if needed)

```bash
# Revert worker code:
cd r2-worker && wrangler rollback --env production

# Revert data — old shards remain in R2 keyed by SHARD_VERSION.
# Change SHARD_VERSION in r2-worker/wrangler.toml and redeploy.

# Revert frontend — revert the commit on main, auto-redeploys.
git revert HEAD && git push origin main
```

---

## Local Development

```bash
node scripts/dev.mjs
```

This does three things:
1. **Seeds local R2** — runs `scripts/seed.mjs` to populate Miniflare R2 from `output/pipeline/deploy/`
2. **Starts R2 worker** — `wrangler dev` at `http://localhost:8787`
3. **Starts Vite dev** — at `http://localhost:5173`, proxies `/data/*` and `/api/*` to the R2 worker

If you re-run the pipeline, re-seed:
```bash
node scripts/seed.mjs --force
```

---

## In-Season Updates (Automated)

GitHub Actions workflow (`.github/workflows/update-in-season.yml`) runs daily at 6 AM PST:
- Scrapes BC gov in-season regulation changes
- Resolves water names → reach IDs using `match_table.json` from R2
- Uploads `in_season.json` to the appropriate R2 bucket

Branch determines target:
- `staging` branch → staging bucket
- `main` branch → production bucket

Manual trigger: GitHub Actions → `update-in-season` → Run workflow.

Local testing:
```bash
./scripts/update-in-season.sh              # scrape + resolve only (no upload)
./scripts/update-in-season.sh --seed       # + refresh local R2
DEPLOY_ENV=staging ./scripts/update-in-season.sh --upload  # upload to staging
```

---

## Key Files

| File | Purpose |
|------|---------|
| `wrangler.toml` | Webapp worker config (SPA serving) |
| `r2-worker/wrangler.toml` | R2 data worker config (API + CORS) |
| `webapp/.env.staging` | Staging R2 worker URL |
| `webapp/.env.production` | Production R2 worker URL + prerender cap |
| `scripts/seed-r2.sh` | Upload deploy/ to R2 via rclone |
| `scripts/promote-r2.sh` | Copy staging R2 → production R2 |
| `scripts/seed.mjs` | Seed local Miniflare R2 for dev |
| `scripts/dev.mjs` | Start local dev environment |
| `STAGING.md` | Detailed staging setup (one-time steps) |

## SHARD_VERSION

Both environments use `SHARD_VERSION = "v1"` in `r2-worker/wrangler.toml`. This prefixes all shard keys in R2. To deploy a new data format:
1. Bump the version in `[env.staging.vars]` / `[env.production.vars]`
2. Re-run pipeline + re-seed R2
3. Old shards remain in the bucket (safe rollback point)

## Cleaning Up R2 Buckets

Old shard versions and stale files accumulate in R2. To clean:

```bash
# List what's in a bucket:
rclone ls r2:bc-fishing-regulations-staging | head -20

# Remove old shard version (e.g. v7 after upgrading to v8):
rclone delete r2:bc-fishing-regulations-staging --include "shards/v7/**"

# Nuclear: wipe entire bucket and re-seed:
rclone delete r2:bc-fishing-regulations-staging
./scripts/seed-r2.sh

# Sync staging → production exactly (removes prod files not in staging):
./scripts/promote-r2.sh --clean
```
