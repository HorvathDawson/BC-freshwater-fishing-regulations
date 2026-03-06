# Deployment Guide — Can I Fish This?

> **Live:** https://canifishthis.ca / https://www.canifishthis.ca

---

## Architecture

```
                        ┌───────────────────────────┐
  Browser ─────────────▶│  bc-fishing-regulations    │  Cloudflare Worker
  canifishthis.ca       │  (static site assets)      │  (auto-deploys via GitHub)
                        └───────────────────────────┘
                                    │
               pmtiles / json fetch │
                                    ▼
                        ┌───────────────────────────┐
                        │  bc-fishing-r2             │  Cloudflare Worker
                        │  (CORS proxy + Range)      │  (manual deploy)
                        └────────────┬──────────────┘
                                     │
                                     ▼
                        ┌───────────────────────────┐
                        │  bc-fishing-regulations    │  Cloudflare R2 Bucket
                        │  (object storage)          │  (private)
                        └───────────────────────────┘
```

| Component | Purpose | URL |
|---|---|---|
| **Site Worker** | Static HTML/CSS/JS, row_images | `canifishthis.ca` |
| **R2 Worker** | Serves large files from R2 with CORS + Range headers | `bc-fishing-r2.horvath-dawson.workers.dev` |
| **R2 Bucket** | Stores pmtiles + JSON data | *(private — accessed only via R2 Worker)* |

### R2 Bucket Contents

| File | Size | Description |
|---|---|---|
| `bc.pmtiles` | ~2.4 GB | Protomaps basemap tiles |
| `regulations_merged.pmtiles` | ~1.6 GB | Regulation vector tiles |
| `waterbody_data.json` | ~37 MB | Unified waterbody and regulation data |

---

## Prerequisites

| Tool | Install | Purpose |
|---|---|---|
| Node.js 22+ | `nvm install 22` | Build webapp |
| rclone | `curl https://rclone.org/install.sh \| sudo bash` | Upload large files to R2 |
| wrangler | `npm i` (dev dep in `webapp/`) | Deploy workers |

### One-Time: Configure rclone for R2

1. Create an R2 API token: **Cloudflare Dashboard → R2 → Manage R2 API Tokens → Create**
   - Permissions: **Object Read & Write**
   - Bucket: `bc-fishing-regulations`

2. Configure rclone:
```bash
rclone config create r2 s3 \
  provider Cloudflare \
  access_key_id YOUR_KEY \
  secret_access_key YOUR_SECRET \
  endpoint https://3e2ebcb3517b897ca56df9d7731133ba.r2.cloudflarestorage.com \
  acl private
```

3. Verify: `rclone lsf r2:bc-fishing-regulations/ --s3-no-check-bucket`

### One-Time: Authenticate wrangler

```bash
cd webapp && npx wrangler login
```

### One-Time: Custom Domain

**Cloudflare Dashboard → Workers & Pages → bc-fishing-regulations → Settings → Domains & Routes:**
- Add `canifishthis.ca`
- Add `www.canifishthis.ca`

Cloudflare provisions DNS + SSL automatically.

---

## How It Deploys

### Site (auto via GitHub)

The site worker is connected to GitHub. Every push to `main` triggers:

1. **Build:** `cd webapp && npm install && npm run build` → `webapp/dist/`
2. **Deploy:** `npx wrangler deploy` → reads root `wrangler.toml`, uploads `dist/` as static assets

Config: `wrangler.toml` at the **repo root** (not `webapp/`).

`VITE_TILE_BASE_URL` is baked in at build time from `webapp/.env.production` — no dashboard env vars needed for it.

### R2 Data (manual via rclone)

Large files are uploaded directly to R2 using rclone (wrangler has a 300 MiB upload limit).

### R2 Worker (manual via wrangler)

The CORS proxy worker is deployed manually with `wrangler deploy` from `webapp/r2-worker/`.

---

## Deployment Scripts

All scripts in `webapp/scripts/`. Run from `webapp/`.

| Script | What it does |
|---|---|
| `./scripts/deploy-all.sh` | Upload data + deploy R2 worker + build & deploy site |
| `./scripts/deploy-all.sh --skip-data` | Deploy R2 worker + build & deploy site |
| `./scripts/deploy-data.sh` | Upload all data to R2 |
| `./scripts/deploy-data.sh --tiles` | Upload only `.pmtiles` |
| `./scripts/deploy-data.sh --json` | Upload only `.json` |
| `./scripts/deploy-data.sh --file <path>` | Upload one file |
| `./scripts/deploy-worker.sh` | Deploy R2 CORS worker |
| `./scripts/deploy-site.sh` | Build + deploy site worker |
| `./scripts/deploy-site.sh --build-only` | Build only |
| `./scripts/deploy-site.sh --deploy-only` | Deploy existing `dist/` |

---

## Common Workflows

### Updated regulation data

```bash
cp output/regulation_mapping/regulations_merged.pmtiles webapp/public/data/
cp output/regulation_mapping/waterbody_data.json webapp/public/data/
cd webapp && ./scripts/deploy-data.sh
```

No site redeploy needed — data is fetched from R2 at runtime.

### Frontend code change

Push to `main` → auto-deploys via GitHub CI. Or manually:
```bash
cd webapp && ./scripts/deploy-site.sh
```

### R2 worker change (CORS, caching)

```bash
cd webapp && ./scripts/deploy-worker.sh
```

### New basemap tiles

```bash
cd webapp && ./scripts/deploy-data.sh --file public/data/bc.pmtiles
```

---

## Deploy From Scratch

If starting fresh (new Cloudflare account, new R2 bucket, etc.):

### 1. Create R2 bucket
```bash
cd webapp && npx wrangler r2 bucket create bc-fishing-regulations
```

### 2. Upload data to R2
```bash
./scripts/deploy-data.sh
```

### 3. Deploy R2 worker
```bash
./scripts/deploy-worker.sh
```

### 4. Create site worker via Cloudflare dashboard
1. **Cloudflare Dashboard → Workers & Pages → Create → Connect to Git**
2. Select **GitHub → HorvathDawson/BC-freshwater-fishing-regulations**
3. Configure:

| Field | Value |
|---|---|
| Worker name | `bc-fishing-regulations` |
| Production branch | `main` |
| Build command | `cd webapp && npm install && npm run build` |
| Deploy command | `npx wrangler deploy` |
| Root directory | `/` |
| Env: `NODE_VERSION` | `22` |

### 5. Add custom domains
**Workers & Pages → bc-fishing-regulations → Settings → Domains & Routes:**
- `canifishthis.ca`
- `www.canifishthis.ca`

---

## Environment Variables

| Variable | Where | Dev | Production |
|---|---|---|---|
| `VITE_TILE_BASE_URL` | `webapp/.env.*` | *(empty — local `/data/`)* | `https://bc-fishing-r2.horvath-dawson.workers.dev` |
| `NODE_VERSION` | Dashboard build env | — | `22` |

Vite reads `.env.production` automatically during `npm run build`.

---

## Project Structure

```
/wrangler.toml                       # Site worker config (used by CI deploy)
webapp/
├── .env.development                 # Dev: tiles from local /data/
├── .env.production                  # Prod: tiles from R2 worker
├── vite.config.ts                   # Excludes R2-only files from dist/
├── r2-worker/                       # R2 CORS proxy worker
│   ├── src/index.ts
│   └── wrangler.toml
├── scripts/
│   ├── deploy-all.sh
│   ├── deploy-data.sh
│   ├── deploy-site.sh
│   └── deploy-worker.sh
├── public/data/                     # Local data (gitignored large files)
│   ├── bc.pmtiles                   # → R2
│   ├── regulations_merged.pmtiles   # → R2
│   ├── waterbody_data.json          # → R2 (unified waterbodies + regulations)
│   └── row_images/                  # → deployed with site (small)
└── dist/                            # Build output (gitignored)
```

---

## Troubleshooting

**CORS errors** — R2 worker not deployed or broken:
```bash
curl -I https://bc-fishing-r2.horvath-dawson.workers.dev/waterbody_data.json
# Should show: access-control-allow-origin: *
cd webapp && ./scripts/deploy-worker.sh
```

**Blank map** — PMTiles not loading:
```bash
rclone lsf r2:bc-fishing-regulations/ --s3-no-check-bucket
curl -H "Range: bytes=0-100" https://bc-fishing-r2.horvath-dawson.workers.dev/bc.pmtiles
```

**Build fails in CI** — Missing `public/` is OK (handled gracefully by vite plugin). Check `NODE_VERSION=22` is set in dashboard build env.

**Auth issues:**
```bash
npx wrangler login && npx wrangler whoami
```
