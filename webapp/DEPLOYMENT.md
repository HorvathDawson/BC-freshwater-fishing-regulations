# Deployment Guide — Can I Fish This?

> **Live URL:** https://canifishthis.ca

## Architecture Overview

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│   Browser     │────▶│  Cloudflare Pages     │     │  Cloudflare R2   │
│               │     │  (static site)        │     │  (data storage)  │
│  canifishthis │     │  bc-fishing-           │     │  bc-fishing-     │
│  .ca          │     │  regulations.pages.dev │     │  regulations     │
└──────┬───────┘     └──────────────────────┘     └────────▲─────────┘
       │                                                     │
       │  pmtiles / json requests                           │
       └───────▶┌──────────────────────┐───────────────────┘
                │  Cloudflare Worker    │
                │  bc-fishing-r2        │
                │  (CORS + Range proxy) │
                └──────────────────────┘
```

| Component | What it serves | URL |
|---|---|---|
| **Pages** | HTML, CSS, JS, small assets, row_images | `canifishthis.ca` / `bc-fishing-regulations.pages.dev` |
| **R2 Worker** | PMTiles, regulations.json, search_index.json (with CORS) | `bc-fishing-r2.horvath-dawson.workers.dev` |
| **R2 Bucket** | Raw object storage | `bc-fishing-regulations` (not publicly accessed directly) |

### Files in R2

| File | Size | Description |
|---|---|---|
| `bc.pmtiles` | ~2.4 GB | Protomaps basemap tiles for BC |
| `regulations_merged.pmtiles` | ~1.6 GB | Fishing regulation vector tiles |
| `regulations.json` | ~1.7 MB | Regulation text/rules lookup |
| `search_index.json` | ~35 MB | Waterbody search index |

---

## Prerequisites

1. **Node.js** (v18+) and **npm**
2. **rclone** — for uploading large files to R2
3. **wrangler** — Cloudflare CLI (included as dev dependency)
4. **Cloudflare account** authenticated via `npx wrangler login`

---

## One-Time Setup

### 1. Install rclone

```bash
curl https://rclone.org/install.sh | sudo bash
```

### 2. Configure rclone for R2

Create an R2 API token at:
**Cloudflare Dashboard → R2 → Manage R2 API Tokens → Create API token**
- Permissions: **Object Read & Write**
- Bucket: `bc-fishing-regulations`

Then run:
```bash
rclone config create r2 s3 \
  provider Cloudflare \
  access_key_id YOUR_ACCESS_KEY_ID \
  secret_access_key YOUR_SECRET_ACCESS_KEY \
  endpoint https://3e2ebcb3517b897ca56df9d7731133ba.r2.cloudflarestorage.com \
  acl private
```

Verify it works:
```bash
rclone lsf r2:bc-fishing-regulations/ --s3-no-check-bucket
```

### 3. Authenticate wrangler

```bash
cd webapp
npx wrangler login
```

### 4. Custom Domain (canifishthis.ca)

This is a one-time setup done in the Cloudflare Dashboard:

1. Go to **Workers & Pages → bc-fishing-regulations → Custom domains**
2. Add `canifishthis.ca`
3. Add `www.canifishthis.ca`
4. Cloudflare automatically creates the DNS records and provisions SSL

Both `www` and non-`www` will serve the site. Cloudflare handles the redirect.

---

## Deployment Scripts

All scripts are in `webapp/scripts/`. Run them from the `webapp/` directory.

### Deploy Everything

```bash
cd webapp
./scripts/deploy-all.sh              # Data + Worker + Site
./scripts/deploy-all.sh --skip-data  # Worker + Site only (faster)
```

### Deploy Data to R2 Only

Use this when you've regenerated PMTiles or JSON data files.

```bash
./scripts/deploy-data.sh              # All data files (pmtiles + json)
./scripts/deploy-data.sh --tiles      # Only .pmtiles files
./scripts/deploy-data.sh --json       # Only .json files
./scripts/deploy-data.sh --file public/data/regulations_merged.pmtiles  # Single file
```

### Deploy R2 Worker Only

Use this if you've modified `r2-worker/src/index.ts`.

```bash
./scripts/deploy-worker.sh
```

### Deploy Website Only

Use this for frontend code changes.

```bash
./scripts/deploy-site.sh              # Build + deploy
./scripts/deploy-site.sh --build-only # Build only (for testing)
./scripts/deploy-site.sh --deploy-only# Deploy existing dist/
```

---

## Common Workflows

### "I updated the regulation pipeline and have new output files"

```bash
# 1. Copy new output files to webapp/public/data/
cp output/regulation_mapping/regulations_merged.pmtiles webapp/public/data/
cp output/regulation_mapping/regulations.json webapp/public/data/
cp output/regulation_mapping/search_index.json webapp/public/data/

# 2. Upload to R2
cd webapp
./scripts/deploy-data.sh

# 3. Rebuild + deploy the site (picks up new regulations.json for small-file serving)
./scripts/deploy-site.sh
```

### "I changed only frontend code (React/CSS)"

```bash
cd webapp
./scripts/deploy-site.sh
```

### "I changed the R2 worker (CORS config, caching, etc.)"

```bash
cd webapp
./scripts/deploy-worker.sh
```

### "I regenerated bc.pmtiles (basemap)"

```bash
cd webapp
./scripts/deploy-data.sh --file public/data/bc.pmtiles
```

---

## Environment Variables

| Variable | Dev Value | Production Value |
|---|---|---|
| `VITE_TILE_BASE_URL` | *(empty — uses local `/data/`)* | `https://bc-fishing-r2.horvath-dawson.workers.dev` |

These are set in:
- `webapp/.env.development` — local dev server
- `webapp/.env.production` — production builds

The Vite build automatically reads `.env.production` when running `npm run build`.

---

## Project Structure (Deployment-Related)

```
webapp/
├── public/data/                 # Local data files (gitignored: *.pmtiles, search_index.json)
│   ├── bc.pmtiles               # Basemap tiles (→ R2)
│   ├── regulations_merged.pmtiles  # Regulation tiles (→ R2)
│   ├── regulations.json         # Regulation lookup (→ R2)
│   ├── search_index.json        # Search index (→ R2)
│   └── row_images/              # Synopsis row images (→ Pages, small files)
├── r2-worker/                   # Cloudflare Worker for R2 with CORS
│   ├── src/index.ts
│   └── wrangler.toml
├── scripts/
│   ├── deploy-all.sh            # Full deployment
│   ├── deploy-data.sh           # Upload data to R2
│   ├── deploy-site.sh           # Build + deploy to Pages
│   └── deploy-worker.sh         # Deploy R2 worker
├── .env.development             # Dev env vars
├── .env.production              # Production env vars
└── dist/                        # Build output (→ Pages, gitignored)
```

---

## Troubleshooting

### CORS errors in browser
The R2 worker (`bc-fishing-r2`) adds CORS headers. If you see CORS errors:
1. Verify the worker is deployed: `curl -I https://bc-fishing-r2.horvath-dawson.workers.dev/regulations.json`
2. Redeploy: `./scripts/deploy-worker.sh`

### "File too large for Pages" during deploy
Pages has a 25 MiB per-file limit. Large files must go to R2:
1. Add the filename to the exclusion list in `vite.config.ts` (`copyPublicWithoutPmtiles`)
2. Upload to R2: `./scripts/deploy-data.sh --file <path>`
3. Ensure the fetch URL uses `DATA_BASE` (R2 in production)

### PMTiles not loading (blank map)
1. Check R2 upload: `rclone lsf r2:bc-fishing-regulations/ --s3-no-check-bucket`
2. Test worker: `curl -H "Range: bytes=0-100" https://bc-fishing-r2.horvath-dawson.workers.dev/bc.pmtiles`
3. Check browser console for the actual URL being requested

### wrangler auth issues
```bash
npx wrangler login   # Re-authenticate
npx wrangler whoami  # Verify
```
