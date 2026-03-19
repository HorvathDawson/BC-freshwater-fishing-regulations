# Custom Domain Migration Plan

Migrate from `workers.dev` URLs to custom subdomains on `canifishthis.ca`.

## New URL Scheme

| | Staging | Production |
|---|---|---|
| **Frontend** | `staging.canifishthis.ca` | `canifishthis.ca` (unchanged) |
| **R2 Data** | `data-staging.canifishthis.ca` | `data.canifishthis.ca` |

`.com` domain: 301 redirect → `.ca` (preserving path).

> `data-staging` (not `data.staging`) because free Cloudflare SSL only covers single-level wildcards (`*.canifishthis.ca`).

---

## Phase 1 — You (Cloudflare Dashboard)

`canifishthis.ca` is already a zone (migrated from Pages to Workers). Only one dashboard step needed.

### 1a. Add `canifishthis.com` as a Zone (for redirect)

- Dashboard → Add Site → `canifishthis.com` → Free plan → update nameservers at `.com` registrar
- Once active, create a **Redirect Rule**:
  - **When:** hostname equals `canifishthis.com` OR hostname ends with `.canifishthis.com`
  - **Then:** Dynamic redirect → `https://canifishthis.ca${http.request.uri}`
  - **Status:** 301 (permanent)
  - **Preserve query string:** yes
- This ensures `canifishthis.com/waterbody/some-lake/?s=abc` → `canifishthis.ca/waterbody/some-lake/?s=abc`

### 1b. Tell me when `.com` zone is active

The `.com` redirect is independent of everything else — can be done anytime.

---

## Phase 2 — Me (Code Changes)

### R2 Worker (`r2-worker/wrangler.toml`)

Add custom domain routes — Wrangler auto-creates DNS CNAME records on deploy:

```toml
# Under [env.staging]:
[[env.staging.routes]]
pattern = "data-staging.canifishthis.ca"
custom_domain = true

# Under [env.production]:
[[env.production.routes]]
pattern = "data.canifishthis.ca"
custom_domain = true
```

> `custom_domain = true` means the Worker IS the origin (no proxying to an upstream). This is the correct mode for both Workers. Wrangler creates the DNS record automatically.
>
> The existing `canifishthis.ca` custom domain on the SPA worker doesn't conflict — different subdomains route to different workers within the same zone.

### SPA Worker (`wrangler.toml`)

```toml
# Under [env.staging]:
[[env.staging.routes]]
pattern = "staging.canifishthis.ca"
custom_domain = true
```

Production `canifishthis.ca` is already configured in dashboard — add it to wrangler.toml too for consistency.

### Env files

| File | Old | New |
|---|---|---|
| `webapp/.env.production` | `https://bc-fishing-r2.horvath-dawson.workers.dev` | `https://data.canifishthis.ca` |
| `webapp/.env.staging` | `https://bc-fishing-r2-staging.horvath-dawson.workers.dev` | `https://data-staging.canifishthis.ca` |

### Other URL references (5 edits)

| File | What changes |
|---|---|
| `scripts/update-in-season.sh` | Both `R2_ORIGIN` defaults |
| `data/fetch_data.py` | Tidal boundary download URL |
| `webapp/vite.config.ts` | Comment only |
| `DEPLOY.md` | All URL references |
| `STAGING.md` | workers.dev references |

---

## Phase 3 — Deploy Sequence

### Staging (just push everything at once)

Staging can break — push all changes together. Verify after:

```bash
curl -I https://data-staging.canifishthis.ca/api/version  # expect 200
curl -I https://staging.canifishthis.ca                    # expect 200
```

### Production (two-step, ~10 min gap)

**R2 custom domain must resolve before the frontend points at it.**

| Step | Action | Verify |
|---|---|---|
| 1 | Push wrangler.toml routes (keep **old** `.env.production` URL) | `curl -I https://data.canifishthis.ca/api/version` returns 200 (wait 5–10 min) |
| 2 | Push `.env.production` URL change + remaining refs | Visit `canifishthis.ca`, search works, map loads |

> **If Step 1 fails:** Don't do Step 2. Old workers.dev URL continues to work. Zero downtime.

---

## Risks & Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Frontend deploys before R2 custom domain resolves | **High** | Phase 3 ordering — deploy wrangler.toml first, verify, then env files |
| Cold edge cache after domain switch | Low | Transient — caches warm in hours. No action needed. |
| CI in-season job uses old URL | Medium | Script defaults change in Phase 2; if DNS isn't ready when CI runs, `R2_ORIGIN` env var in GH Actions overrides |
| `.com` redirect drops path | Low | Redirect rule uses `${http.request.uri}` to preserve full path |
| Old workers.dev URLs stop working | Low | `workers_dev = true` on staging keeps old URL alive. Production already has `workers_dev = false` — old URL may already be dead |

---

## Post-Migration Cleanup (Optional, Later)

- Set `workers_dev = false` on staging R2 worker (disable old `.workers.dev` URL)
- Tighten CORS from `*` to `canifishthis.ca, staging.canifishthis.ca` (optional, public data)
- Delete this file

---

## Files Changed (Summary)

| File | Edits |
|---|---|
| `r2-worker/wrangler.toml` | +2 custom_domains sections |
| `wrangler.toml` | +1 custom_domains section |
| `webapp/.env.production` | URL change |
| `webapp/.env.staging` | URL change |
| `scripts/update-in-season.sh` | 2 URL changes |
| `data/fetch_data.py` | 1 URL change |
| `webapp/vite.config.ts` | Comment update |
| `DEPLOY.md` | URL + docs updates |
| `STAGING.md` | URL updates |
| **Total** | **~14 edits across 9 files** |
