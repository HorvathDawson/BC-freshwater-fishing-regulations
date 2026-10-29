# Staging — inventory and deploy (2026-10-28)

Owner 2026-10-27/28: Claude may clear + upload the STAGING bucket and deploy the STAGING site. Production
(`bc-fishing-regulations` bucket, `bc-fishing-build-assets`, worker `bc-fishing-regulations` / `bc-fishing-r2`,
canifishthis.ca / data.canifishthis.ca) is never touched.

## Inventory BEFORE any change (2026-10-28 19:05 UTC)
Cloudflare account `Horvath.dawson@gmail.com's Account` (3e2ebcb3…), wrangler 4.154.0 (`npx --prefix app wrangler`).

| Thing | Staging | Production (not touched) |
|---|---|---|
| Site worker (static assets) | `bc-fishing-regulations-staging` -> **https://staging.canifishthis.ca** (custom domain), last deploy 2026-07-24 | `bc-fishing-regulations` -> canifishthis.ca |
| Data worker (R2 proxy: CORS, Range, edge cache) | `bc-fishing-r2-staging` -> **https://data-staging.canifishthis.ca**, last deploy 2026-07-24, source `archive/r2-worker/` (`--env staging`) | `bc-fishing-r2` -> data.canifishthis.ca |
| R2 bucket | `bc-fishing-regulations-staging` (created 2026-03-18): **0 objects, 0 B** (rclone `r2:` remote; `data_version.json` 404 via wrangler) | `bc-fishing-regulations` (20,033 objects, 8.0 GiB) |
| Pages projects | none in the account | none |

- Hosting is Workers + static assets (not Pages). v1's config: `archive/webapp/wrangler.toml` (`[env.staging]`), docs
  `archive/STAGING.md`, `archive/DEPLOY.md`. v1 deployed both workers by Cloudflare git integration on a `staging` branch.
- What staging served before: the v1 SPA (title "BC Fishing Regulations Map | …"); `/waterbody/<id>/` answered 200 (SPA
  fallback); `robots.txt` is Cloudflare's managed content-signals file; data-staging `/api/version` -> `{"version":"v2"}`.
- Nothing new is created: the existing staging site worker gets the new assets, the data worker is reused as is.

## Layout the app reads (app/apps/mobile/src/config.ts, base = `EXPO_PUBLIC_DATA_BASE`)
`/release/manifest.json`, `/release/<release>/…` (publish.py's keys under a `release/` prefix: tiles/<ver>/…, basemap/…,
r-<id>/…, manifest last), `/feeds/live/<station>.json` + `index.json`, `/feeds/dfo/*.json`, `/sql-wasm-browser.wasm` at the root.

## What was done (2026-10-28)
Release **r-46be3c135e68** (LATEST; tiles db853e9556df81a6.16b3906b04bf, basemap-147bd82d051f).
1. **Data worker replaced** (`deploy/staging/data.wrangler.toml` + `data-worker.js`, same name/domain/bucket). The v1 worker
   gzipped the gzip-stored release files a second time (`*.sqlite.gz` came back double-encoded, so `fetchVerified`'s sha256
   would fail): the new one passes stored Content-Encoding through (`encodeBody: "manual"`) and honours each object's own
   Cache-Control (manifest `no-cache`, content-named files immutable). **Production's `bc-fishing-r2` has the same defect** —
   fix before a release goes to prod (T86). v1's `/api/resolve|version|feedback` are gone on staging (the new app uses none).
2. **Bucket cleared and filled** by `deploy/staging/upload.sh r-46be3c135e68` (rclone; bucket name hardcoded): publish.py's
   files under `release/` (tiles folder, basemap, 20 data files gzip, salmon parts, synopsis PDF, manifest LAST), the gauge
   feed `feeds/live/` (442 files, as last written locally — no scheduled publisher on staging, so it goes stale), `feeds/dfo/`,
   `sql-wasm{,-browser}.wasm`. 1,260 objects, 505 MiB.
3. **Builds**: `expo export --platform web` with `EXPO_PUBLIC_DATA_BASE=https://data-staging.canifishthis.ca`
   (`EXPO_PUBLIC_APP_BUILD="staging <sha>"`, contact unset) -> `app/dist/web-staging` (87 files, 9.8 MB);
   `build-seo.mjs --release-dir data/release/r-46be3c135e68` -> 19,774 pages (19,482 waters), p95 3,342 B gzip, largest
   7,984 B, css 814 B, budget problems 0, all titles/descriptions unique (97 titles > 60, 1 description > 155), slug map
   unchanged vs the committed one, v1 redirects water 19,147 / region 162 / home 22.
4. **Site deployed** by `deploy/staging/site.sh` (merge app + SEO into `app/dist/staging-site`, no clashes; `_worker.js` is
   the worker, `run_worker_first = ["/waterbody/*"]`, `not_found_handling = "404-page"`): 39,350 files.
   Staging-only edits: `robots.txt` = `Disallow: /`, `_headers` adds `X-Robots-Tag: noindex, nofollow` on every path, and the
   pages' "Open in the app" link is made relative (`/?regs=…`) so it opens the staging app, not prod. Canonicals stay on
   https://canifishthis.ca.

## Verified against https://staging.canifishthis.ca (shots: staging-shots/)
- App: loads and paints (01), reads manifest/paint/tiles/basemap/feeds from data-staging (38 requests, no 4xx), a tap on the
  Fraser opens its regs (02), `/?regs=gnis:3062` opens Vedder River regs, desktop + phone (03, 04).
- Water pages 200: vedder-river, cowichan-river, skeena-river, harrison-river, kootenay-lake-main-body (05, 06, 08); hubs
  `/region/2`, `/region/2/open-this-weekend`, `/topic/steelhead`, `/about` 200 (07); unknown path -> real 404 page (09).
- `sitemap.xml` (index) + `sitemap-1.xml` 19,775 `<loc>` (19,774 pages + `/`), all on https://canifishthis.ca.
- Legacy: `/waterbody/328939712/`, `/329457701/`, `/200-948755-905772-422233/`, `/930-055749/` -> ONE 301 each to
  nehounlee-marsh, howie-bowlett-lake, emerslund-creek, san-juan-river (names match v1's live titles); `/328961697/` -> 301
  `/region/7`; `/100/` -> 301 `/` (10). Every target 200.
- Not indexable: every response carries `X-Robots-Tag: noindex, nofollow`.

## Findings (owner / prod)
- **Cloudflare managed robots.txt** (zone setting) is PREPENDED to ours: it opens with `User-agent: * / Allow: /`, which
  merged with our `Disallow: /` resolves to allow (Google takes the least restrictive on a tie). So staging relies on the
  X-Robots-Tag header, not robots. In prod the same block will precede tools/seo's robots (and blocks AI crawlers incl.
  ClaudeBot/GPTBot): owner's call whether to keep the managed robots.
- **Lighthouse (mobile, lighthouse 12.8.2, 4 pages)**: A11y 100, CLS 0, but Performance 69-76, LCP 1.7-3.1 s, TBT ~1.1-1.2 s
  — all of it Cloudflare-injected script (`/cdn-cgi/challenge-platform/.../jsd/main.js` 2.6 s of script, plus the Web
  Analytics beacon). Our pages ship no JS. Turning off Bot Fight Mode / JS detections and RUM injection on the zone is the
  owner's call; re-run the gate after. SEO 58 on staging only because of the noindex (is-crawlable), expected.
- `/water/<slug>/` answers **307** (assets `drop-trailing-slash`), not 301: prod should 301 it (worker or a redirect rule).
- `/favicon.ico` 404 on every page (no favicon in the app export or the SEO build).

## Redo
`deploy/staging/upload.sh <release>` (clears the bucket EXCEPT `feeds/**` + `private/**` — the feeds worker's output and state, T225 Phase A — then runs `deploy/staging/sync-inputs.sh`), rebuild the two dist folders as in 3, `deploy/staging/site.sh`.
`npx --prefix app wrangler deploy -c deploy/staging/data.wrangler.toml` only if the data worker changes.

## Phase B (2026-10-29 UTC): release r-0fa7927b16e5 on staging
- `deploy/staging/upload.sh r-0fa7927b16e5` (clear spares feeds/, private/, runtiming/, test/): tiles 10cbcb72e9f907cf.edc12ab96087
  (obstacle layer in the pmtiles), basemap, 20 data files + 12 salmon parts, synopsis-08f58537261f.pdf, manifest stamped
  `published 2026-10-29` (T251 stamp) and put LAST; sync-inputs (entries same, matches 22). 1,313 objects, 557 MiB.
- Builds: `pnpm -C app/apps/mobile export:web:staging` (T232; build 0374a71) and `pnpm -C app seo --release-dir
  ../data/release/r-0fa7927b16e5` → 19,945 pages (19,653 waters), p95 3,346 B gzip. `deploy/staging/site.sh` → version 6cbb47ac.
- Verified on https://staging.canifishthis.ca (shots staging-shots/B01–B13): map paints from the new release (every request
  to data-staging, release r-0fa7927b16e5, no 4xx but /favicon.ico) (B01); Vedder regs (B02); 6-row in-season bell (B03); tap on
  an unnamed stream → "Unnamed stream · Region 2" regs (B04); Pitt River Conditions: estimate from 08MH006 (North Alouette,
  donor), live reading 21:45 PDT, CLEVER/COFFEE (B05); Salmon mode (B06); Run timing mode, runtiming/index.json
  tile_version = the release's tiles (B07); in-season banner on Cadorna Creek (B08); fish obstacles layer (B09); "Rule book
  p.N" opens our synopsis PDF (blob of release/…/synopsis-08f58537261f.pdf, #page=11) (B10); sync button "Refresh data: up
  to date" (B11); Updates log (app 0.1.0 build 1 0374a71, data staging, channel live) (B12); feeds /status all ok, gauges
  last run 05:35 UTC from the :35 cron (B13).
- Seen: More → Data still says "In-season changes are not read" although the app now reads them (T230 wording to fix).
