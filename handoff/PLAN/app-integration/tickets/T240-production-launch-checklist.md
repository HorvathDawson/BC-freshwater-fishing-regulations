# T240 — production launch checklist (one list, in order)

Status: OPEN — the list is written and brought up to date 2026-10-29 (T269-T271, T274, T279, map-assets, privacy page, splash); nothing on it is started (prod is the owner's; T86 is POSTPONED until he says go).

Source: owner 2026-10-28 — approved proposal P6 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").
Pulls together what T86 (hosting), T198 (SEO redirects / Search Console), T225 (feeds), T232 (environments),
handoff/PLAN/seo/staging.md "Findings (owner / prod)" and OWNER-TODO.md say prod needs. Staging is the rehearsal:
everything below was done or measured on staging.canifishthis.ca / data-staging.canifishthis.ca first.

★ = owner only (credentials, billing, DNS, accounts). Everything else Claude can prepare (configs, scripts, a dry run
against staging) and the owner runs on prod.

## 1. Data worker (prod `bc-fishing-r2`, bucket `bc-fishing-regulations`)
- [ ] Replace prod's worker with `deploy/staging/data-worker.js` (a prod wrangler file next to it, `data.prod.wrangler.toml`,
      prod bucket + route `data.canifishthis.ca`). Fixes: **release files double-gzipped** by the old worker, and the
      **weak ETag compare** (`If-None-Match: W/"…"` → 304; the edge sends `W/` when it gzips; fixed on staging
      02e50024, 2026-10-28). The T231 update service depends on both.
- [ ] `index.json` / feed headers: `Content-Encoding: gzip`, `Cache-Control: public, max-age=300` (T86 / T182 item 5).
- [ ] ★ Decide the prod data host name (default `data.canifishthis.ca`, as `apps/mobile/src/env.ts` ORIGINS.production).
- [ ] Check: `curl -I` a release file (one gzip, right `Content-Type`), a 304 on a repeat with the weak ETag, `private/`
      not served (T249).

## 2. Prod R2 upload
- [ ] ★ R2 credentials for the prod bucket (wrangler login on the owner's machine).
- [ ] Prod upload script = `deploy/staging/upload.sh` with the bucket parameterised (never clears `feeds/**` +
      `private/**`), run with the current release (LATEST) + tiles + basemap + synopsis PDF copy; then
      `sync-inputs.sh` against prod (DFO entry files + slim roster into `private/feeds/…`).
- [ ] Verify the manifest's every file answers 200 from `data.canifishthis.ca` with the right sha256 (a script).

## 3. Feeds worker prod + crons (T225)
- [ ] A prod section in `app/workers/feeds/wrangler.toml` (or a second file): name `bc-fishing-feeds`, prod bucket,
      the same crons as staging (gauges :05/:35 day + hourly night, DFO :15, in-season :45).
- [ ] ★ Secrets on prod: `RUN_TOKEN` (fresh, not staging's), `NOTIFY_EMAIL_TO`; vars `NOTIFY_EMAIL_FROM`,
      `NOTIFY_DIGEST`; Email Routing destination verified.
- [ ] ★ GitHub secrets for the prod option of `feeds-hydat.yml` / `feeds-sync-inputs.yml` (`CLOUDFLARE_ACCOUNT_ID`,
      prod R2 key pair) and un-comment their prod option.
- [ ] Run every job once by hand with `?dryNotify=1`, then let the schedule run 24 h; `GET /status` all fresh, the gauge
      feed < 2 h old throughout (T86 acceptance).

## 4. Retire the old prod jobs — ONLY once the prod app reads `feeds/`
- [ ] ★ Disable the `bc-fishing-cron` worker (source `archive/cron-runner/`; dispatches `main`'s `update-hydro.yml`
      every 15 min).
- [ ] ★ Delete or disable `main`'s `.github/workflows/update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml`
      (stocking moves to T200; the in-season "unmatched → issue" duty is now the feeds worker's email).
- [ ] Leave the OLD R2 layout (`stations.json`, `gauges.geojson`, `recent/`, `history/`, `climatology/`) for 30 days for
      old app installs, then delete (a dated reminder in OWNER-TODO).

## 5. Site + DNS + redirects (T198)
- [ ] Prod site wrangler (from `deploy/staging/site.wrangler.toml`): name `bc-fishing-regulations`, route
      `canifishthis.ca`; `site.sh` with a prod switch that does NOT apply the staging-only edits (no `Disallow: /`
      robots, no `X-Robots-Tag: noindex, nofollow`, "Open in the app" links stay absolute to prod).
- [ ] ★ Registrar/DNS: `canifishthis.com`, `www.canifishthis.com`, `www.canifishthis.ca` → **301** to
      `https://canifishthis.ca` (path + query kept; `redirects.json`); HSTS once https is confirmed everywhere.
- [ ] `/water/<slug>/` answers 307 today (assets `drop-trailing-slash`): make it a 301 (worker or a redirect rule).
- [ ] v1 legacy paths `/waterbody/*` → one 301 each (already in the site worker; re-check on prod).
- [ ] Add a favicon (404 on every page today).

## 6. Search (T198 / T188)
- [ ] ★ Google Search Console Domain property (DNS TXT) + Bing Webmaster import; submit `/sitemap.xml`.
- [ ] Remove staging's noindex from prod (§5) and confirm prod sends no `X-Robots-Tag` (a check in T241's smoke test).
- [ ] Keep the existing v1 Search Console property; watch the 301s re-index (T198 outcome).

## 7. Cloudflare zone settings (★ owner's call; defaults from seo/staging.md findings)
- [ ] **Managed robots.txt**: it is PREPENDED to ours (`User-agent: * / Allow: /` + AI-crawler blocks). Default: keep it off
      on prod so tools/seo's robots.txt is the one robots (`Disallow: /api/, /data/`); owner decides on AI crawlers.
- [ ] **Bot Fight Mode / JS detections + Web Analytics (RUM)**: inject ~2.6 s of script into every page (Lighthouse
      Performance 69–76 instead of ≥ 95). Default: off on the site zone (or excluded for `canifishthis.ca/*` pages);
      re-run the T196 Lighthouse gate after.

## 8. App config — production values (T232)
- [ ] `eas.json` `production` profile: `EXPO_PUBLIC_APP_ENV=production` (set), plus `EXPO_PUBLIC_APP_BUILD`
      (version + build number), ★ `EXPO_PUBLIC_CONTACT` (the release build refuses to build without it — T86).
- [ ] Optional `EXPO_PUBLIC_ARCGIS_API_KEY` stays unset (owner 2026-10-21: no key).
- [ ] Web prod export points at the prod data origin (`export:web` with `EXPO_PUBLIC_APP_ENV=production`).
- [ ] The About page shows no environment badge on a production build reading production (T232 `aboutEnvVM`).

## 9. Secrets inventory (with T249)
- [ ] One table (name, where set, who rotates): feeds `RUN_TOKEN`, `NOTIFY_EMAIL_TO`; GitHub `CLOUDFLARE_ACCOUNT_ID`,
      R2 key pairs (staging + prod); EAS / store credentials (T244); none in the repo (`.run-token` stays gitignored).

## 10. Smoke tests + go
- [ ] T241's smoke test pointed at prod (`--base https://canifishthis.ca --data https://data.canifishthis.ca`): map paints,
      tap → regs, conditions, salmon, PDF, a `/water` page, a legacy 301, the map attribution, the disclaimer.
- [ ] The same on a phone with a `production` build (T77 / T244 internal track).
- [ ] Rollback written down: the previous release stays in R2 (T72 keeps live + previous); LATEST back one line.

## 11. Added 2026-10-29 (what today's work puts on the prod run; every command is the OWNER's — prod stays human)
Paths are from the repo root; `<R>` = the release folder (`data/release/<r-id>`), `<PB>` = the prod bucket `bc-fishing-regulations`.
- [ ] **Map assets (T270)** — fonts + symbols from our host, no CDN: `rclone copy <R>/$(python3 -c "import json;print(json.load(open('<R>/manifest.json'))['tiles']['assets']['url'])") r2:<PB>/map-assets/v1 --s3-no-check-bucket --transfers 16 --header-upload "Cache-Control: public, max-age=31536000, immutable"` (pbf as `application/x-protobuf`, as `upload.sh` does for staging). Check: `curl -I https://data.canifishthis.ca/map-assets/v1/<a glyph>` is 200 and `tiles.assets` in prod's manifest.
- [ ] **HYDAT day flow (T274)** — `rclone copy data/generated/gauges/hydat_daily r2:<PB>/feeds/hydat --s3-no-check-bucket --header-upload "Cache-Control: public, max-age=86400"` (same HYDAT vintage as the prod bundle; rerun after `scripts/update-hydat.sh`). Check: one station file 200 with CORS open.
- [ ] **Feeds worker prod (T225, T271)** — beyond section 3: the worker also runs the daily report purge (:25 cron: `private/reports/**` older than 180 d). After deploying with the prod config: `curl -X POST -H "Authorization: Bearer $RUN_TOKEN" https://<prod feeds worker>/run/purge-reports`, then `/status.json` shows `reportRetention`. Command: `npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.prod.toml` (the prod file named in section 3).
- [ ] **SEO build for prod (T269, T279)** — water pages are the app's own pane, so the SEO build MUST be given the prod app export it will be served beside, or the pages' script tag names a bundle the site lacks (`site.sh` refuses it for staging):
      `EXPO_PUBLIC_APP_ENV=production pnpm -C app --filter @app/mobile exec expo export --clear --platform web --output-dir ../../dist/web-prod` then
      `node app/tools/build-seo.mjs --release-dir data/release/<r-id> --app app/dist/web-prod --threads 4 --out app/dist/seo-prod` (heavy — one at a time via `scripts/heavy.sh`, ≤4 threads; T279 stores each page compressed only, `index.html.gz`/`.br`, served by the worker by Accept-Encoding; the prod site switch of `site.sh` is section 5; confirm the exact flags against the `site.sh` staging call when writing the prod switch). Check: the page's `<script src>` is in the export; `/water/<slug>` shows the pane with JS off.
- [ ] **Prod site deploy of the above** — the prod wrangler (section 5) with the pre-compressed pages (T279, when done) and the host's file-count/size headroom noted there.
- [ ] **Privacy page live (T268/T244)** — `https://canifishthis.ca/privacy` answers 200 on prod (it is part of the SEO build; linked from the footer, About, Sources, the report form). The store listings quote this URL. The mailbox `privacy@canifishthis.ca` exists (owner set up) and the build's `EXPO_PUBLIC_CONTACT` is the same address.
- [ ] **Release promote check** — prod `LATEST` and the manifest the app reads name the same release as the tiles and the SEO build (one vintage: tiles, bundle and pages).
- [ ] **Store listing (T244)** — owner signs off `handoff/PLAN/app-integration/store/listing-draft.md` + `privacy-labels-draft.md`; then Play internal track (T268/T267) and TestFlight (T88). Splash + icon are in the production build's prebuild (T244).
Dependencies unchanged: sections 1-3 before the app's production build reads prod; section 4 only after.

## Acceptance
Every box ticked or marked n/a by the owner; the smoke test green on prod; the feed fresh for 24 h; the old cron off
only after the prod app reads `feeds/`.

Depends: T86 (POSTPONED — this list is its body), T198, T225, T232, T241, T249. Size: S (Claude's part) + owner time.
Owner: yes — every ★ line. Default: the defaults stated on each line.
