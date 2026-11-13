# Production releases (T404)

Production is canifishthis.ca (site), data.canifishthis.ca (bucket `bc-fishing-regulations`, worker `bc-fishing-r2`),
the feeds worker `bc-fishing-feeds` and the Play app built with `EXPO_PUBLIC_APP_ENV=production`.

**Who does what.** Every step that writes production is the OWNER's: it refuses without `CONFIRM_PROD` and Claude never
runs it. Claude builds, runs staging, and rehearses (`--dry-run`, or `--bucket <non-prod>`), which need no confirmation
because they write nothing to production.

**The one switch.** The app picks its environment from `EXPO_PUBLIC_APP_ENV` (eas.json, `build-apk.sh --production`).
Every host comes from `app/apps/mobile/src/env.ts` (data, tiles, map assets, feeds, synopsis, sql.js, `play.json`,
reports, crashes); the public site is `@app/core` `SITE_ORIGIN`. `app/tools/no-env-hosts.test.ts` fails on a host spelt
anywhere else in app code.

| piece | staging | production |
|---|---|---|
| data upload | `deploy/staging/upload.sh` | `deploy/prod/upload.sh` (promotes what staging serves) |
| data rollback | — | `deploy/prod/rollback.sh` |
| data worker | `deploy/staging/data.wrangler.toml` | `deploy/prod/data.wrangler.toml` (same `data-worker.js`) |
| feeds worker | `deploy/staging/feeds.sh` + `wrangler.toml` | `deploy/prod/feeds.sh` + `app/workers/feeds/wrangler.prod.toml` |
| worker inputs | `deploy/staging/sync-inputs.sh` | `deploy/prod/sync-inputs.sh` (called by upload) |
| site | `deploy/staging/site.sh` | `deploy/prod/site.sh` |
| Play build | `deploy/staging/build-apk.sh --play` | `deploy/prod/build-apk.sh` (= `--play --production`) |
| smoke | `node app/tools/smoke-staging.mjs` | `node app/tools/smoke-staging.mjs --production` |

## The first transition (once, in this order)

Before: the release is on staging and the owner has checked it there (phone + web).

1. **Claude — rehearse every script** (nothing written to production):
   ```bash
   deploy/prod/upload.sh --bucket bc-fishing-regulations-staging --dry-run
   deploy/prod/feeds.sh --dry-run
   npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml --dry-run --outdir /tmp/data-dry
   deploy/prod/build-apk.sh --dry-run
   deploy/prod/site.sh --dry-run          # after step 6's two builds
   ```
2. **Owner — accounts and buckets.** In Cloudflare: R2 bucket `bc-fishing-regulations-backup` with staging's lifecycle
   rules (daily/ 30 d, monthly/ 400 d, curated/ 365 d, restore-test/ 1 d) (Q172). Email Routing has
   `support@` and `privacy@canifishthis.ca`. The prod feeds placeholder and its secrets exist
   (`CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds-placeholder.sh`, done 11-05). Write the production `RUN_TOKEN` to
   `app/workers/feeds/.run-token.prod` (gitignored).
3. **Owner — the data worker.** Check the old one's name and route (Workers → bc-fishing-r2 → Domains & Routes; Q174), then
   ```bash
   npx --prefix app wrangler deploy -c deploy/prod/data.wrangler.toml
   curl -sI https://data.canifishthis.ca/stations.json | head -3      # the v1 keys still answer (until step 11, the same day)
   ```
4. **Owner — the data.** The release staging serves:
   ```bash
   CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>   # runs CONFIRM_PROD=canifishthis.ca deploy/prod/upload.sh <r-id>
   ```
   It refuses unless staging's live manifest is byte-identical to `data/release/<r-id>/manifest.json`.
5. **Owner — the feeds worker** (deploys over the placeholder; the secrets stay):
   ```bash
   CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh
   curl -X POST -H "Authorization: Bearer $(cat app/workers/feeds/.run-token.prod)" \
     https://feeds.canifishthis.ca/run/purge-reports
   ```
   Then in GitHub: uncomment `- production` and the prod secrets in `feeds-hydat.yml`, `feeds-runtiming.yml`
   (the inputs go up from the Mac: `deploy/prod/sync-inputs.sh`, T520), and add `PROD_R2_ACCESS_KEY_ID` / `PROD_R2_SECRET_ACCESS_KEY`. Until then the prod worker's
   HYDAT / run-timing starts fail and say so by email (it asks for `target: production`, never staging's bucket).
6. **Claude builds, owner deploys — the site** (one vintage with the data; the SEO build is heavy):
   ```bash
   EXPO_PUBLIC_CONTACT=support@canifishthis.ca EXPO_PUBLIC_APP_ENV=production \
     pnpm -C app --filter @app/mobile exec expo export --clear --platform web --output-dir ../../dist/web-prod
   scripts/heavy.sh seo-prod 30 -- node app/tools/build-seo.mjs --release-dir data/release/<r-id> \
     --app app/dist/web-prod --threads 4 --out app/dist/seo-prod
   CONFIRM_PROD=canifishthis.ca deploy/prod/site.sh                 # owner
   ```
   Cloudflare (owner): Managed robots.txt OFF; Bot Fight Mode / JS detections and Web Analytics OFF on the site zone.
7. **Owner — the www redirect keeps the path.** Today `www.canifishthis.ca/water/vedder-river` → 301 to the home page.
   Cloudflare → canifishthis.ca → Rules → Redirect Rules → the www rule → Dynamic,
   `concat("https://canifishthis.ca", http.request.uri.path)`, 301, preserve query string. (Or delete the rule and
   `npx --prefix app wrangler deploy -c deploy/prod/host.wrangler.toml`, which also covers the two .com names.) Check:
   ```bash
   curl -sI https://www.canifishthis.ca/water/vedder-river | grep -i location   # https://canifishthis.ca/water/vedder-river
   ```
8. **Owner — check it all:** `node app/tools/smoke-staging.mjs --production`; `curl -s https://feeds.canifishthis.ca/status`
   all fresh, and again after 24 h (gauges < 2 h old throughout).
9. **Owner — the app.** The changelog entry for the version is in `app/packages/core/src/changelog.ts` (Claude), then
   ```bash
   CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh             # AAB -> Play internal; play.json -> prod bucket
   ```
   Install from internal on the phone (About shows no environment badge), then promote the same build to Closed testing
   in Play Console (T371). From now on every Play build is production; staging stays on the sideload APK page (Q173).
10. **Owner — search.** Search Console Domain property (DNS TXT) + Bing import; submit `https://canifishthis.ca/sitemap.xml`.
11. **Owner — the same day (Q174, owner 2026-11-08: the owner is the only user, so no 30-day wait).** Once the prod app
    reads `feeds/` and the new site is live: disable the `bc-fishing-cron` worker and `main`'s `update-hydro.yml`,
    `update-in-season.yml`, `update-stocking.yml`, and delete the v1 R2 layout (`stations.json`, `gauges.geojson`,
    `recent/`, `history/`, `climatology/`).

## Every later release

1. **Claude:** `python -m pipeline release --staging` (build → checks → promote LATEST → staging upload + smoke); staging
   feeds worker / site / APK when they changed.
2. **Owner:** check staging (phone + web).
3. **Owner — data:** `CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>`.
4. **Owner — feeds worker, only if it changed** (with the app, never ahead): `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh`.
5. **Owner — site, if pages or the web app changed:** step 6 above (Claude builds, owner deploys).
   **Store pictures (T704), after the staging site carries the final web build, before the Play listing is updated:**
   `node app/tools/store-shots/run.mjs` (no arguments; a few minutes, headless Chrome, no credits). It re-shoots the 8 Play
   phone pictures from the staging site into `app/dist/store-shots/play-phone/` and FAILS (exit 1, `report.json` says which
   scene) when a shot's content is missing: salmon above the game-fish calendar, the forecast on the chart, more than one
   part on the river. Look at them, then upload to Play Console. A failure on shot 2 usually means the staging site is older
   than the app code: deploy the site first.
6. **Owner — app, if it changed:** `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`, then in Play Console the
   same build to Closed / Production with a staged rollout (e.g. 20 % → 100 %).
7. **Owner — watch:** the `[production]` emails (crash digest, feed health) and `/status` for a day.

## Rollback

- **Data:** `CONFIRM_PROD=canifishthis.ca deploy/prod/rollback.sh` (the previous pointer back; it checks every file it
  names is still there), then `echo <previous r-id> > data/release/LATEST`. A promote never deletes the previous release.
- **A worker** (feeds, data, site): `npx --prefix app wrangler deployments list -c <its config>`, then
  `npx --prefix app wrangler rollback <version-id> -c <its config>`. Configs: `app/workers/feeds/wrangler.prod.toml`,
  `deploy/prod/data.wrangler.toml`, `deploy/prod/site.wrangler.toml`.
- **The app:** Play Console → the release → Halt rollout; ship a fixed build (a higher versionCode). Our `play.json`
  only adds the "What's new" bullets once Play offers that build, so a halted build announces nothing.

## Not yet (tickets)

Old releases pile up in the prod bucket (a promote clears nothing): prune by hand to live + previous until a prune
script exists. `pipeline/runtiming/feed/publish.py` names only staging's bucket, so `feeds-runtiming.yml` cannot publish
to production yet; until it can, `deploy/prod/upload.sh` copies staging's run timing for the same tiles.
