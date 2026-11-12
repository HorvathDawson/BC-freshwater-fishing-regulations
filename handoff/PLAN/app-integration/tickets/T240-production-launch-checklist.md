# T240 — production launch checklist (one list, in order)

Status: OPEN — brought up to date 2026-11-11 (T540 pass: STATE table below, every item checked against the repo; the prod runbook is handoff/PLAN/release/PRODUCTION.md, T404); nothing on production is run (human-only, CONFIRM_PROD); the old section bodies are the 2026-10-29 text.

Source: owner 2026-10-28 — approved proposal P6 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").
Pulls together what T86 (hosting), T198 (SEO redirects / Search Console), T225 (feeds), T232 (environments),
handoff/PLAN/seo/staging.md "Findings (owner / prod)" and OWNER-TODO.md say prod needs. Staging is the rehearsal:
everything below was done or measured on staging.canifishthis.ca / data-staging.canifishthis.ca first.

★ = owner only (credentials, billing, DNS, accounts). Everything else Claude can prepare (configs, scripts, a dry run
against staging) and the owner runs on prod.

## STATE 2026-11-11 (T540 documentation pass: each item checked against the repo today)
How to read it: **H** = human-only (writes production, or an account/DNS/Console step; Claude never runs it, each script
refuses without `CONFIRM_PROD`). **C** = Claude can do it on staging or by a dry run (nothing written to production).
**DONE** names the proof. **NOT VERIFIED** says what would verify it. The step-by-step prod order is now
`handoff/PLAN/release/PRODUCTION.md` (T404, 124f5257c); this list is the checklist behind it. The section bodies further
down are the 2026-10-29 text, kept for their commands; where this table says "changed", the table is right.

Prod pieces built (all in git, none run on production): `deploy/prod/upload.sh` (2f09829fe), `rollback.sh`, `site.sh`,
`build-apk.sh`, `data.wrangler.toml`, `host.wrangler.toml`, `feeds.sh` (069d61d45), `sync-inputs.sh` (5325db02f),
`feeds-placeholder.sh`, `app/workers/feeds/wrangler.prod.toml` (5c7b9719d); `python -m pipeline release --prod` exists
(`pipeline/release.py`); `node app/tools/smoke-staging.mjs --production` exists (T404).

| § | item | state | who |
|---|---|---|---|
| 1 | Prod data worker code = staging's `data-worker.js` (one gzip, weak ETag, `private/` never served) | DONE in code: `deploy/prod/data.wrangler.toml` (124f5257c; the file is NOT called `data.prod.wrangler.toml`), weak-ETag compare in `deploy/staging/data-worker.js` (reads `If-None-Match`, strips `W/`) | C |
| 1 | Deploy that worker over prod's `bc-fishing-r2` | OPEN; first check the old worker's name + route (file header says why; Q174) | H |
| 1 | `index.json`/feed headers `Content-Encoding: gzip`, `Cache-Control: public, max-age=300` | DONE in `deploy/prod/upload.sh` (headers on the manifest/index uploads); the live check (`curl -I`) is open | H |
| 1 | Prod data host name | DONE: `data.canifishthis.ca` is `ORIGINS.production` in `app/apps/mobile/src/env.ts` and the route in `data.wrangler.toml` | — |
| 2 | Prod R2 credentials (wrangler login, rclone remote `r2:`) | OPEN (owner's Mac; `runbooks/secrets.md`) | H |
| 2 | Prod upload script, never clears `feeds/**`/`private/**`, `sync-inputs.sh` against prod | DONE in code: `deploy/prod/upload.sh` promotes what staging serves, refuses unless staging's live manifest equals the local one, `--bucket <non-prod> --dry-run` rehearses; running it on prod is open | C rehearse / H run |
| 2 | Every manifest file answers 200 with the right sha256 from prod | NOT VERIFIED: `upload.sh` stats each object after copying, but I did not find a sha256 compare in it or in `smoke-staging.mjs`; verify by reading `upload.sh` lines ~90-110 and running `smoke-staging.mjs --production` once prod is up | C (script) / H (run) |
| 3 | Prod feeds worker config with staging's crons | DONE: `app/workers/feeds/wrangler.prod.toml` (name `bc-fishing-feeds`, route `feeds.canifishthis.ca`, same crons; `test/config.test.ts` holds it to `wrangler.toml`); deploy `CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh` is open | H |
| 3 | Prod secrets `RUN_TOKEN`, `NOTIFY_EMAIL_TO`, `REPORT_SALT`, `GITHUB_DISPATCH_TOKEN` | changed: set on the prod PLACEHOLDER worker (`deploy/prod/feeds-placeholder.sh`, 11-05; REPORT_SALT 11-06 per `runbooks/secrets.md`); the real worker deploys over it and keeps them. NOT VERIFIED today: `wrangler secret list` on prod (owner's login) | H |
| 3 | GitHub `PROD_R2_ACCESS_KEY_ID` / `PROD_R2_SECRET_ACCESS_KEY`; un-comment `- production` in `feeds-hydat.yml` / `feeds-runtiming.yml` | OPEN: the `production` option is still commented in both workflows (checked today); secrets "not set yet" per `runbooks/secrets.md` (11-05) | H |
| 3 | One run of every job with `?dryNotify=1`, then 24 h of `/status` fresh | OPEN on prod. On staging the same proof is T597's "a week of /status"; `?dryNotify=1` exists in `app/workers/feeds/src/index.ts` | C staging / H prod |
| 4 | Retire the old prod jobs (`bc-fishing-cron` worker; `main`'s `update-hydro.yml`, `update-in-season.yml`, `update-stocking.yml`) | OPEN, changed: done the SAME DAY as the switch, no 30-day wait (owner 2026-11-08, PRODUCTION.md step 11). The workflows still exist on `main`; `cron-runner/` is on `main` (not `archive/cron-runner/`, and not on this branch) | H |
| 4 | Delete the v1 R2 layout (`stations.json`, `gauges.geojson`, `recent/`, `history/`, `climatology/`) | OPEN, changed: same day, not after 30 days (Q174) | H |
| 5 | Prod site wrangler + script | DONE 21a2d17ff, 124f5257c: `deploy/prod/site.wrangler.toml`, `site.sh`; deploy is open | H |
| 5 | `.com` / `www` hosts 301 to `https://canifishthis.ca` keeping path + query, HSTS | changed: a generated host worker (`deploy/prod/host.wrangler.toml`, 21a2d17ff; or a Cloudflare dynamic redirect rule, PRODUCTION.md step 7). Open, and the www rule dropped the path when OWNER-TODO last observed it (NOT re-checked today: `curl -sI https://www.canifishthis.ca/water/vedder-river`) | H |
| 5 | Trailing slash 301; v1 `/waterbody/*` 301s; favicon | DONE in the site worker (`run_worker_first` in `site.wrangler.toml`), proven on staging (21a2d17ff); re-check the legacy 301 on prod | C staging / H prod |
| 6 | Search Console Domain property (TXT) + Bing import; submit `/sitemap.xml` | OPEN (PRODUCTION.md step 10) | H |
| 6 | Prod sends no `X-Robots-Tag` | DONE as a check: `smoke-staging.mjs --production` fails on it; prod run open | H |
| 7 | Cloudflare managed robots.txt OFF; Bot Fight/JS detections/Web Analytics OFF on the site zone | OPEN (PRODUCTION.md step 6, owner); re-run the Lighthouse gate afterwards: T409 is PARTIAL (water pages 77-83 on staging) | H |
| 8 | `eas.json` production profile `EXPO_PUBLIC_APP_ENV=production` + `EXPO_PUBLIC_CONTACT` | DONE: `app/apps/mobile/eas.json`; a production build without the contact is refused (e45841b85) | — |
| 8 | `EXPO_PUBLIC_APP_BUILD`, no ArcGIS key, About shows no environment badge on a prod build | NOT VERIFIED: install a prod-env build on the emulator and read About (needs `deploy/prod/build-apk.sh --dry-run` first) | C |
| 8 | Web prod export points at the prod origin | DONE in the recipe (PRODUCTION.md step 6, `expo export` with `EXPO_PUBLIC_APP_ENV=production`); the artefact itself is not built today | C builds / H deploys |
| 9 | Secrets inventory | DONE: `handoff/PLAN/runbooks/secrets.md` (11-05). Still open there: GitHub `CLOUDFLARE_API_TOKEN` is read by no workflow (delete or keep) | H |
| 10 | Smoke test at prod | built: `smoke-staging.mjs --production`; run open | H |
| 10 | Rollback written | DONE: `deploy/prod/rollback.sh` + PRODUCTION.md "Rollback" (worker rollbacks, Play halt) | — |
| 11 | Map assets to `map-assets/v1`; HYDAT day flow to `feeds/hydat`; report purge | DONE in code: copied by `deploy/prod/upload.sh` (immutable cache headers; HYDAT with the clim guard, T554 item 4); purge command is in PRODUCTION.md step 5; running them is open | H |
| 11 | SEO build for prod (heavy) and the prod site deploy | changed: PRODUCTION.md step 6 (builds through `scripts/heavy.sh seo-prod`); Claude may build `app/dist/web-prod` and `app/dist/seo-prod`, the owner runs `site.sh` | C builds / H deploys |
| 11 | Privacy page and the two mailboxes | page: DONE in the SEO build (`/privacy` is a site route, `tools/seo/hosting.ts`), live on staging; on prod it exists only after the site deploy. Mailboxes `privacy@` / `support@`: NOT VERIFIED (Cloudflare Email Routing, owner) | H |
| 11 | Release promote check: one vintage (tiles, bundle, pages) | OPEN on prod; `upload.sh` promotes the whole release together and refuses a mismatched manifest | H |
| 11 | Store listing sign-off | changed: the files are now `handoff/PLAN/play/listing.md` and `handoff/PLAN/play/data-safety.md` (T371) beside `handoff/PLAN/app-integration/store/listing-draft.md` + `privacy-labels-draft.md`; owner sign-off open (T244 IN PROGRESS) | H |
| CT | Closed-testing list (below): the 0.5.0 boxes | DONE by the builds since (Play internal carries later builds: STATUS 11-11 "build 27 is out: staging and Play internal"); the box texts are historical | — |
| CT | "Closed testing needs no production hosting" | CHANGED: owner 2026-11-07 (T495) the goal is closed testing ON production, the site moved off v1 first; after the switch every Play build reads production and staging stays on the sideload page (Q173, PRODUCTION.md step 9) | — |

### Added since 2026-10-29 (each with its source ticket)
| item | source | state | who |
|---|---|---|---|
| Read `handoff/PLAN/release/PRODUCTION.md` and answer Q171-Q175 (prod report endpoint `feeds.canifishthis.ca/report`, backup bucket name, Play builds read prod, data worker replaces `bc-fishing-r2`, `canifishthis.ca` links in every environment) | T404 (IN REVIEW) | OPEN; nothing production was run | H |
| Create R2 bucket `bc-fishing-regulations-backup` with staging's lifecycle rules (daily/ 30 d, monthly/ 400 d, curated/ 365 d, restore-test/ 1 d); read the prod backup lifecycle | T404 step 2, T559 (IN REVIEW), Q172 | OPEN | H |
| Backups and rate-limit records deleted as the privacy page says; deploys check `daily/` <= 30 d | T559 (cfcdf7e07, fb5f387a3); owner Q179 11-09: shorten/purge to match | built; proof is the next staging worker deploy plus a backup run | C staging |
| Prod `sync-inputs.sh` refuses an input format the production feeds worker does not list, so deploy the prod feeds worker WITH the app, never ahead | T597 (PARTIAL, 5325db02f), PRODUCTION.md | built; staging worker deployed 11-12 per the ticket; left: a week of `/status` | C staging |
| Feeds worker audit fixes (rate limits on IPv6 /64, in-season URL pinned, no rank without the envelope, caps) go to staging first | T609 (PARTIAL), T558 (PARTIAL) | NOT VERIFIED which are deployed to staging; check `wrangler deployments list -c app/workers/feeds/wrangler.toml` against the ticket's commit list | C staging |
| Shared links open the installed app: the site must serve `/.well-known/assetlinks.json` on `canifishthis.ca` (Play app signing SHA-256 is in `@app/core` share.ts) and the APK carries the intent filter | T625 (PARTIAL, built 11-11) | OPEN: prod site deploy + a Play install to verify; a side-loaded staging APK cannot verify. Emulator proof (`adb shell pm get-app-links ca.canifishthis.app`) needs a Play-signed or locally re-signed build | C staging / H (Play install) |
| Share page "Open in the app" points at the Play listing | T514 (PARTIAL): set `OPEN_IN_APP_URL` in `app/packages/core/src/share.ts` once the listing is public (default null = web app + greyed "coming soon") | OPEN, waits for the public listing | H (listing) then C (the one value) |
| Play Console: Closed track, tester list (12 testers, 14 days), app-content forms (privacy URL, data safety, audience, ads none, content rating), requirements written down | T371 (IN PROGRESS); drafts `handoff/PLAN/play/*.md` done | OPEN, owner's Console steps (OWNER-TODO "T371") | H |
| Store screenshots, shot list + titles | T319 (OPEN, back in 11-11) | generator built, waits on the owner's shot list | H list / C generate |
| In-app update prompt so testers get fixes | T390 (PARTIAL, code merged) | left: emulator/phone checks with the next APK | C emulator |
| First real `python -m pipeline release --staging` run (build, checks, promote, staging upload + smoke) is the proof of the release path that `--prod` reuses; then `release --prod <r-id>` | T666 (IN PROGRESS), PRODUCTION.md | staging run open; prod run | C staging / H prod |
| Dry-run every prod script (`deploy/prod/upload.sh --bucket bc-fishing-regulations-staging --dry-run`, `deploy/prod/feeds.sh --dry-run`, wrangler `--dry-run`, `build-apk.sh --dry-run`, `site.sh --dry-run`) | T404 / T495 step 4, PRODUCTION.md step 1 | NOT VERIFIED: I did not run them; no recorded run found. Run once, write the date here | C |
| Prod run timing: `feeds-runtiming.yml` cannot publish to production yet (`pipeline/runtiming/feed/publish.py` names only staging's bucket); `deploy/prod/upload.sh` copies staging's run timing meanwhile | PRODUCTION.md "Not yet" | OPEN gap, a build ticket | C |
| Old releases pile up in the prod bucket (a promote clears nothing): prune by hand to live + previous until a prune script exists | PRODUCTION.md "Not yet" | OPEN | H (until scripted) |
| Per release: changelog entry in `app/packages/core/src/changelog.ts`, then `CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh` (AAB to Play internal, `play.json` to the prod bucket), promote the same build to Closed in Console | T404 step 9, T371 | OPEN | C changelog / H build + Console |
| The launch gates (T495): gate 1 T462 audit DONE 85ba2e821 (30 findings, none ticketed; owner decides); gate 2 T351 DONE, T360 PARTIAL; gate 3 T420 DONE, T401 PARTIAL (owner: not urgent); gate 4 T478, T466 DONE | T495 (OPEN) | T360 and the T462 findings are the open gate work | C |
| DFO salmon extent review before any production switch-on | T164 (OPEN, owner), T495 | OPEN | H |
| `/code-review ultra` on the whole codebase | T463 (OPEN, owner runs it, Claude triages) | OPEN | H runs / C triages |
| TestFlight / iOS | T88 (PARTIAL; script `deploy/staging/build-ios-testflight.sh`), benched until closed testing has started | OPEN, needs the Apple Developer account | H |

### Open items, split (2026-11-11)
**Claude on staging / by dry run:** dry-run the prod scripts and record the date; first real `release --staging` (T666);
a week of staging `/status` (T597); staging worker deploy of T558/T559/T609 fixes and the proof they list; T625 emulator
proof with a re-signed build; T390 emulator checks; build `web-prod` + `seo-prod` (heavy.sh, one at a time); verify a
prod-env build's About page and the manifest-sha256 check; T319 screenshot generation once the owner's list exists;
`OPEN_IN_APP_URL` once the listing is public; the prod-run-timing publish gap; T360 and T462 findings (gate work).
**Human-only (production, accounts, DNS, Console):** read PRODUCTION.md + answer Q171-Q175; backup bucket and lifecycle;
prod R2 credentials; deploy the prod data worker, run `release --prod`, deploy the prod feeds worker, uncomment the
prod workflows + add the two GitHub R2 secrets; `site.sh` deploy; `.com`/`www` redirects and zone settings; Search
Console/Bing; mailboxes; retire `bc-fishing-cron`, the three old workflows and the v1 R2 layout; delete/keep
`CLOUDFLARE_API_TOKEN`; prune old prod releases; T371 Console steps and testers; listing sign-off; T164 DFO extents;
T463 ultra review; the prod APK build; T88 Apple.

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
- [ ] ★ GitHub secrets for the prod option of `feeds-hydat.yml` / `feeds-runtiming.yml` (`CLOUDFLARE_ACCOUNT_ID`,
      prod R2 key pair) and un-comment their prod option. The worker inputs go up from the Mac only (T520):
      `CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh`.
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
- [x] Prod site wrangler (2026-11-05, 21a2d17ff): `deploy/prod/site.wrangler.toml` + `deploy/prod/site.sh` (owner runs with
      `CONFIRM_PROD=canifishthis.ca`); name `bc-fishing-regulations`, route
      `canifishthis.ca`; a prod script that does NOT apply the staging-only edits (no `Disallow: /`
      robots, no `X-Robots-Tag: noindex, nofollow`, "Open in the app" links stay absolute to prod).
- [ ] ★ Registrar/DNS: `canifishthis.com`, `www.canifishthis.com`, `www.canifishthis.ca` → **301** to
      `https://canifishthis.ca` (path + query kept; `redirects.json`); HSTS once https is confirmed everywhere.
- [x] Trailing slash on ANY path → one 301 (2026-11-05, 21a2d17ff: the site worker runs first for `/*/`; verified on staging).
- [ ] v1 legacy paths `/waterbody/*` → one 301 each (already in the site worker; re-check on prod).
- [x] Favicon (T244, 2026-10-29): `/favicon.svg` + icon link on the hub pages (SEO build, `tools/seo/favicon.ts`) and the app export's `favicon.ico`; prod already serves the v1 `favicon.svg`: keep it (same file).

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
      (version + build number), `EXPO_PUBLIC_CONTACT=support@canifishthis.ca` (owner 2026-10-29; set in eas.json's three profiles, `build-apk.sh`, the `export:web` scripts; the mailbox must exist: OWNER-TODO).
- [ ] Optional `EXPO_PUBLIC_ARCGIS_API_KEY` stays unset (owner 2026-10-21: no key).
- [ ] Web prod export points at the prod data origin (`export:web` with `EXPO_PUBLIC_APP_ENV=production`).
- [ ] The About page shows no environment badge on a production build reading production (T232 `aboutEnvVM`).

## 9. Secrets inventory (with T249)
- [x] One table (2026-11-05): `handoff/PLAN/runbooks/secrets.md` (feeds worker, GitHub, the Mac's Play keys, who
      rotates each). Found: GitHub `CLOUDFLARE_API_TOKEN` is read by no workflow (owner: delete, or keep for T404).

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
      `EXPO_PUBLIC_CONTACT=support@canifishthis.ca EXPO_PUBLIC_APP_ENV=production pnpm -C app --filter @app/mobile exec expo export --clear --platform web --output-dir ../../dist/web-prod` then
      `node app/tools/build-seo.mjs --release-dir data/release/<r-id> --app app/dist/web-prod --threads 4 --out app/dist/seo-prod` (heavy — one at a time via `scripts/heavy.sh`, ≤4 threads; T279 stores each page compressed only, `index.html.gz`/`.br`, served by the worker by Accept-Encoding; the prod site switch of `site.sh` is section 5; confirm the exact flags against the `site.sh` staging call when writing the prod switch). Check: the page's `<script src>` is in the export; `/water/<slug>` shows the pane with JS off.
- [ ] **Prod site deploy of the above** — the prod wrangler (section 5) with the pre-compressed pages (T279, when done) and the host's file-count/size headroom noted there.
- [ ] **Privacy page live (T268/T244)** — `https://canifishthis.ca/privacy` answers 200 on prod (it is part of the SEO build; linked from the footer, About, Sources, the report form). The store listings quote this URL. The mailboxes `privacy@canifishthis.ca` (privacy page) and `support@canifishthis.ca` (store contact, the app's Contact page = `EXPO_PUBLIC_CONTACT`) exist and forward to the owner's inbox.
- [ ] **Release promote check** — prod `LATEST` and the manifest the app reads name the same release as the tiles and the SEO build (one vintage: tiles, bundle and pages).
- [ ] **Store listing (T244)** — owner signs off `handoff/PLAN/app-integration/store/listing-draft.md` + `privacy-labels-draft.md`; then Play internal track (T268/T267) and TestFlight (T88). Splash + icon are in the production build's prebuild (T244).
Dependencies unchanged: sections 1-3 before the app's production build reads prod; section 4 only after.

## Acceptance
Every box ticked or marked n/a by the owner; the smoke test green on prod; the feed fresh for 24 h; the old cron off
only after the prod app reads `feeds/`.

Depends: T86 (POSTPONED — this list is its body), T198, T225, T232, T241, T249. Size: S (Claude's part) + owner time.
Owner: yes — every ★ line. Default: the defaults stated on each line.

- Launch item (owner 2026-10-31): T319 — store screenshots that sell the app, auto-generated, before the public listing.

## Scope, owner 2026-11-04
"we could work on a checklist but we arent going to production beyond a closed testing." For 0.5.0: a CLOSED-TESTING
checklist only (Play Closed track, staging data, the feeds worker on staging); the production switch-on list stays as
written for later.

## CLOSED TESTING checklist (written 2026-11-04 for 0.5.0; the Play Console side is T371)
State 2026-11-05: 0.6.2 (versionCode 11) is on Play internal; Closed testing promotes that SAME build after 2–3 clean
days (T371). The 0.5.0 boxes below were done for 0.5.0–0.6.2 (STATUS 11-05); what is left is ★ T371 and T420 if cheap.
The app a closed tester installs reads STAGING (`apps/mobile/src/env.ts`: a phone build is staging unless made with
`EXPO_PUBLIC_APP_ENV=production`), so closed testing needs no production hosting. ★ = owner.
- [ ] The one 0.5.0 rebuild promoted locally (bundle → release: T372 `n`, T376 basins, T339 names, T370 `water-shared`,
      T386 `lines` phone-held), its test pins re-cut, the request walk re-pinned (T370).
- [ ] The full app + pipeline suites once, green; `pnpm -C app check` green.
- [ ] Staging R2: the release + tiles uploaded (`deploy/staging/upload.sh`), manifest `minAppVersion` 0.5.0 (an older
      app says "Update the app"), every manifest file 200 with its sha256 (the smoke script).
- [ ] The staging feeds worker deployed WITH the app, never ahead (T347 forecast weekdays, T375 ten days, T380 peak
      percentile; T178's monthly score if ready); `GET /status` all fresh after one run of each job.
- [ ] One AAB (versionCode bumped), the same build to internal and Closed; the emulator sitting on that APK first (the
      device checks, OWNER-PHONE.md's list on the owner's phone).
- [ ] ★ T371: the Closed track, the tester list (12 testers, 14 days), the app-content forms (Claude drafts data safety
      and listing text from the code), rollout, every requirement the Console raises written down there.
- [ ] Privacy page live on staging (T352 wording) — its URL is the app-content form's privacy policy.


## Merged in (ticket audit 2026-11-05, owner)
This ticket now also carries the leftovers of: T50, T75, T80, T87 — read each file's open items and finish them here; each closes pointing here.

## Leftovers carried here (2026-11-05)
- **T50:** the gesture/back walk on a real device (back table via `adb input keyevent 4` + the edge gesture, view-swipe
  vs system edge gesture, sheet pan vs scroll, predictive back is off: `enableOnBackInvokedCallback=false`). Rides the
  owner's Closed-testing phone walk.
- **T75:** an offline native launch draws the ground map (phone). Same walk. Plus the T416 gap: a release with no
  basemap must be refused (T416 does that).
- **T80:** item 4 (the Map → Conditions flash on a tab switch: per-frame look at `applyLook` order, `packages/map`);
  item 3's two open shapes (needs a deliver); **item 6 done 2026-11-05**: CI runs on every push to the branch and has
  been RED since 10-29 — app: the spinner PNG deflates differently on CI's Node 22 (fixed: `app/.nvmrc` = the Mac's
  25.9.0, 25b07f669); pipeline: 29 failed + 27 errors, tests reading local data without a `needs_*` marker and a
  shallow clone for `git show` (fix in progress). Owner calls Q-T80a/b/c keep their built defaults.
- **T87:** the owner's sign-off walk (web + phone), recorded in decisions.md. Owner.
- **T86:** contact guard done (e45841b85: `app.config.js` refuses a production build without `EXPO_PUBLIC_CONTACT`).
  Its prod hosting is T404's (prod twins of the deploy scripts and worker configs, when prod is near).
