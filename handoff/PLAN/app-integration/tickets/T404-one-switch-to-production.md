# T404 — One switch to production, and the prod release runbook

Status: IN REVIEW — built 2026-11-07 (b8d4bec0b) on branch worktree-agent-add737f1e347ca347 (not merged); proof pending: the owner
reads handoff/PLAN/release/PRODUCTION.md and answers Q171–Q175; nothing production was run.

Source: owner 2026-11-05, on `play.json` living on staging until prod: "can we have a single switch for moving to
production later and having data access update everywhere etc... with a doc on how we will release to prod etc. like we
need a workflow for the transition. we dont need this yet but will need it eventually".

## Work
1. **One switch.** A single build-time setting (e.g. `APP_ENV=staging|production`) from which EVERY origin derives: data
   release + manifest + map assets/tiles, feeds (gauges, DFO, in-season, run timing), crash + problem reports, the app
   version file (`latest.json` / `play.json`), the synopsis PDF, the web site and SEO canonicals, test-bed defaults.
   Today "staging" is spelt in ~25 files (core/appUpdate, data/update/*, data/report/outbox, ui/crashReport,
   map/endpoints, ui-native Shell/menu, tools, deploy/staging/*). One module owns the origins; a guard test fails on a
   staging/production host spelt anywhere else. The app shows which environment it is in (About), so a tester never
   wonders.
2. **Deploy scripts per environment.** deploy/staging/* gains a production twin driven by the same switch (upload,
   feeds worker + its cron, site, build-apk `--play`), production steps stay HUMAN-ONLY (they print or ask, never run
   unattended by Claude).
3. **The runbook** (`handoff/PLAN/release/PRODUCTION.md`): the first transition (accounts, buckets, DNS, secrets, run
   token, Play production track, Search Console) and then every release: build → staging → owner checks → promote the
   same release id to prod R2 → feeds worker → `play.json` → Play rollout % → watch crash digest → rollback steps
   (previous release id, previous worker version, halt rollout). Which steps Claude may do, which are the owner's.
4. **A dry run** of the transition against a scratch "prod-like" bucket before the real one.

## Launch steps the runbook must carry (first transition)
- **www.canifishthis.ca redirect drops the path** (owner 2026-11-05: do it at the prod release). Checked 11-05:
  `www.canifishthis.ca/water/vedder-river` → 301 `https://canifishthis.ca/` (home); the .com and www.com rules already
  keep the path. Fix: Cloudflare → canifishthis.ca → Rules → Redirect Rules → the www rule → Dynamic
  `concat("https://canifishthis.ca", http.request.uri.path)`, 301, preserve query string — or deploy the generated host
  worker (`deploy/prod/host.wrangler.toml`). Check after: the curl above lands on `/water/vedder-river`.
- The SEO launch steps (prod site deploy, managed robots.txt off, Search Console + Bing, outreach) — OWNER-TODO
  "SEO / domain" and T198.

Links: T86 (production hosting + ship config), T240 (launch checklist), T198 (.com → .ca, Search Console), T371 (Play
closed testing), T234/T390 (`play.json`).
Size: M.

## Prep 2026-11-05 (owner): the prod feeds worker exists early, with its secrets
`deploy/prod/feeds-placeholder.sh` (the OWNER runs it: `CONFIRM_PROD=bc-fishing-feeds`) deploys `bc-fishing-feeds` as a
503 placeholder (no bindings, no crons) and prompts for GITHUB_DISPATCH_TOKEN, RUN_TOKEN, REPORT_SALT, NOTIFY_EMAIL_TO
(new values for the two random ones). At the switch: a production wrangler config for app/workers/feeds with
`name = "bc-fishing-feeds"` (prod bucket, email, crons, FEEDS_ENV=production) deployed over it; the secrets stay.

## Built 2026-11-07 (IN REVIEW)
The runbook: `handoff/PLAN/release/PRODUCTION.md` (first transition, every release, rollback, the www fix).

**Audit — where an environment's host / origin / bucket / worker name is spelt** (grep `canifishthis.(ca|com)`,
`*.workers.dev`, `bc-fishing-*`; 76 files before this change: 71 under app/, deploy/, pipeline/ + 5 workflows):
- **App code (app/apps, app/packages, non-test):** `apps/mobile/src/env.ts` (owner: data origins, report/crash URLs),
  `core/src/share.ts` (owner: the public site), `core/src/menu/about.ts` (PRIVACY_URL spelt + two comments → now
  `${SITE_ORIGIN}/privacy`, comments generic), `map/src/endpoints.ts` (`OUR_ASSETS` = STAGING's data host as the map's
  default → removed; `releaseTiles` / `releaseMapInput` now REQUIRE the environment's assets folder; no-assets = a bare
  path naming no host). Email addresses only (not hosts): `app.config.js`, `eas.json`, `package.json`. A comment:
  `map/src/controls.css` (v1's site). The ticket's list (core/appUpdate, data/update/*, data/report/outbox,
  ui/crashReport, ui-native Shell/menu) says the WORD staging, never a host: those all read env.ts's values.
- **Tests (excepted from the guard):** `env.test.ts`, `core/menu.test.ts`, `ui-native/DeveloperCard.test.tsx`,
  `ui-native/menu/Menu.test.tsx`, `ui/crashReport.test.ts`, `map/runtime-style.test.ts`.
- **Feeds worker (app/workers/feeds):** `wrangler.toml` (staging names), `src/email.ts` (`wrangler tail
  bc-fishing-feeds-staging` in every health email → `WORKER_NAME` var), `src/index.ts` + `src/backup.ts` (the backup
  bucket and manifest `source` spelt staging → `BUCKET_NAME` / `BACKUP_BUCKET_NAME` vars), `src/dispatch.ts` +
  `src/runtiming.ts` (User-Agent only), `src/gauges/forecast.ts` (User-Agent: the public site). New twin:
  `wrangler.prod.toml`; `test/config.test.ts` holds it to staging's (crons, bindings, limits, vars; only names differ).
  A production worker's workflow starts ask `target: production` (`dispatch.ts`), never staging's bucket.
- **Tools (app/tools, dev-only, not app code):** `smoke-staging.mjs` (now `--production` too), `bad-net.mjs`,
  `build-icons.mjs`, `play-shots.mjs`, `seo/{favicon,hosting,html,legacy-crosswalk}.*` + 2 tests (the public site and
  its redirects: production by design), `testbed/lib/{channel,device}.mjs`, `testbed/scenarios/report.mjs` (staging).
- **deploy/staging (16):** every script hardcodes staging (`upload`, `sync-inputs`, `feeds`, `site`, `build-apk`,
  `build-ios-*`, `backup`, `restore-test`, `rotate-run-token`, `check-public`, `data-worker.js` + its toml,
  `flood-test`, `test-updates`, `site.wrangler.toml`). `build-apk.sh` gained `--production` (Play only, CONFIRM_PROD).
- **deploy/prod (6 before → 11):** `feeds-placeholder.*`, `host.wrangler.toml`, `site.*` + new `upload.sh`,
  `sync-inputs.sh`, `rollback.sh`, `feeds.sh`, `build-apk.sh`, `data.wrangler.toml`.
- **Pipeline:** `release.py` (`--prod` now runs `deploy/prod/upload.sh`, the one prod upload; publish.py no longer
  in that path), `runtiming/feed/{publish,base}.py` (BUCKET staging-only — left: see below), `runtiming/feed/diff.py`
  (staging URL in usage), User-Agents in `gauges/feed/forecast.py`, `runtiming/fetch/*`, `deliver/tiles/basemap_source.py`,
  `atlas/waters/added_streams/fetch_osm.py`, `tools/{source_pins,audit/tidal_triage}.py`; `tests/test_staging_scripts_fail_safe.py`.
- **Workflows:** `feeds-hydat/runtiming/sync-inputs.yml` (production option commented out, owner enables),
  `staging-security.yml`, `staging-smoke.yml`.

**Guard:** `app/tools/no-env-hosts.test.ts` fails on a host in app/apps or app/packages (.ts/.tsx/.js/.mjs/.cjs) outside
`apps/mobile/src/env.ts` and `packages/core/src/share.ts`. Excepted exactly: `*.test.*` files, `test/` and `__tests__/`
folders, docs (.md, not walked), `android/`, `ios/`, `node_modules/`, `dist/`, dot-folders. Email addresses are not hosts.

**App:** production reports + crashes → `https://bc-fishing-feeds.horvath-dawson.workers.dev/report` (`/crash`), was
none (Q171). The version file is already per environment (`<data origin>/app/android/play.json`; `build-apk.sh
--production` writes it to the prod bucket). About already shows the environment (T232).

**Rehearsed (nothing written to production):** every prod script refuses without CONFIRM_PROD (exit 2: upload,
sync-inputs, rollback, feeds, site, build-apk, feeds-placeholder); `deploy/prod/upload.sh --bucket
bc-fishing-regulations-staging --dry-run` end to end (r-f56b90ad2540: manifest identical to staging's, run timing,
files, seeds, sync-inputs 12 objects, pointer; 0 copied); `deploy/prod/rollback.sh` for real on a LOCAL fake bucket
(rclone's local backend: rolls back; refuses a second time and a previous release with a missing file); on staging
it refuses (no previous pointer yet); `deploy/prod/feeds.sh --dry-run` (220 worker tests + bundle with prod bindings);
`wrangler deploy --dry-run` of `deploy/prod/data.wrangler.toml`; `deploy/prod/build-apk.sh --dry-run` (production
env, no switch/lab, track internal, play.json → prod bucket) and staging's `--play --dry-run` unchanged. NOT rehearsed:
`deploy/prod/site.sh --dry-run` (needs the prod web export + SEO build, heavy).

**Left:** a prune for old prod releases (a promote clears nothing; by hand until then); `pipeline/runtiming/feed/publish.py`
takes no `--bucket` (feeds-runtiming.yml can't publish to prod; upload.sh copies staging's for the same tiles);
`site.sh --dry-run` once the prod builds exist; the dry run against a scratch prod-like R2 bucket (item 4) if the
owner wants one beyond the above.

**Owner questions (README table, built with the default):** Q171 prod report/crash endpoint
(`bc-fishing-feeds.horvath-dawson.workers.dev/report`, staging's pattern); Q172 prod backup bucket
(`bc-fishing-regulations-backup`, staging's lifecycle); Q173 after the switch every Play build reads production,
staging stays on the sideload APK page (yes); Q174 the prod data worker replaces v1's `bc-fishing-r2` in place (yes,
check its name first); Q175 share links + privacy page are `canifishthis.ca` for every environment (yes).
