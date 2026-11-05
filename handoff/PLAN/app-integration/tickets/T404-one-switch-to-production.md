# T404 — One switch to production, and the prod release runbook

Status: OPEN — not needed yet; do with T86/T240 when production is near (owner 2026-11-05).

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
