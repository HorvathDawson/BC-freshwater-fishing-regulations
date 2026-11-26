# T404 — One switch to production, and the prod release runbook

Status: IN PROGRESS — 124f5257c, 4f4b5c827, d63416d48; owner part done; --bucket on runtiming publish/base merged; left (owner): uncomment the workflow production option + add the PROD_R2 secrets. (Earlier note: IN PROGRESS — 124f5257c (merged 2026-11-07). The owner's part is done: Q171–Q175 answered (tickets/README; Q172 in old owner to-do batch 13) and production was run from PRODUCTION.md on 2026-11-18/19 (old owner to-do record 2026-11-18 22:13; STATUS 2026-11-19 evening); the prod prune landed with T865 (4f4b5c827). `--bucket` on `runtiming/feed/publish.py` + `base.py` built by lane L3 2026-11-20 (d63416d48; the workflow's production option stays commented). left (Claude): main chat review + merge; then the owner uncomments the workflow's production option and adds the PROD_R2 secrets. Earlier: IN REVIEW — built 2026-11-07 (b8d4bec0b) on branch worktree-agent-add737f1e347ca347 (not merged); proof pending: the owner reads handoff/PLAN/release/PRODUCTION.md and answers Q171–Q175; nothing production was run.)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T404 | One switch to production (every origin from one setting) + the prod release runbook and transition workflow | IN PROGRESS — left (owner): uncomment the workflow production option + add the PROD_R2 secrets | 124f5257c, 4f4b5c827, d63416d48 | owner then uncomments the workflow's production option + PROD_R2 secrets |

Source: owner 2026-11-05, on `play.json` living on staging until prod: "can we have a single switch for moving to
production later and having data access update everywhere etc... with a doc on how we will release to prod etc. like we
need a workflow for the transition. we dont need this yet but will need it eventually".

## What
A single build-time setting from which EVERY origin derives (data release + manifest + map assets/tiles, feeds, crash +
problem reports, the app version file, the synopsis PDF, the web site and SEO canonicals, test-bed defaults), per-
environment deploy scripts (production steps HUMAN-ONLY: they print or ask, never run unattended by Claude), and the
runbook `handoff/PLAN/release/PRODUCTION.md` (first transition, every release, rollback, the www fix).

Done: runbook + switch + guard (124f5257c, 2026-11-07); prod run from PRODUCTION.md 2026-11-18/19; prod prune with T865
(4f4b5c827); `--bucket` on `runtiming/feed/publish.py` + `base.py` (d63416d48, L3 2026-11-20; production bucket needs
`CONFIRM_PROD=canifishthis.ca`, shared `publish.checked_bucket`; tests `test_t404_publish_honours_the_bucket`,
`test_t404_the_production_bucket_needs_confirm_prod`). Q171–Q175 answered (Q171 prod report/crash endpoint
`bc-fishing-feeds.horvath-dawson.workers.dev/report`; Q172 prod backup bucket `bc-fishing-regulations-backup`).

Where it lives: origins in `apps/mobile/src/env.ts` and `packages/core/src/share.ts` only; guard
`app/tools/no-env-hosts.test.ts` (fails on a host in app/apps or app/packages .ts/.tsx/.js/.mjs/.cjs elsewhere; excepted
exactly: `*.test.*` files, `test/` and `__tests__/` folders, docs (.md, not walked), `android/`, `ios/`,
`node_modules/`, `dist/`, dot-folders; email addresses are not hosts). Feeds worker twin `app/workers/feeds/wrangler.prod.toml`
(`test/config.test.ts` holds it to staging's; only names differ). `deploy/prod/`: `feeds-placeholder.*`,
`host.wrangler.toml`, `site.*`, `upload.sh`, `sync-inputs.sh`, `rollback.sh`, `feeds.sh`, `build-apk.sh`,
`data.wrangler.toml`; every one refuses without CONFIRM_PROD (exit 2). `release.py --prod` runs `deploy/prod/upload.sh`.

## Work (left)
1. **The owner's edits** (also in the `feeds-runtiming.yml` header): (1) uncomment `- production` in the `target`
   options; (2) swap the two `AWS_*` env lines for the two commented prod lines; (3) add the secrets
   `PROD_R2_ACCESS_KEY_ID` / `PROD_R2_SECRET_ACCESS_KEY` (an R2 token scoped to `bc-fishing-regulations`). A production
   run also needs the production release live and its binding base at
   `bc-fishing-regulations/private/runtiming/base/<handles>.json.gz` (`deploy/prod/upload.sh` puts it there). The
   workflow already derives the guard, `DATA` (production: `https://data.canifishthis.ca`) and `CONFIRM_PROD` from `target`.
2. NOT rehearsed: `deploy/prod/site.sh --dry-run` (needs the prod web export + SEO build, heavy), once the prod builds
   exist; the dry run against a scratch prod-like R2 bucket only if the owner wants one.
3. Launch steps the runbook carries (first transition): **www.canifishthis.ca redirect drops the path** (owner
   2026-11-05: do it at the prod release). Checked 11-05: `www.canifishthis.ca/water/vedder-river` → 301
   `https://canifishthis.ca/` (home); the .com and www.com rules already keep the path. Fix: Cloudflare →
   canifishthis.ca → Rules → Redirect Rules → the www rule → Dynamic `concat("https://canifishthis.ca",
   http.request.uri.path)`, 301, preserve query string — or deploy the generated host worker
   (`deploy/prod/host.wrangler.toml`). Check after: the curl above lands on `/water/vedder-river`. The SEO launch steps
   (prod site deploy, managed robots.txt off, Search Console + Bing, outreach) — T198.

Links: T86 (production hosting + ship config), T240 (launch checklist), T198 (.com → .ca, Search Console), T371 (Play
closed testing), T234/T390 (`play.json`).
Size: M.

## Owner

- [ ] 2026-11-20 — Add the `PROD_R2_*` secrets in GitHub and uncomment the production option of the run-timing workflow (the owner part left of this switch).
- [x] 2026-11-08 — After the switch every Play build reads production; share links and the privacy page are `canifishthis.ca` in every environment; the prod data worker replaces v1's in place → "yes" to each (Q173-Q175, 2026-11-08).
