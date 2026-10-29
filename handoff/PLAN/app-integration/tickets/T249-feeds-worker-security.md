# T249 — feeds / worker security

Status: DONE — ec26ed584 (2026-10-29)

Source: owner 2026-10-28 — approved proposal P15 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Rate limits on every public worker route (feeds worker `/run/*`, `/notify/*`, data worker, future report/crash
endpoints): Cloudflare rate-limiting rules or a Durable-Object/KV counter; run-token rotation procedure (documented,
tested: old token refused); a check (CI or the feeds worker's health job) that no `private/` object is ever served by
the data worker; secrets inventory (names, where set, who rotates).
## Acceptance
A request flood gets 429; GET data-staging/private/... is 404/403 (test); rotation runbook executed once on staging.
Depends: T225. Size: S. Owner: yes — Cloudflare rules on the zone (default: Claude writes them, owner applies on prod).

## Outcome (ec26ed584)
- Rate limits (Workers Rate Limiting bindings, per IP, approximate — synced within a Cloudflare location, fail-open):
  feeds worker `RL_PUBLIC` 60/min (status + everything unknown), `RL_AUTH` 10/min on `/run/*`, `/notify/*`, `/reports`
  checked BEFORE the token; T237's `REPORT_LIMITER` + daily cap still guard `/report`. Data worker `RL` 300/10 s
  (a map session pans at a few hundred tile requests a MINUTE). 429 + `Retry-After: 60`.
- `deploy/staging/flood-test.mjs` on staging: feeds 404-path GETs → 429 from #256; data OPTIONS → 429 from #1206
  (approximate by design). Tests: `test/guard.test.ts` (flood → 429, other IP fine, auth limited before token, binding
  absent/throwing = through), `app/tools/data-worker.test.ts`.
- Token: constant-time compare (`src/guard.ts`). `deploy/staging/rotate-run-token.sh` (new 48-hex secret → wrangler
  secret → `.run-token` → verifies new 200 / old 403 on `POST /notify/preview`). EXECUTED on staging 2026-10-29: new 200,
  old 403. Readers of the token: only `.run-token` (curation-review reports.py, the owner's curl).
- private/ never public: data worker 404s every spelling (`%2F`, `//`, `./`, `PRIVATE/`, `..`, bare prefix).
  `deploy/staging/check-public.mjs`: all 40 private keys + 15 spellings → 404; 459 public text objects scanned for the
  R2 key pair, RUN_TOKEN, reporter emails and secret shapes (names printed, never values); `--site-dir` scans the built
  site. PASS on staging; a planted value is caught. Workflow `staging-security.yml` (manual + daily once on main).
### Secrets inventory (names only)
| Name | Where set | Used by | Rotates |
|---|---|---|---|
| `RUN_TOKEN` | wrangler secret, bc-fishing-feeds-staging; local `app/workers/feeds/.run-token` (git-ignored, 600) | /run/*, /notify/*, /reports; curation-review reports tab | Claude/owner: `rotate-run-token.sh` |
| `NOTIFY_EMAIL_TO` | wrangler secret, feeds worker | owner emails | owner |
| `REPORT_SALT` | wrangler secret, feeds worker (T237) | IP hash for report limits | owner (rotating resets the daily caps only) |
| `CLOUDFLARE_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | GitHub Actions secrets | feeds-*.yml, staging-security.yml | owner (Cloudflare R2 API tokens) |
| `PROD_R2_ACCESS_KEY_ID`, `PROD_R2_SECRET_ACCESS_KEY` | named (commented) in feeds-*.yml; set by owner for prod | prod only | owner |
| `CLOUDFLARE_API_TOKEN` | GitHub Actions secret (2026-03) | no workflow references it | owner: delete or document |
| rclone `r2:` key pair | `~/.config/rclone/rclone.conf` (Mac) | upload.sh, sync-inputs.sh, backup/restore | owner |
| wrangler OAuth login | `~/Library/Preferences/.wrangler/` (Mac) | every deploy | per login |
| data / site workers | none | — | — |
- Owner (prod, T240): the static site has no worker in front (assets) — a zone WAF rate-limiting rule is the only limit
  there; prod workers get the same `[[ratelimits]]` + a fresh RUN_TOKEN.
