# T240 — production launch checklist (one list, in order)

Status: OPEN — production went live 2026-11-18/19 (old owner to-do record 2026-11-18 22:13: data worker bc-fishing-r2, backup bucket bc-fishing-regulations-backup, feeds worker on feeds.canifishthis.ca, website; STATUS 2026-11-19: data r-6258ffded676 and website 0.8.3 on production). Done since (old owner to-do 2026-11-18 ~22:58 and ~23:05): the owner's Cloudflare steps, and the switch (bc-fishing-cron schedules removed, main's three update workflows gone, deploy/prod/drop-v1.sh run: T865). The STATE table below (2026-11-11) predates that and is not re-checked. Left: the GitHub production option + PROD_R2 secrets for feeds-hydat / feeds-runtiming and the prod run-timing publish (T404); the branch structure the owner asked for 2026-11-18 ~23:05 (main / staging / feature branches, the working branch protected like main; no ticket of its own yet). The prod runbook is handoff/PLAN/release/PRODUCTION.md.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T240 | production launch checklist (updated 2026-10-29: section 11 = map-assets, HYDAT, report purge, SEO build --app, privacy page, store; data-worker fix, prod R2, feeds prod + crons, retire old cron/workflows, DNS 301s, Search Console, noindex off, CF bot/robots, secrets, smoke, env) | OPEN | — | production live 11-18/19; Cloudflare + v1 switch done 11-18 (T865); open: GitHub prod option + PROD_R2 secrets, prod run-timing publish (T404); branch structure (owner 11-18 … (detail: ticket file) |

Source: owner 2026-10-28 — approved proposal P6 in proposed-tickets-2026-10-28.md (in git history at 03be2862e) ("approved ALL 16").
Pulls together what T86 (hosting), T198 (SEO redirects / Search Console), T225 (feeds), T232 (environments),
handoff/PLAN/seo/MEASURE.md "Findings for production" and the old owner to-do page say prod needs.
Scope, owner 2026-11-04 (superseded by the 11-18/19 launch): "we could work on a checklist but we arent going to
production beyond a closed testing."

## What
Production is live. The prod runbook is `handoff/PLAN/release/PRODUCTION.md` (T404) with the steps in
`handoff/PLAN/runbooks/release-by-hand.md`; this list is only what the launch left or never verified.
H (★) = human-only (writes production, an account/DNS/Console step; each script refuses without `CONFIRM_PROD`).
C = Claude, on staging or by a dry run.

Done: the first transition 2026-11-18/19 (buckets, data worker, www redirect, search engines, retiring v1:
`git show a3ca419e2:handoff/PLAN/release/PRODUCTION.md`); the switch (T865: bc-fishing-cron schedules removed, main's
three update workflows gone, `deploy/prod/drop-v1.sh`); old-release pruning is automatic (`prod_prune`, T865). Prod
pieces built: `deploy/prod/upload.sh` (2f09829fe), `rollback.sh`, `site.sh` + `site.wrangler.toml` (21a2d17ff),
`build-apk.sh`, `data.wrangler.toml` + `host.wrangler.toml` (124f5257c), `feeds.sh` (069d61d45), `sync-inputs.sh`
(5325db02f), `feeds-placeholder.sh`, `app/workers/feeds/wrangler.prod.toml` (5c7b9719d), `python -m pipeline release
--prod`, `smoke-staging.mjs --production`; contact guard (e45841b85); secrets inventory `handoff/PLAN/runbooks/secrets.md`.
The full 2026-11-11 STATE table, the 2026-10-29 section bodies (§1–§11, their commands) and the 0.5.0 closed-testing
checklist: `git show bd52b3ef0:<this file>` (this ticket before the T1081 distil).

## Work (left, from the 2026-11-11 STATE table; not re-checked since the launch)
| § | item | state | who |
|---|---|---|---|
| 3 | GitHub `PROD_R2_ACCESS_KEY_ID` / `PROD_R2_SECRET_ACCESS_KEY`; un-comment `- production` in `feeds-hydat.yml` / `feeds-runtiming.yml` | OPEN (PRODUCTION.md "Not yet"); until then the prod worker's HYDAT / run-timing starts fail by email | H |
| 3 | Prod run timing: `pipeline/runtiming/feed/publish.py` names only staging's bucket; `deploy/prod/upload.sh` copies staging's run timing meanwhile | OPEN gap, a build ticket (T404) | C |
| — | Branch structure the owner asked 2026-11-18 ~23:05 (main / staging / feature branches, the working branch protected like main) | see T995 (PARTIAL, `main` = trunk since 2026-11-22) | C |
| 1 | `curl -I` a release file (one gzip, right `Content-Type`), a 304 on a repeat with the weak ETag, `private/` not served; `index.json`/feed headers `Content-Encoding: gzip`, `Cache-Control: public, max-age=300` | live check open | H |
| 2 | Every manifest file answers 200 with the right sha256 from prod | NOT VERIFIED: no sha256 compare found in `upload.sh` (stats each object, lines ~90-110) or `smoke-staging.mjs`; run `smoke-staging.mjs --production` | C (script) / H (run) |
| 3 | Prod secrets `RUN_TOKEN`, `NOTIFY_EMAIL_TO`, `REPORT_SALT`, `GITHUB_DISPATCH_TOKEN` (set on the placeholder 11-05/11-06) | NOT VERIFIED: `wrangler secret list` on prod (owner's login) | H |
| 3 | One run of every job with `?dryNotify=1`, then 24 h of `/status` fresh, gauge feed < 2 h old | OPEN on prod (staging proof: T597's "a week of /status") | C staging / H prod |
| 5 | `www` keeps path + query (`curl -sI https://www.canifishthis.ca/water/vedder-river`); v1 `/waterbody/*` legacy 301 on prod; HSTS | re-check | H |
| 7 | Cloudflare managed robots.txt OFF (it is PREPENDED to ours); Bot Fight / JS detections / Web Analytics OFF on the site zone (~2.6 s of script per page) | OPEN, owner; then re-run the Lighthouse gate (T409 PARTIAL, water pages 77-83 on staging) | H |
| 8 | `EXPO_PUBLIC_APP_BUILD`, no ArcGIS key, About shows no environment badge on a prod build | NOT VERIFIED: a prod-env build on the emulator (`deploy/prod/build-apk.sh --dry-run` first) | C |
| 9 | GitHub `CLOUDFLARE_API_TOKEN` is read by no workflow | delete or keep (`runbooks/secrets.md`) | H |
| 11 | Mailboxes `privacy@` / `support@canifishthis.ca` forward to the owner | NOT VERIFIED (Cloudflare Email Routing) | H |
| — | Dry-run every prod script (`upload.sh --bucket bc-fishing-regulations-staging --dry-run`, `feeds.sh --dry-run`, wrangler `--dry-run`, `build-apk.sh --dry-run`, `site.sh --dry-run`) | NOT VERIFIED, no recorded run; run once, write the date here | C |
| — | Feeds worker audit fixes T609 / T558, T559 retention (proof: next staging worker deploy + a backup run), T597 a week of `/status` | check `wrangler deployments list -c app/workers/feeds/wrangler.toml` against each ticket's commits | C staging |
| — | Shared links open the app: `/.well-known/assetlinks.json` on `canifishthis.ca` + the intent filter (T625) | OPEN: needs a Play install (`adb shell pm get-app-links ca.canifishthis.app` needs a Play-signed or re-signed build) | C / H |
| — | `OPEN_IN_APP_URL` in `app/packages/core/src/share.ts` (T514) | waits for the public listing | H then C |
| — | T371 Play Console steps; T319 store screenshots; T390 update-prompt emulator checks; T495 gate work (T360, the T462 findings); T463 `/code-review ultra` | in those tickets | — |

## Acceptance
Every box ticked or marked n/a by the owner; the smoke test green on prod; the feed fresh for 24 h; the old cron off
only after the prod app reads `feeds/`.

Depends: T86 (POSTPONED — this list is its body), T198, T225, T232, T241, T249. Size: S (Claude's part) + owner time.
Owner: yes — every ★ line. Default: the defaults stated on each line.

- Launch item (owner 2026-10-31): T319 — store screenshots that sell the app, auto-generated, before the public listing.

## Leftovers carried here (ticket audit 2026-11-05, owner: T50, T75, T80, T87, T86 close pointing here)
- **T50:** the gesture/back walk on a real device (back table via `adb input keyevent 4` + the edge gesture, view-swipe
  vs system edge gesture, sheet pan vs scroll, predictive back is off: `enableOnBackInvokedCallback=false`). Rides the
  owner's Closed-testing phone walk.
- **T75:** an offline native launch draws the ground map (phone). Same walk. Plus the T416 gap: a release with no
  basemap must be refused (T416 does that).
- **T80:** item 4 (the Map → Conditions flash on a tab switch: per-frame look at `applyLook` order, `packages/map`);
  item 3's two open shapes (needs a deliver). Done: item 6, CI red since 10-29 (`app/.nvmrc` 25.9.0, 25b07f669). Owner
  calls Q-T80a/b/c keep their built defaults.
- **T87:** the owner's sign-off walk (web + phone), recorded in decisions.md. Owner.
- **T86:** done: contact guard (e45841b85). Its prod hosting is T404's.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
production live 11-18/19; Cloudflare + v1 switch done 11-18 (T865); open: GitHub prod option + PROD_R2 secrets, prod run-timing publish (T404); branch structure (owner 11-18 23:05); STATE table of 11-11 not re-checked
