# T225 — Feeds: where they live, how they are triggered, how they fail (design, then build)

Status: OPEN — ported to TS + measured on staging (see "Port + measurements"); prod switch-on is the owner's. Was: design first; owner picks the host (owner 2026-10-28: "how to make the feeds work the best as possible,
where they should live, how they are triggered etc").

Source: owner 2026-10-28. Today every feed runs only when someone runs it on this Mac; OWNER-TODO carries "set up the
feed cron + R2 sync" for the gauges (T177) and DFO salmon (T211 part 2), plus `FEED_NOTIFY_URL` and
`scripts/update-hydat.sh`.

## What the feeds are today
| feed | code | output → served at | cadence wanted | notes |
|---|---|---|---|---|
| gauges (live flow/level, change, forecast, climatology) | `pipeline/gauges/feed/publish.py` (+ change/forecast/skill) | `data/generated/gauges/feeds/` → R2 `feeds/live/` | hourly-ish (ECCC realtime) | upload only `feeds/`, never `feeds.runs/` (T177) |
| DFO salmon (openings/closures) | `python -m pipeline.regs.dfo_salmon.live` (~15 s, no secrets) | `data/generated/regs/dfo_salmon/feed/` → R2 `feeds/dfo/` | 6 h Jun 1–Oct 31, daily otherwise (Q145) | never sync `feed.notified.json`; held changes → `live queue` + review app |
| HYDAT (historical archive) | `scripts/update-hydat.sh` | local db → climatology | on each new HYDAT release (few/yr) | `feed/notify.py` messages on a new release |
| (later) run timing, stocking | T199 / T200 | — | yearly / seasonal | last band |

The app already reads the feeds offline-first: the last captured feed is shown when offline (owner 10-27), with its age.

## Work
1. **Host options** — compare on cost (target free), reliability, secrets, Python + data size, ops effort:
   - GitHub Actions scheduled workflows (free minutes; Python native; cron ≥5 min, can be delayed; needs the code
     public or minutes budget; R2 via S3 keys as repo secrets).
   - Cloudflare Worker Cron Triggers (free; JS only → port the fetch/parse, or a Worker that calls a container;
     Cloudflare Containers/Workflows if Python must stay).
   - A small always-on box (home server / cheap VPS): runs the existing Python as is; we own uptime.
   - Hybrid: light, frequent feeds (gauges, DFO) where they're cheapest; heavy, rare jobs (HYDAT) stay manual.
2. **Triggering**: schedule per feed (season-aware for DFO), jitter, back-off on source errors, skip-if-unchanged
   (ETag/hash) so R2 writes and app downloads stay small, a manual "run now".
3. **Publishing**: atomic write to R2 (upload new, then flip `index.json`), cache headers (short max-age +
   stale-while-revalidate), CORS for the web origin, versioned names for big files, cleanup of old runs.
4. **Failure + alerting**: `FEED_NOTIFY_URL` (ntfy/Discord/Slack) on held changes, parse failures, two failed fetches,
   staleness past N× cadence; a status JSON the app (and the owner) can read; the app's stale badge thresholds.
5. **Human-in-the-loop**: DFO held changes (new/reworded places) → review queue; never auto-publish a change the
   safety rules hold.
6. **Staging first**: run against `bc-fishing-regulations-staging` (`feeds/`), then prod (prod R2 writes = owner).
7. Docs: one runbook (where it runs, how to pause, rotate keys, rerun, read alerts); remove the manual feed items from
   OWNER-TODO once automated.

## Owner questions (each with a default)
- Q-T225a host: **DECIDED owner 2026-10-28 — Cloudflare Workers Cron Triggers** (GitHub Actions schedules judged
  too flaky). HYDAT stays manual on notify. Owner 2026-10-28: the
  owner HAS the paid Workers plan; feeds are request-based, so **port fetch + parse to TypeScript** (shares the app's
  toolchain) rather than Python Workers. Still measure CPU per run against the paid limits; Workflows for retries.
- Q-T225b alerts: default **inside Cloudflare** — the Worker posts to ntfy/Discord via fetch, and/or Email Workers
  `send_email` to the owner's verified address; a `status.json` in R2 + Cloudflare's own Worker error notifications.
- Q-T225c gauge cadence: default hourly in daylight, 3-hourly at night.

## Port + measurements (2026-10-28)
Owner 10-28: "convert the feeds to javascript and check what the compute time for cloudflare would be". Done for both
live feeds; HYDAT/climatology stays Python (clim.json is an input the worker reads). **Nothing runs unattended**: the
staging worker has no `[triggers]`; prod is untouched.

**Code** — `app/workers/feeds/` (own package, outside the pnpm workspace; resolves deps from app/node_modules; no new npm deps):
- `src/gauges/` — publish.py + main()'s orchestration, forecast, change, liveness, skill.append_runs; `runGauges(deps)`.
- `src/dfo/` — live.run() and its run path (fetch, parse, untangle, locations, typed, feed, entries.reconcile, header,
  deliver.salmon, names); a port of Python's html.parser + bs4's tree builder (not parse5: bs4 closes nothing implicitly),
  difflib.SequenceMatcher (autojunk), Python regex/round/repr, Python-compatible json.dumps. `runDfo(deps)`.
- `src/io.ts` (Fetch/Store/Meter, r2Store), `src/notify.ts` (send_due), `src/index.ts` (the Worker: `POST /run/gauges|dfo`
  with a bearer `RUN_TOKEN`, `scheduled` wired but no cron), `wrangler.toml` (STAGING: `bc-fishing-feeds-staging`).
- R2 layout: published `feeds/live/`, `feeds/dfo/`; private `private/feeds/gauges/` (roster.json, notified.json,
  runs/<YYYY-MM>.jsonl) and `private/feeds/dfo/` (entries/region-<s>.json, notified.json, manifest, raw/, history/).
  The staging data worker now 404s `private/` (deployed); **prod's data worker needs the same line before switch-on**.
- Inputs that left the repo: the roster (slim `[{station, realtime}]`, 90 KB) and the DFO entry files (585 KB) are R2
  private keys, so a curator's rewording reaches the app next run with no redeploy — **needs a sync step** (script or CI on
  commit to `data/curated/regulations/entries/dfo_salmon/`); uploaded by hand to staging today.

**Parity** (Python vs TS on recorded inputs; `vitest run --root app/workers/feeds`, 19 tests):
| feed | fixtures | result |
|---|---|---|
| gauges | 22 stations committed (level-only, temp, CLEVER/COFFEE/ELF, archive refill, 404, silent station, drift, hydat notice) + full 438-station recording (local, gitignored) | 0 semantic diffs. Bytes differ only by: integral floats (`5.0` vs `5`), numeric-looking key order, `notified.json` spacing, and 19 last-digit values (≤1e-12 rel, V8 vs libm exp/log in `combine_day`) |
| DFO | 6 scenarios / 68 runs: fresh, prev-state, fetch-fail ×2 (stale + notify), parse-fail (held), curation variants (section_moved, drift, new_water, structural holds), all 53 history snapshots replayed; + 62 real pages and 2,300 fuzzed pages for typed rules | **byte-identical** index.json, unplaced.json, notices |

**Measured** — staging worker invoked by hand, CPU from `wrangler tail` (Cloudflare's billed cpuTime); local = node 25 with fixtures in memory.
| run | CF CPU ms | CF wall s | local CPU ms | fetch subreq | R2 A / B | MB from source / R2 read / R2 written |
|---|---|---|---|---|---|---|
| gauges, all 438, morning forecast reissue (worst seen) | 4,996 | 79.6 | 840–970 | 935 | 436 / 480 | 23.5 / 25.7 / 18.2 |
| gauges, all 438, steady state (×2) | 2,377–3,813 | 45–46 | ~850 | 485–491 | 440 / 481 | 21.1 / 26.9 / 18.1 |
| DFO, all 9 regions, page changed | 1,325 | 10.9 | 175–600 | 9 | 13 / 12 | 0.25 / 0.66 / 0.34 |
| DFO, all 9 regions, unchanged (×3) | 329–1,078 | 4.2–6.0 | 102 | 9 | 3 / 12 | 0.25 / 0.67 / 0.07 |
| DFO, region 6 only | 213 | 2.8 | 92 | 1 | 3 / 4 | 0.06 / 0.35 / 0.07 |
Worker bundle 240 KiB (73 KiB gzip), startup 166 ms. Peak heap (node, full gauge run) ~70 MB of the 128 MB isolate; clim.json
(8 MB) is GET + JSON.parse every run (~25–30 ms). Cloudflare CPU is 3–6× node's (cold isolate JIT + GC are billed).

**Cost** (Cloudflare published pricing, read 2026-10-28: Workers Paid $5/mo incl. 10M requests + 30M CPU-ms, then $0.30/M
and $0.02/M CPU-ms; R2 $0.015/GB-mo, class A $4.50/M, class B $0.36/M, free 10 GB + 1M A + 10M B per month, egress free;
DeleteObject free):
| cadence | runs/mo | CPU-ms/mo (at worst measured) | R2 class A / B per mo |
|---|---|---|---|
| gauges hourly 06–22 + 3-hourly night (~19/day) | ~580 | 2.9M | 255k / 280k |
| gauges hourly all day (bound) | 720 | 3.6M | 317k / 346k |
| gauges every 30 min (bound) | 1,440 | 7.2M | 634k / 693k |
| DFO 6-hourly Jun–Oct / daily otherwise | 124 / 31 | 0.16M / 0.04M | ≤1.6k / 1.5k |
**Every cadence fits inside the $5 plan's included CPU (≤25%) and requests: $0 extra compute.** R2 ops fit the monthly
free tier (gauges ~25–65% of the 1M class A) — the free tier is account-wide, so the app's read traffic (out of scope)
shares it; past it, the hourly gauge writes would cost ~$1.4/mo. Storage: ~26 MB live + the runs archive (~8 MB/month,
14 months kept) ≈ 0.15 GB → cents.

**Limits checked** (Workers Paid, docs read 2026-10-28):
- CPU per cron invocation: 30 s for an interval < 1 h, 15 min for ≥ 1 h. Worst gauge run 5.0 s → fine at hourly, and still
  6× headroom at 30-min. No fan-out needed.
- Subrequests: 10,000 per invocation by default (up to 10M), and **R2 binding calls count**. Gauges: ~940 R2 + 485–964
  fetches = 1,400–1,900; worst theoretical (series cap 1,200 + archive refill for every station + temp retries) ≈ 3,000.
  Under the limit; the old 1,000 cap would have been hit, so don't drop below the default.
- Memory 128 MB: the tightest. Full gauge run ~70 MB peak (already slimmed: station files cached slim, clim trimmed to the
  asked stations). Design answer if it grows: split the forecast-series refresh into its own cron, or shard stations
  across two invocations (Queues not needed at this size).
- 6 simultaneous outgoing connections (R2 included): the 8-wide pools queue at 6; wall 45–80 s against a 15-min cron limit.
- Cron Triggers: 3 needed (gauges day, gauges night, DFO; season via the cron's month field) of 250/account.

**Before switch-on** (staging week first, then prod = owner): `[triggers]` crons; the entries/roster sync step (workflow
written, secrets to set); prod data worker 404 on `private/`; `FEED_NOTIFY_URL` as a worker secret; status.json + alerting
(DONE, follow-up below); retention needs `delete` (added to r2Store). Python findings (FIXED in the TS by the follow-up below; the Python is unchanged): `forecastIssuedAt` written twice in index.json (T52's value
overwrites T68's); `temp_history.json` is published though private-ish; the runs archive month file is rewritten whole
each run (~8 MB late month — per-run objects would cut R2 bytes); DFO `dates._SEP` misses "October 1 to …" (row → advisory);
one bad DFO row holds a whole region; held text embeds pydantic 2.13's message. DFO re-types every page every run (as the
Python); caching by page sha256 would halve its CPU.

## Follow-up 2026-10-28 (owner): robustness, status, change-aware runs, CI jobs
Redeployed `bc-fishing-feeds-staging` only (version f5c73ac9); both feeds run by hand twice; `vitest run --root
app/workers/feeds` 86 tests. The Python is unchanged; the TS now intentionally differs where listed, and the parity tests
run the Python-compatible mode (`pythonParity: true` / `changeAware: false`) or fold the intended difference back and check it.
- **DFO dates** (`src/dfo/dates.ts`): a new reader — "until further notice" (145 corpus rows; the start runs on, past the
  year end, but any dated row outranks it), "To"/"until"/"through"/dashes, day-month ("1 Oct"), ordinals, years, typos
  ("Aprl", "futher", "Setpember"), lists of ranges, ranges across the year end. All 281 distinct cells DFO has printed
  (`test/fixtures/dfo/date-cells.json`, from cache/ + fixtures) read; it agrees with the Python on every cell the Python
  could read; 4,000 seeded rewritings read the same; 5,000 garbage cells never throw. A cell it cannot read keeps the
  row's limit and becomes a `check` verdict on every day for that species ("Dates unparsed — check DFO: …") — the
  Python made it an advisory and dropped the limit. Committed corpus: check-verdict days 11,770 → 6,646 (the rest are
  "To be determined").
- **One bad row** holds only its location: last answers carried (or `check` when there are none), listed in
  `regions.<s>.holds` with the reason, notice `dfo-row-held:…`; the rest of the region publishes. Page parse/verify
  failures and STRUCTURAL reconciles still hold the region.
- **Gauges**: `forecastIssuedAt` written once (T68's newest run feed-wide; Python's second key — T52's — overwrote it);
  `temp_history.json` moved to private (a legacy public copy is migrated, then deleted — done on staging); the runs
  archive is append-only: one `runs/<m>/<d>/<HHMMSS>.jsonl` per run + `runs/state.json` (dedupe: last 8 issue times per
  station+model, seeded once from the legacy month files), closed days → `runs/<m>/<d>.jsonl`, closed months →
  `runs/<m>.jsonl` (byte-identical to Python's month file). **The Python scoring CLI's copy step must fold the open
  month's day/part objects** (concatenate in key order).
- **Status**: `GET /status` (HTML, ~2 KB) and `GET /status.json`, no auth, from `private/feeds/status.json` written by every
  run: last run (time, wall ms, counts, R2 ops, skipped writes, unchanged sources), last success, last error, held
  regions/places with reasons, consecutive failures, state ok/degraded/failing/stale (stale = no success for 2.5×
  cadence: gauges 60 min, DFO 360 min Jun–Oct else 1440). CPU is not readable inside a Worker → `cpuMs: null` (tail /
  observability). **Alerts**: `FEED_NOTIFY_URL` (not set) gets one post per feed state change; unset → logged and recorded.
  Staleness is evaluated when either feed runs (a dead cron is seen by the other feed's run; Cloudflare's own error
  notifications cover "nothing runs").
- **Change-aware** (owner add-on): DFO — a region whose page sha256 AND entry file hash are unchanged (and `DFO_REV`) is
  not re-typed; its answers are carried (verdict/gear ids now numbered canonically so carried = recomputed, byte for byte);
  feed objects are written only when their stable content changes (index.json ignores `at`/`checked`; 24 h heartbeat so
  the app's "checked" age stays under a day — status.json carries the real checked-at). Gauges — station CSVs are asked
  with `If-None-Match`/`If-Modified-Since`; a 304 station with the same day/pentad/clim/forecast inputs keeps its last
  index entry (temperature recomputed), no parse, no station-file read or write (`private/feeds/gauges/stations.cache.json`).
  A station with forecast runs is always recomputed (its `ahead` is read at the minute) — 377–430 of 438 have runs.
- **Measured** (staging, CF cpuTime from `wrangler tail`, two back-to-back runs):
  | run | CF CPU ms | wall s | R2 A / B | skipped writes / unchanged sources |
  |---|---|---|---|---|
  | DFO 9 regions, first on new code | 1,017 | 7.6 | 5 / 14 | 0 / 0 |
  | DFO 9 regions, pages unchanged | **157** | 5.9 | **1** / 14 | 2 / 9 |
  | gauges 438, first | 2,396 | 48.4 | 442 / 484 | 0 / 0 |
  | gauges 438, CSVs unchanged (best case) | 2,302 | 41.3 | 397 / 436 | 0 / 45 |
  Node (fixtures, `parity/measure-change.ts`): DFO unchanged run 113–159 → 18–19 ms CPU, 2 → 0 feed writes; gauges
  623–691 → 612–645 ms. **Cost update**: DFO's CPU at every cadence drops ~85% (≤0.03M CPU-ms/mo) and its class A to
  ~2/run (manifest + status). Gauges barely move: at an hourly cadence ECCC's CSVs change every hour and most stations
  carry forecasts, so ~0–10% of station writes are skipped (≤45 of 442 class A/run); the table above (≤3.6M CPU-ms,
  ≤320k class A per month hourly) stands. Each run now also writes `status.json` (+1 class A). The real gauge saving would
  need a coarser `ahead` clock (owner's call; it changes the feed).
- **CI** (manual `workflow_dispatch` only, staging-only guard, prod option commented out, secrets named not set:
  `CLOUDFLARE_ACCOUNT_ID`, `STAGING_R2_ACCESS_KEY_ID`, `STAGING_R2_SECRET_ACCESS_KEY`):
  `.github/workflows/feeds-hydat.yml` (HYDAT → clim.json → `feeds/live/clim.json`; skips when unchanged unless `force`; the
  bundle half of update-hydat.sh stays on the Mac — the build needs the 10 GB atlas) and
  `.github/workflows/feeds-sync-inputs.yml` (DFO entry files + slim roster → `private/feeds/…`, writes only what differs;
  `push:` trigger commented). Listed in the Actions tab only once on `main`; until then `gh workflow run … --ref`.
  Not triggered. Staging's DFO entry files were re-uploaded by hand from the repo (region-6 had drifted).
- **The old prod cron** (`bc-fishing-cron`, source in `archive/cron-runner/`, untouched): every 15 min it dispatches
  `main`'s `update-hydro.yml` (Python hydro job → prod R2 `stations.json`, `gauges.geojson`, `recent/`, `history/`,
  `climatology/` — the OLD layout, which the prod app reads); `main` also has `update-in-season.yml` (6 h; opens a GitHub
  issue for unmatched in-season regulations) and `update-stocking.yml`. At prod switch-on: the new worker does not write
  the old layout, so retire bc-fishing-cron (and those workflows) only when the prod app reads `feeds/`; the in-season
  "unmatched → issue" duty is now the DFO feed's unplaced notices + `FEED_NOTIFY_URL`; stocking stays T200.
- Staging `RUN_TOKEN` rotated; the value is in `app/workers/feeds/.run-token` (gitignored, local only).

## Follow-up 2 (owner 2026-10-28): owner notifications by email, forecast reuse
Staging redeployed (b8715797), every job run by hand with `?dryNotify=1`; `vitest run --root app/workers/feeds` 101 tests.
- **One outbox** (`src/events.ts`): events with stable keys → deduped (told once; forgotten after a year) → email via the
  Cloudflare Email Workers binding `EMAIL` (`[[send_email]]`, unrestricted; from `NOTIFY_EMAIL_FROM` to `NOTIFY_EMAIL_TO`,
  OWNER sets both as secrets: the destination must be verified in Email Routing, the sender on an Email Routing domain);
  on email failure or no email → `FEED_NOTIFY_URL` webhook; neither → logged and recorded. A failed send stays pending and
  is retried next run. `NOTIFY_DIGEST=daily` → one digest a day (first run at/after `NOTIFY_DIGEST_HOUR_UTC`, default
  15 = 08:00 PDT); health is always immediate. **Dry run**: `POST /run/<job>?dryNotify=1` returns the messages, sends and
  records nothing; `POST /notify/preview` (bearer) shows the queued digest. DFO emails carry no link (the review app is local):
  "First: … queue --regions N / Then: Review app → DFO → Extents: <place>" (owner 2026-10-28; REVIEW_APP_URL removed).
- **What is told**: (a) HYDAT — the listing is asked once a day now (`private/feeds/gauges/hydat.check.json`), a new
  release → "New HYDAT <date> — run the HYDAT action" (the Actions workflow, then `scripts/update-hydat.sh --upload`);
  (b) in-season — `POST /run/inseason` ports main's `update-in-season.yml` scrape (provincial page accordion, 32 rows / 7
  sections today; a Worker is NOT blocked by the page's Akamai, unlike GitHub's runners): first run is the baseline, then
  any added / changed / removed row → one email with the diff (snapshots in `private/feeds/inseason/`); the old
  GitHub-issue job also matched rows to synopsis waters — not ported (the in-season rows are not in the app yet);
  (c) DFO — every unplaced / held / moved / unbound-header notice, one list per run, with the `queue --regions` command and
  the review-app tab to open (DFO → Extents); (d) health — a feed going degraded / failing / stale and recovering;
  (e) also included: **ECCC roster drift** (the station list changed → refresh the roster, then the sync workflow).
  Checked and NOT included: `update-stocking.yml` (weekly export + upload, no owner action — stocking is T200) and
  `update-hydro.yml` (data only). Staging dry run today: DFO 2 places to review; gauges a roster-drift message (+10
  stations ECCC lists that the roster calls silent).
- **Forecast reuse**: the forecasts come from the BC River Forecast Centre's ArcGIS layers (not ECCC). Issuance seen
  2026-10-28: COFFEE once ~09:04 PT; CLEVER in batches 09:35–10:55 PT; ELF ~09:12 PT, not daily (last 10-27). Each run
  now asks the three layers' `editingInfo.dataLastEditDate` (3 small requests); unchanged and the same Centre-local day
  → the last forecast run's output is reused (index `ahead` / `aheadDays` / `change`, the station files' `forecasts` +
  `change` verbatim), no summary queries, no station-file forecast reads, no forecast maths; only observations update.
  A new issuance or a new day recomputes once (`private/feeds/gauges/forecast.state.json`). Trade-off (owner's call):
  between issuances `ahead` is read at the issuing run's hour and the observation scaling is not refreshed.
- **Measured** (staging, CF cpuTime): gauges recompute 2,227 ms → **reuse 1,481 ms** (−33%), R2 B 438 → 401, A ~397
  (station files still carry new observations), subrequests 487 → 462. DFO unchanged run 123 ms. In-season 18 ms CPU,
  1 subrequest. Node (`parity/measure-change.ts`, full roster): 680–726 → 400–464 ms.
  **Cost table update** — gauges hourly 06–22 + 3-hourly night (~580 runs/mo), ~4 recomputes/day (issuance batches + the
  day roll) ≈ 120 × 2.3 s + 460 × 1.5 s ≈ **1.0M CPU-ms/mo** (was 1.4–2.9M); R2 class A ≈ 230k/mo (was 255k), B ≈ 235k;
  DFO ≤ 0.03M CPU-ms; in-season every 6 h ≈ 2k CPU-ms. All inside the $5 plan's 30M; R2 inside the free tier.

## Follow-up 3 (owner 2026-10-28): notification policy, email live on staging, in-season feed
- **Policy**: DFO places to review and in-season changes are URGENT (emailed at once); HYDAT, roster drift and feed
  health go in ONE daily digest, sent only on a day with at least one issue/action — a recovery alone (`fine`) never
  makes a digest (it rides along when there is one). Current state is /status's job, not email's.
- **Email is live on staging** (version 765d9851): Email Routing on canifishthis.ca enabled / ready, horvath.dawson@gmail.com
  a verified destination (both read through the API, nothing changed). `NOTIFY_EMAIL_TO` = secret; vars in wrangler.toml:
  `NOTIFY_EMAIL_FROM=status@canifishthis.ca`, `NOTIFY_DIGEST=daily`;
  `FEED_NOTIFY_URL` unset. One labelled TEST email sent through `POST /notify/test` (bearer) — accepted by send_email.
  From now on a non-dry run emails for real (the next DFO run: its 2 unplaced places, once).
- **In-season feed** `feeds/inseason/index.json` (published by `POST /run/inseason`; served by the staging data worker),
  format `inseason-feed/1` (recommended; spec in `src/inseason.ts`): top level `fetchedAt`, `changedAt`, `source`,
  `pageHash`, `matching {method, matched, unmatched}`, `rows[]`, `removed[]` (30 days). Each row: `id` (stable hash of
  region + water + its order among that water's rows — a reworded row keeps it), `region` (1…8, 7A/7B, provincial),
  `section`, `water` (as printed), `waters` (matched ids; empty) + `matched`, `effective` verbatim + `from`/`to` ISO,
  `change` verbatim, `parsed` only when confident (`kinds` closure / release / limit / bait / gear / licence / rescind /
  clarification, `species` names, `dailyQuota`, one `window` date range), `status` new / changed / unchanged (vs the
  page version before the last change) + `previous`, `firstSeen`, `changedAt`. Matching to waters is a separate,
  optional step (not built: all 32 rows publish `matched: false`). Written only when its content changes (+24 h
  heartbeat). /status shows rows, unmatched, new, changed, removed.

## Acceptance
Feeds update on schedule on staging for a week with no hand runs; a forced source failure alerts once and the app
shows the stale age; runbook written; then prod switched on by the owner.

Depends: T177 (gauge feed), T211 (DFO feed), staging deploy (T192–T198 lane). Size: M. Owner: prod keys (host decided: Cloudflare Workers).
