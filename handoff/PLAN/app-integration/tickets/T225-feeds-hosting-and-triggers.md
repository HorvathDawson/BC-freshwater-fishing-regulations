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

**Before switch-on** (staging week first, then prod = owner): `[triggers]` crons; the entries/roster sync step; prod data
worker 404 on `private/`; `FEED_NOTIFY_URL` as a worker secret; status.json + alerting (Work 4); retention needs `delete`
(added to r2Store). Python findings to decide (ported as-is): `forecastIssuedAt` written twice in index.json (T52's value
overwrites T68's); `temp_history.json` is published though private-ish; the runs archive month file is rewritten whole
each run (~8 MB late month — per-run objects would cut R2 bytes); DFO `dates._SEP` misses "October 1 to …" (row → advisory);
one bad DFO row holds a whole region; held text embeds pydantic 2.13's message. DFO re-types every page every run (as the
Python); caching by page sha256 would halve its CPU.

## Acceptance
Feeds update on schedule on staging for a week with no hand runs; a forced source failure alerts once and the app
shows the stale age; runbook written; then prod switched on by the owner.

Depends: T177 (gauge feed), T211 (DFO feed), staging deploy (T192–T198 lane). Size: M. Owner: prod keys (host decided: Cloudflare Workers).
