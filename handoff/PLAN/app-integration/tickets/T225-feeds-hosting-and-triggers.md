# T225 — Feeds: where they live, how they are triggered, how they fail (design, then build)

Status: OPEN — design first; owner picks the host (owner 2026-10-28: "how to make the feeds work the best as possible,
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

## Acceptance
Feeds update on schedule on staging for a week with no hand runs; a forced source failure alerts once and the app
shows the stale age; runbook written; then prod switched on by the owner.

Depends: T177 (gauge feed), T211 (DFO feed), staging deploy (T192–T198 lane). Size: M. Owner: prod keys (host decided: Cloudflare Workers).
