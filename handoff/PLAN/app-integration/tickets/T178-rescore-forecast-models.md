# T178 — Re-score CLEVER vs COFFEE once the run archive holds enough, and set the weights by the verdict

Status: PARTIAL — the worker scores monthly and reports (2026-11-04, not deployed: goes with the app); left: the first
full-month verdict (2026-12-01 for November) and, if it moves, the reviewed weight change.

Source: T177 (owner Q160 "look into which model is best"). On 2026-10-24 only 5 stations × 1 issue day × lead 1 could be
scored, so the weights stayed equal. `python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds` reads
the run archive (`data/generated/gauges/feeds.runs/`) and prints the per-lead table and `skill.verdict` (equal until a
lead has ≥ 200 paired cases over ≥ 20 issue days; then equal within 2 SE, else inverse-error weights).

## Work
Run it; if the verdict says so, change `FLOOD_WEIGHTS` in pipeline/gauges/feed/change.py (one line, pinned by the policy
test), republish, record the table in decisions.md. A per-season answer needs a full year — say so. Size: S.

## 2026-11-04: checked early (not due)
The local `data/generated/gauges/feeds.runs/` stopped on 10-24 (the pipeline no longer publishes); the archive that grows
is the staging worker's: R2 `bc-fishing-regulations-staging/private/feeds/gauges/runs/` — a closed month is one
`<YYYY-MM>.jsonl`, the open month is `<YYYY-MM>/<day>.jsonl` + `<YYYY-MM>/<day>/<HHMMSS>.jsonl` parts (concatenated in key
order = Python's month file). To score: `rclone copy` it, concatenate the open month into `<feed>.runs/<YYYY-MM>.jsonl`,
run `skill --feed <feed>`. On 11-04 it held CLEVER/COFFEE issue days 10-28, 10-29, 10-30, 11-02, 11-03 (weekdays only):
verdict still "too few" (115 paired over 2 days). Twenty issue days ≈ early December, as planned.

## Owner 2026-11-04: automatic, in the feeds worker?
T178 scores the two flood models (CLEVER vs COFFEE) PER LEAD DAY across all stations — not per water — and sets the
blend weights `FLOOD_WEIGHTS`. Proposal: the worker scores the archive itself on a monthly cron (the scoring is
`skill.py`; port it like the rest, parity-tested) and REPORTS the verdict (status + email); applying new weights stays
a reviewed change (a weight that moves on its own would change every forecast without anyone seeing why). Per-water
scoring needs far more history (≥ a season per station).
Worker cost (Cloudflare analytics, bc-fishing-feeds-staging, 2026-10-28 → 11-04, 8 days): 1,330 invocations, 77,490
subrequests, 0 errors, 687 CPU-s (~86 s a day → ~2.6 M CPU-ms a month, ~9 % of the paid plan's included 30 M; a cron
run p99 ~4 s CPU). Production adds a second worker's crons on the same account (~18 % together).

Owner 2026-11-04: agreed — the worker scores monthly and reports; applying weights stays reviewed.

## 2026-11-04: built — the feeds worker scores monthly and reports
`app/workers/feeds/src/gauges/score.ts` ports skill.py's `band` / `cases` / `_stats` / `score` / `verdict` /
`feed_observed` / `clim_ranks` (ranks = the publisher's own `percentileOf` + `bandFor` on clim.json's discharge
envelope; observed = each station file's `daily`). Cron `55 10 1 * *` (the 1st, 02:55 PST) scores THE ARCHIVE MONTH
THAT JUST CLOSED — `runs/<YYYY-MM>.jsonl` + that month's day files / parts still in runs/state.json, in key order (= the
Python month file). One month on purpose: ~22 weekday issue days (past MIN_ISSUE_DAYS) and the station files' `daily`
still hold that month on the 1st (this year + last December through January). The CLI scores the whole archive; that
is the one difference.
Out: `private/feeds/skill.json` (table + per-lead verdict + shipped weights + `differs`), shown on GET /status (a
"Forecast model scores" table) and /status.json (`skill`); one `skill` event through the outbox (daily digest, key
`skill:<month>`) whose text says which lead's verdict differs and that NOTHING changes by itself. By hand:
`POST /run/skill[?month=YYYY-MM]` (Bearer RUN_TOKEN).
Applying: edit FLOOD_WEIGHTS in BOTH app/workers/feeds/src/gauges/change.ts and pipeline/gauges/feed/change.py, table in
decisions.md, deploy the worker — a reviewed change (owner 2026-11-04).
Cost: one month ≈ 25 k runs (~8–12 MB) + ~400 station files + clim.json: measured ~0.5 s CPU locally on a 12 MB / 400
station synthetic month — about 1 s a month, ~0.003 % of the plan's 30 M CPU-ms; ~400 R2 reads, 1 write.
Parity: `test/gauges.skill.test.ts` — Python (`parity/gauges/skill_harness.py`, concatenating the worker layout as this
ticket describes) vs `runSkill` on a generated month in the worker layout (month file, day files, parts, a torn line,
other months not read): same table and verdict (a "COFFEE better" month and a "too few" month).
