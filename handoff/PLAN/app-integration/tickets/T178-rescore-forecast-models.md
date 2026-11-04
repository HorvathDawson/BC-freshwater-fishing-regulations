# T178 — Re-score CLEVER vs COFFEE once the run archive holds enough, and set the weights by the verdict

Status: OPEN — due about 6 weeks after 2026-10-24 (the archive started with T177).

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
