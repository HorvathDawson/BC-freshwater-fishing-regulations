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
