# T178 — Re-score CLEVER vs COFFEE once the run archive holds enough, and set the weights by the verdict

Status: OPEN — due about 6 weeks after 2026-10-24 (the archive started with T177).

Source: T177 (owner Q160 "look into which model is best"). On 2026-10-24 only 5 stations × 1 issue day × lead 1 could be
scored, so the weights stayed equal. `python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds` reads
the run archive (`data/generated/gauges/feeds.runs/`) and prints the per-lead table and `skill.verdict` (equal until a
lead has ≥ 200 paired cases over ≥ 20 issue days; then equal within 2 SE, else inverse-error weights).

## Work
Run it; if the verdict says so, change `FLOOD_WEIGHTS` in pipeline/gauges/feed/change.py (one line, pinned by the policy
test), republish, record the table in decisions.md. A per-season answer needs a full year — say so. Size: S.
