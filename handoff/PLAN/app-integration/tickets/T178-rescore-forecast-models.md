# T178 — Re-score CLEVER vs COFFEE once the run archive holds enough, and set the weights by the verdict

Status: PARTIAL — c97f30294: the feeds worker scores CLEVER vs COFFEE monthly and reports (deployed with the feeds worker, staging and production 2026-11-18/19); left (Claude): the first full-month verdict (2026-12-01 for November) and, if it moves, the reviewed weight change.
full-month verdict (2026-12-01 for November) and, if it moves, the reviewed weight change.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T178 | re-score CLEVER vs COFFEE from the run archive (~6 weeks) and set the weights by the verdict | PARTIAL (worker scores monthly + reports; deployed; weights stay reviewed) | c97f30294 | first full verdict 2026-12-01 |

Source: T177 (owner Q160 "look into which model is best"). On 2026-10-24 only 5 stations × 1 issue day × lead 1 could be
scored, so the weights stayed equal. Owner 2026-11-04: agreed — the worker scores monthly and reports; applying weights
stays reviewed.

Done (c97f30294): `app/workers/feeds/src/gauges/score.ts` ports skill.py (scores PER LEAD DAY across all stations, not per
water); cron `55 10 1 * *` (the 1st, 02:55 PST) scores the archive month that just closed (R2
`bc-fishing-regulations-staging/private/feeds/gauges/runs/`); out: `private/feeds/skill.json`, GET /status ("Forecast
model scores") and /status.json (`skill`), one `skill` event through the outbox (key `skill:<month>`) saying which lead's
verdict differs and that NOTHING changes by itself. By hand: `POST /run/skill[?month=YYYY-MM]` (Bearer RUN_TOKEN).
Parity: `test/gauges.skill.test.ts` vs `parity/gauges/skill_harness.py`. Cost ~1 s CPU a month.

## Work
Read the 2026-12-01 verdict. `skill.verdict`: equal until a lead has ≥ 200 paired cases over ≥ 20 issue days; then equal
within 2 SE, else inverse-error weights. On 11-04: 5 issue days, 115 paired, "too few". If it moves: edit FLOOD_WEIGHTS in
BOTH app/workers/feeds/src/gauges/change.ts and pipeline/gauges/feed/change.py (one line, pinned by the policy test),
table in decisions.md, deploy the worker — a reviewed change (owner 2026-11-04). A per-season answer needs a full year —
say so; per-water scoring needs ≥ a season per station. By CLI instead: `rclone copy` the R2 runs, concatenate the open
month (`<YYYY-MM>/<day>.jsonl` + `<YYYY-MM>/<day>/<HHMMSS>.jsonl` parts, key order) into `<feed>.runs/<YYYY-MM>.jsonl`,
run `python -m pipeline.gauges.feed.skill --feed <feed>` (the local `data/generated/gauges/feeds.runs/` stopped on 10-24).
Size: S.
