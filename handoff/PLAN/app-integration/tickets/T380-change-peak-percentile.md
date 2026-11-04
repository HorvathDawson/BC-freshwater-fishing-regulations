# T380 — Change: say the percentile today and at the peak, beside the ratio

Status: DONE in code (528620c65; owner 2026-11-04, in 0.5.0); live with the worker deploy.

## Ask (owner 2026-11-04)
"when it says this (At its highest, about 3× today's flow — fairly confident, forecast from 4 gauges nearby.) could we
also say what its peak will be percentile wise like will rise from 5th percentile flow to 95th percentile flow (about 3x
todays flow) at its highest."

## Why it is not app-only
The section's Change number is the RATIO alone (coded, `core/change.ts changeRatio`), carried through the donor pathway.
The per-day forecast percentiles (`ahead`, T171) come from ONE model per day; the ratio is CLEVER+COFFEE combined
(`change_source`). Reading the peak percentile off `ahead` could say "3× today's flow" beside a percentile that barely
moves. So the percentile at the extreme must come from the same combined series as the ratio.

## Work
1. Publisher (Python `pipeline/gauges/feed/change.py` AND the worker port `app/workers/feeds/src/gauges/change.ts`,
   parity-tested): per station, the extreme day's flow (base × ratio) placed in that day's climatology → a percentile;
   carried as a second coded number beside `change.discharge` (e.g. `change.peakPct`), through the donor pathway like
   the ratio (weights, own-gauge lead, refusals).
2. Core: `changeSentence` (and `changeReads` per donor) say "rising from the 5th percentile to about the 95th (about 3×
   today's flow)"; today's percentile is the horizon-0 estimate. Falls mirror it ("falling from … to …").
3. Tests: Python + worker parity, core words, the panel VM. Takes effect with the worker deploy (0.5.0).
Size: M.

## Done 2026-11-04
Publisher (Python + worker, parity green) puts the extreme day's `percentile` in the index row's `change`; data
`reading(row, q, "changeEnd")` + `PanelAnswer.changeEnds` (today = the Now view's own answer, extreme = the same pathway,
only when both answer); core `changeSentence` says "At its highest, from the 5th percentile today to about the 95th
(about 3× today's flow) — …". Per-donor rows (`changeReads`) unchanged.
