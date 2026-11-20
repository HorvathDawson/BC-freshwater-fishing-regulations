# T939 — DFO salmon: the quota quote drops trailing conditions ("only 2 over 35 cm")

Status: OPEN.

Source: independent review of lane/l1-feeds (T623 F13), 2026-11-20. Out of F13's scope, so ticketed apart.

## What happens
The quota reader (Python `pipeline/regs/dfo_salmon/typed.py` and the worker's `app/workers/feeds/src/dfo/typed.ts`
`RE_QUOTA_SPAN`) stops the quoted quota at "per day" (F13 adds only a trailing "hatchery marked (fish) only"). A cell such
as "4 hatchery marked per day, only 2 over 35 cm" (4 cells in the 2026-11-20 scrape) is quoted as "4 hatchery marked per
day": the size limit on the quota is lost from the words shown.

## Work
Keep a trailing condition of the quota in the quote in both readers (same rule, shared rows in dfo-rows.json), bump
`DFO_REV`, regenerate vectors, re-type (`python -m pipeline.regs.dfo_salmon.feed`, no credits). List every cell whose
quote changes; nothing else may move.

## Acceptance
Every scraped cell with a trailing quota condition quotes it whole, in both readers; vectors agree.
