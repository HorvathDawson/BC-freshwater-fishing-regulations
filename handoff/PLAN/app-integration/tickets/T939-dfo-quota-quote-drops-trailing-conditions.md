# T939 — DFO salmon: the quota quote drops trailing conditions ("only 2 over 35 cm")

Status: DONE — 10ddff399; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — lane/lb-rules-leftovers done; left (Claude): main chat review + merge.)

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

## Result (lane/lb-rules-leftovers, 2026-11-20)
The premise does not hold: no condition is lost, so no quote changes, the change list is empty and `DFO_REV` stays.
- Every trailing quota condition in the 2026-11-20 scrape (only N over X cm, of which may be over, can be unmarked,
  none over, maximum size, cm or less) is already typed as its own sub-limit rule.
- Both the verdict's short line `t` (Python `deliver.salmon._text`, worker `salmon.ts`) and the quota line
  `quota.words` add the sub-limit's quote after the count: "4 hatchery marked per day, only 2 over 35 cm (Sep 1–Dec 31)"
  and "4 a day, hatchery-marked only · only 2 over 35 cm". Checked on every open verdict, 60 days, all regions.
- Running the count's own quote on would print the condition twice in both readers. F13 differed: "hatchery marked
  only" has no sub-rule.
- Parity pin: shared case "a quota's trailing condition is its own rule (T939)" in
  `app/workers/feeds/test/fixtures/dfo/dfo-rows.json` (five forms; parent quotes, `own`, `quota`, and a new `says` =
  the whole `t`), asserted in `pipeline/tests/test_t671_salmon_rows.py` and `app/workers/feeds/test/dfo.t671.test.ts`;
  `python-vectors.generated.json` regenerated. pytest t671 + dfo_salmon 270 passed; vitest dfo.t671 + dfo.vectors 132
  passed.
