# T515 — Bait rule card: one heading line, no NOW-chip repeat

Status: DONE (words + layout) — owner 2026-11-08, phone screenshot of a Bait card (dark): "The formatting here isn't great. The bait ban now line at top is gross. Need to rework this a bit."

## What
- Before: sentence "No natural bait of any kind while the ban runs. ⓘ", then a NOW chip line "All year: bait ban · in force today", then the ✕ list. Same fact twice, vague "while the ban runs", mixed grey/white weights.
- After: ONE heading line over the list (ink, bold), built in `core/src/regs/bait.ts` `banHeading`:
  - all year: "No natural bait — all year ⓘ"
  - seasonal: "No natural bait May 1–31 · in force today ⓘ" (several runs join with ", "; "· not today" when none is on)
  - an exception stays ✓ (roe, worms…): "Bait ban …" instead of "No natural bait" (never claims "any kind" over a ✓ row)
  - only some baits banned and no season: no heading, just the lists.
- The chip lines (`BaitVM.dated`) are gone from the Bait card; ChangesLine stays for the Gear lines. Rule meaning, banned items, the ⓘ term and the Rule book link are untouched.

## Proof
- `bait.test.ts` banHeading cases (all year, in season, out of season, multi-run, partial, none); `gear.render.test.tsx`; frozen fixtures re-cut with UPDATE_REGS_FIXTURES=1.
- Shots: `shots/T515-*.png` (before/after, dark + light).
