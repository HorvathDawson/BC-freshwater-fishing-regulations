# T1036 — The every-source sheet ("All sources ›") lists rules the screen does not draw, and misses the drawn gear on salmon-only days

Status: OPEN.

Source: the independent review of PR #63 (T991 round 3, 2026-11-23), probing the live release r-741815c6090d over 8 waters ×
parts × 9 days. The Source row was fixed in #63 to cite exactly what the drawn tabs cite (`drawnRegsOf`,
`WaterScreen.sources.test.tsx`). The sheet behind its "All sources ›" (`src:all`, T990,
`core/regs/sheets/allsources.ts` `pageSourceRules`) has the same two faults and was left as it was:
1. It reads `page.gear` / `page.methods`. On a day only salmon may be fished, Gear and Methods draw the day's gear "if
   fished" (`gearTabsFor`, salmon.ts), so the sheet misses those rules (p.8 / p.10 on gnis:2308 Jan–Jun, gnis:27804 part 1,
   gnis:10061 Apr–Sep; p.70 on gnis:10061) and lists the closed day's rules instead.
2. It adds every card row's deciding rules (`env.card.rows`), even on a closed day, and `closeForFinfish` (finfish.ts) keeps
   the original sheet. So rules nothing on the screen cites are listed (p.68 on gnis:10061 Aug–Sep, p.13 on gnis:27804
   part 1, p.64 on gnis:14619 part 2, the p.6 / p.7 general pages on open days; p.7 is the licences page the owner rejected
   on Sweltzer, T916).

## Work
Build `src:all` from the drawn view models, as the Source row is built: a screen-level wrap of `page.sheet` (as
`withInSeason` already does for the in-season changes), fed `drawnRegsOf(page, tabs, licence)`. Each listed rule must be one
a drawn line cites (its key on a drawn source line, a drawn card row, the drawn trout table). Decide with the owner whether a
species sheet's own rules (a row's "See every regulation line ›") belong in the page's sheet: today they do.

## Acceptance
The probe of `WaterScreen.sources.test.tsx` extended to the sheet: the sheet's pages equal the drawn tabs' pages, on the
fixture cases and the built salmon-only day; mutation-proved.

Depends: T991. Size: M. Owner: the question in Work. Default: list only what the drawn tabs cite.
