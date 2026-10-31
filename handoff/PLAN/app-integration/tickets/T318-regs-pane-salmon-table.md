# T318 — Regs pane: time-of-day closures in the year bar; salmon as a species/quota table; one image per species

Status: DONE (b7b3c1434, 048caf48a) (no APK) (owner 2026-10-31, with phone screenshots of the Fraser on 0.3.0: T318-shots/owner-fraser-regs.png,
owner-fraser-salmon.png).

## Work
1. **"At certain times" (e.g. no fishing 60 min after sunset → 60 min before sunrise):** not a separate boxed note under the
   bar. Integrate it with the year bar: a short clear line right under the bar ("Daylight only: closed from 1 h after sunset
   to 1 h before sunrise"), with the rule-book link inline; it applies to the whole year shown.
2. **Stray "source ›" box** at the top of What you can keep (empty card with only a link) — find what it is and either give it
   its words or remove it.
3. **Salmon as a table:** one row per salmon species (Chinook, Coho, Sockeye, Pink, Chum) with today's status and quota
   (daily / possession / size, "release", "closed"), then the per-place DFO rules folded per place (not repeated image grids).
   Region-wide DFO rules stay in the T309 rows.
4. **Images:** show only the ocean-phase image per species in lists; tapping a species opens the ID pane with both ocean and
   spawning images.
Words in core tables; existing components; no information dropped (pin with a test that every DFO rule is still reachable).

## Acceptance
Phone before/after shots of the Fraser (Region 2) regs pane and salmon card; test pins.

Depends: T309. Size: M. Owner: no.

## Done
- Timed closure: one line under the game-fish bar ("Daylight only: closed from 1 h after sunset to 1 h before sunrise", rule-book link inline, tap opens the rule); core `timedLineText`, `TimedLines`. The boxed "At certain times" block is gone for the timed kind. Pinned in Edges.test.tsx on the x15p2 fixture (the local Fraser data has no timed rule today, so no shot of it).
- Stray "source >" box: it is the in-season banner (`InSeasonBanner`); a banner with no text of its own is no longer drawn. Cause on the owner's phone not reproduced (local feed rows have text), so the guard is defensive.
- Salmon: core `salmonTable(card)`: one row per species (ocean picture only, status, sizes), DFO's words once under it, each unmapped DFO place folded to one line ("Show rules"), gear lines, region rules (T309) unchanged. Tapping a species opens `sp:CODE` (both looks, ID tips). Reachability pinned: core `salmon.test.ts` (salmonRowTexts within salmonTableTexts, five cases) and `SalmonCard.test.tsx` (every row/place/region/water rule text on the card once places are opened).
- Not done: daily/possession are not structured per species in the data, so the table carries status + sizes; the daily/possession limits stay in the T309 region rule rows.
- Shots (390 px headless, local data): T318-shots/{before,after}-fraser-salmon-phone.png, {before,after}-kispiox-salmon-phone.png, before/after-fraser-regs-phone.png.
