# T934 — The salmon year bar shows the B.C. bait ban and fly-only days too

Status: OPEN.

Source: owner 2026-11-20 ~00:08 (main chat), screenshot `T934-shots/owner-2026-11-20-cheakamus.png` (Cheakamus River,
Regulations, Nov 20: the GAME FISH bar carries the bait-ban band all year, the SALMON bar only Open / Closed). Verbatim:
"Bait ban needs to apply to salmon [too]. So would fly fish only..."

Cause: `salmonStripOf` (`app/packages/core/src/salmon.ts`) builds the salmon bar from DFO's day statuses alone; it reads
the provincial bar (`general`) only for its today marker and "when" line, never its per-run `bits` (the stored strip's
bait-ban / fly-only flags) that the game-fish bar draws. The gear lines already say a B.C. bait ban for salmon (T870
item 2, "— DFO and B.C. rules"); only the bar and its key leave it out.

## Work
1. Core: the salmon bar's runs carry the provincial gear flags of the same book days (bait ban, fly fishing only,
   artificial fly only) on the days salmon may be fished (open / release); a closed day stays the closed stripe. The key
   adds "Bait ban" / "Fly only" as the game-fish bar's key words it, only when drawn.
2. UI (app + website, the same component as the game-fish bar's bands): draw the band the same way.
3. Whole-string / runs tests in core; Cheakamus River pinned.
## Acceptance
Pictures (Cheakamus River, and a fly-only water with salmon, 390 light/dark) approved by the owner before it lands.
Depends: none. Size: S. Owner: yes (pictures). Batched with T922–T925.

## Built (lane/t934, 05a64a47d)
Core `salmonStripOf`: each salmon run carries `gear` = the provincial strip's bits 4 / 8 / 16 of the same book days, on
open / release days only; the key adds each drawn gear mark in the game-fish key's words ("Fly fishing only", "Bait ban").
The two bars' day counts must agree (throws otherwise). UI `SalmonRect`: fly-only / artificial-flies stripes in the salmon
keep colour on the card, the bait band as the game-fish bar's. Tests: salmon.test.ts (T934 case), SalmonYearStrip.closed.
GAP (not built): the provincial strip drops gear bits on a game-fish CLOSED day (`display/strip.py` `strip_code`: CLOSED
alone), so a day salmon are open while game fish are closed shows no band. Closing it needs the pipeline to carry the
gear bits on closed frames too (and the game-fish bar to keep ignoring them): a data change for the next release (owner 00:58 below: not built; T936 warns instead).
## Owner 2026-11-20 ~00:58 (main chat): the gap stays unbuilt
Verbatim: "Yes in theory we could have gear rules on a closed river because it could be a region rule bait ban but closed
river. But on the river that is closed that doesn't matter because no hear [gear] in water so closed is only closed". So no
pipeline change: a closed day is closed only, no gear bits.
