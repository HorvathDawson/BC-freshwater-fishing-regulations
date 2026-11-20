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
## Owner 2026-11-20 ~00:58 (main chat): the gap stays unbuilt
Verbatim: "Yes in theory we could have gear rules on a closed river because it could be a region rule bait ban but closed
river. But on the river that is closed that doesn't matter because no hear [gear] in water so closed is only closed". So no
pipeline change: a closed day is closed only, no gear bits.
