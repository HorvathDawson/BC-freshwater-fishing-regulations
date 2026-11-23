# T754 — Places that cover one bank of a river only (Kitimat west bank)

Status: OPEN (owner answered 2026-11-15, round 4: "Build it this way (Recommended)"; to build)

Source: owner 2026-11-15, while labelling DFO salmon places: "also for kitimat it is (on west bank between signs at
Kitimat hatchery outfall. · ) we should have a way to define one half of the river closing maybe? could color only
west or east side? could be an option in extent i think this is the only river like this? but worth at least doing the
coloring. then idk how to represent it otherwise. have an agent look into this please"

Everything below is a proposal. What a wording means is the owner's call. V = verified (read in a file or computed),
I = inferred (not run or not checked).

## 1. How common

Scanned every string in: `data/curated/dfo_salmon/places/region-*.json`, `data/generated/regs/dfo_salmon/region*.json`
(the scraped rows), `data/generated/review/dfo-suggestions.json` (the queue), every page under
`cache/dfo_salmon/history/`, `data/curated/regulations/synopsis/region-*.json`,
`data/generated/regs/extraction/synopsis_raw_data.json`, `data/curated/inseason/notice_waters.json`, `data/generated/inseason/`
and the in-season feed fixtures. Patterns: north/south/east/west/left/right/near/far/opposite + bank/side/shore/half,
"side of the river", "from the bank", "centre line". (V)

**Half of a river's width (the real cases): 2 rivers, 3 records.**

| Source | Water | Wording | Stretch named too? | Stored today |
|---|---|---|---|---|
| DFO R6 (active; in 14 history pages since 2017) | Kitimat River | "on west bank between signs at Kitimat hatchery outfall." | yes: the two outfall signs | `between` the two sign cuts, tributaries true, no note, `spatial_caveat` false (`places/region-6.json:4200`); owner mark saved 2026-11-15 (`marks.jsonl:2662`) |
| Synopsis R6 p.51 | Kitimat River | "No Fishing on the west half of river between fishing boundary signs near Kitimat Hatchery outfall" | yes: same two cuts | rule `kitimat_river.r1`, `side: west` (`synopsis/region-6.json:3755-3793`) |
| DFO R6 (dormant; history 2020, 2022) | Nakina River | "from the centre line to the northern bank, starting from a boundary sign ~300 m … to ~1300 m upstream of the Nakina and Sloko rivers confluence" | yes: 300 m and 1300 m cuts | whole channel + note, `spatial_caveat: true` (`places/region-6.json:10253-10324`); owner ruling 2026-11-12, T683, RULINGS E19 |

So the Kitimat is not the only one: the Nakina is the second, and it is dormant (not on DFO's current page). In-season
notices: none. (V)

**Look alike but are not a half-width (no change proposed):**
- Stave River: "on the east side of the Stave River, known as the Ruskin Spawning Channel" / "on the west side … the
  Northrop Spawning Channel" (DFO R2). These are separate side channels, not halves of the river (the queue already
  maps Ruskin to an OSM side channel). (V)
- Lines between signs "on opposite shores" / "on either side of the river" (Chilliwack, Stamp, San Juan, Little
  Campbell, Fraser at Lillooet, ~15 lake bays): a cut across the water, already the cut model. (V)
- Lake halves: Big Lagoon "(west side of lake)" no powered boats (R5); "south half only" speed limit (R4); "near eastern
  shore" speed limit (R4). Lakes use `undrawn_part` (AGENTS rule 49). Vaseux Lake's "lagoons on the west side of
  Okanagan River" are named waters. (V)

## 2. What exists already

**The synopsis already has a one-half model, end to end (V):**
- Rule field `side: north|south|east|west` (`pipeline/regs/parsing/catalogue_parts/rule.py:457-468`, enum
  `vocab.py:134-144`), checked both ways against the sentence (`rule.py:809-826`, `HALF_OF_CHANNEL` `vocab.py:163`).
  AGENTS rule 49 (user ruling 2026-09-28): "Half of a river is `side` … read beside the other rules and displaces none,
  so the other half follows the water's other regulations."
- Ladder: a `side` rule in force becomes state `part`, shown beside, never governing (`pipeline/deliver/bundle/read/ladder.py:805-811`).
- Words: "west half of the channel only — on the east half, this water's other regulations apply"
  (`catalogue_parts/labels.py:434-441`). Guide: "Show `parts.side` … never colour or close the whole width by it"
  (`pipeline/deliver/export/guide.py:1169-1178`).
- App: `settled.side` (`app/packages/core/src/regs/settle.ts:106`), caveat box "⚠ On one side of the channel only"
  (`regs/caveats.ts:177-184`, `regs/words.ts:125-126`). The map never colours it.

**DFO places have no side field (V):** a binding is extents + tributaries + `notes` + `spatial_caveat`
(`pipeline/regs/dfo_salmon/entries.py:94-97`). The half-width note pattern only knows "from the centre line to the X
bank" (`locations.py:186-189`, T683), so the Kitimat's "on west bank" gets no note. The card line is DFO's own words
(`pipeline/deliver/salmon.py:613-615`): the live release (`r-f749475d0962`) carries "Kitimat River — on west bank
between signs at Kitimat hatchery outfall." In that release the place was still unbound (its chain carries the undrawn
`n` runs), so it is a card caveat only. With the extent the owner saved today it will paint the stretch closed to
salmon across the whole width from the next bundle (`chain_calendar`, `salmon.py:573-604`) (I: not built).

So today the same 700 m stretch is treated two ways: the synopsis says the other half follows the other rules and does
not colour it; DFO (as the Nakina ruling set it) closes the whole width and colours it.

**The straddling-section "side marking" cannot carry this (V):** that note was about which region a section
belongs to along the river (a lengthwise choice at a region cut). It was then replaced by T183: a straddler takes both
regions' rules and the strictest wins; `section_home` is for display only (`pipeline/deliver/bundle/schema.sql:636-641`).
It has nothing across the channel, so a bank side cannot use it.

**Map building blocks (V):** stream sections are one line in the `stream` layer, painted by feature-state. Extra
marks are their own runtime layers over that line: the closed stripe (`app/packages/map/src/paint.ts:260-290`), the
passing-run dash (`paint.ts:241`), gear edges (`paint.ts:216`). A river's wide reach is a polygon in `lake`/`wetland`
plus its route line in `stream` under the same section id (`pipeline/deliver/tiles/export.py:257-259`). Nothing uses
`line-offset` today.

## 3. Options, smallest first

The verdict for the reader on the other bank is a separate choice from the drawing (owner question 2):
**V-beside**: the other bank follows the water's other rules (synopsis rule 49). **V-whole**: the whole width takes
the rule, which is stricter (Nakina, E19). Each option below works with either.

### A. Words only (cheapest)
- Mark/extent: `ExtentFix` gets `bank: "left" | "right"` (looking downstream) and `bank_word` as printed
  ("west", "north"); the DFO `Binding` gets the same two fields (`entries.py` `Binding.to_dict/from_dict`).
- Editor: one row under "tributaries" in `ExtentFixEditor.tsx` (~l.192): `bank: whole width | left | right`, with
  "(= west, from the line's bearing)" shown beside it.
- Bundle: the salmon place carries `bank` and the words; the card line adds "west bank only (the hatchery side)".
  The feeds worker builds places live, so `app/workers/feeds/src/dfo/locations.ts` must carry it too (AGENTS rule 40).
- Map: unchanged (V-whole: closed across the width; V-beside: not drawn by this rule).
- Answer: the closed line always names the bank; the other bank's line says which rule it answers to (V-beside) or
  "closed across the whole width between the signs; DFO's rule names the west bank" (V-whole).
- Rebuild: bundle + `sync_inputs` + worker deploy. No atlas, no tiles.

### B. Words + one-side colouring (recommended)
Everything in A, plus:
- Bundle: per section of the stretch a bank sign `bk` = +1 (right of the drawn line) / -1 (left), computed by the
  pipeline against the direction of the exact line it exports. FWA route measure grows upstream (V: u/s sign
  5046 m > d/s sign 4296 m on blk 360885824), and added streams are written mouth first (V:
  `added_streams/build_dataset.py:240`), so drawn lines most likely run upstream, not downstream (I: not checked in
  the tiles). The sign must come from the geometry, never be assumed.
- App map: two runtime layers over `stream`, `stream-bank-r` and `stream-bank-l`, each with a fixed `line-offset`
  (± half the stream width + 1 px; positive is right of the line's direction) and opacity by feature-state `bk`, in the
  rule's colour (closed). Two fixed layers avoid relying on feature-state inside `line-offset` (I: not tested on the
  patched MapLibre). Main line colour: V-beside = the other bank's verdict; V-whole = closed.
- Wide reach drawn as a polygon: the stripe is drawn along the route line inside it, so at high zoom it sits off the
  real bank (I). Acceptable for the Kitimat if it is a line there (not checked); otherwise option C.
- Synopsis: the same layer could draw the synopsis Kitimat rule too (today never coloured; guide.py:1175 says so).
  That changes a stated rule, so it is owner question 6.
- Rebuild: bundle + worker + app build (new layers). No atlas, no tiles.

### C. True half-width geometry + beside verdict in the salmon cascade
- Pipeline writes the half as its own shape: a one-side offset band for lines (shapely `offset_curve`), the polygon
  split along its route line for wide reaches, into a small `layers/*.geojsonl` beside the tiles.
- Salmon cascade: the bank place is shown beside instead of governing, in `pipeline/deliver/salmon.py` and the worker's
  `live.compose`, so the other bank gets the next place's verdict (V-beside only).
- Rebuild: tiles layer + bundle + worker + app. The most work for two rivers, one of them dormant.

Never: bind nothing (drops the closure), or close the width with no bank words (the east-bank reader is told something
DFO did not say, with no reason given).

## 4. Which side is "west", mechanically

- Flow bearing = the chord from the upstream end of the stretch to the downstream end. Right bank looking downstream is
  flow + 90 degrees. "West" is whichever bank's direction has the negative east component, and so on for N/S/E.
- Kitimat (V, from the sign coordinates in `data/curated/waters/cuts.json:7589,7608`): chord 702 m, flow bearing 210
  degrees (SSW), right bank faces 300 degrees (WNW), so **west bank = right bank looking downstream**. The outfall point
  (owner's 54.03470,-128.68244) lies 121 m at 328 degrees from the on-river point, on the right bank: the hatchery side.
- Nakina: north bank is most likely the right bank (river flowing west to the Sloko) (I: not computed).
- Ambiguous: when the bank's direction is within about 30 degrees of the axis the word does not name (a river flowing
  east or west and a "west bank" wording, or flowing north/south and a "north bank" wording), and on a meander whose
  pieces' local bearings give opposite answers. Then the machine gives no default.
- Proposal: the owner picks **left / right looking downstream** in the editor. The form shows the computed compass word
  as a hint ("right = west here") and the map preview draws the stripe on the chosen side. The stored value is
  left/right, and the printed word is kept as written.

## 5. Recommendation

Option B, with one model for both sources: `bank: left|right` (owner-picked) plus the printed word on the DFO binding,
and the same `bank` beside the synopsis rule's `side`. The card always names the bank, and the map draws a one-side
stripe. Two cases do not justify C. The verdict choice (question 2) decides the main line's colour, and it should be the
same for DFO and the synopsis.

## Owner questions (his call)
1. "On west bank" (DFO) vs "west half of river" (synopsis), same signs: one meaning (the west half of the water), or
   does DFO's mean fishing from the west bank's shore?
2. Other bank: beside (follows the water's other rules, as synopsis rule 49) or whole width (stricter, as the Nakina
   E19)? One answer for both sources? Does the Nakina get reopened to match?
3. Kitimat: confirm west = right bank looking downstream (the hatchery side).
4. Colour: one-side stripe in the closed colour; on a wide reach drawn as a polygon, is a stripe along the route line
   (not the bank) acceptable?
5. Tributaries: the DFO Kitimat place binds with tributaries (from DFO's "Kitimat River (including tributaries)"
   heading; the 11-15 mark kept it), while the synopsis rule has none. Should a one-bank closure take in tributaries?
6. Should the synopsis Kitimat rule get the stripe too (today the map never colours it)?

Depends: owner answers. Size: B is M (pipeline field + editor row + worker + two map layers + tests).

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
owner 2026-11-15: "we should have a way to define one half of the river closing maybe? could color only west or east side?"; 2 rivers, 3 records; recommended: bank left/right looking downstream + one-side stripe
