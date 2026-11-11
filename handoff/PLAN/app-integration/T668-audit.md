# T668 audit — DFO salmon places on one stretch from more than one region

Read-only, 2026-11-11. Release `r-303b4007a70a`; curated places `data/curated/dfo_salmon/places/region-*.json`.
Nothing in the product or the curated data changed.

## Method

- Decoded the release salmon file (`salmon` + every `salmon-<r>` shard) with `pipeline.deliver.salmon.decode_files`.
  It holds 456 chains over 1,949,844 sections. A chain is the ordered list of DFO places on a section (`l`, drawn) plus
  the places on its water that are not drawn yet (`u`). Its `r` is every DFO region those places come from.
- A "cross-region" section is one whose chain has `len(r) > 1`. For each one I recorded the bundle shards the section
  sits in. I also compared the combined chain's per-day verdicts with each member place's own single-place chain, to
  see who wins and whether the losing region would say something else.
- Read the code paths: `deliver/salmon.py` `chain_order` / `verdict_on` / `build` (the `undrawn_on` step),
  `core/src/salmon.ts` `salmonCard` / `undrawnChains` / `salmonTimelineKey`, `ui/src/salmonKey.ts`, and
  `regs/review/dfo.py` (marks keyed by `location_id`).
- **Not verified:** the live feed (`workers/feeds/src/dfo/live.ts`, which may repaint a chain), the app on a device or
  emulator, and the review app's UI. The card behaviour below comes from reading the code, not from a screen.

## What a two-region section does today

There is no merge and no conflict flag. Every place on the section goes into one chain, sorted by tier (precedence),
then by the place covering **fewer sections**, then by location id. For each species and day, the first place with a
rule answers. The losing region is never shown. The card's source line lists every region in the chain, but no row
says which region's notice it is showing.

## Cases (27 chains, 493 sections)

| class | what | chains | sections | status differs? | example |
|---|---|---|---|---|---|
| **A** Fraser R2/R3, not drawn | All 10 Fraser places (8 R2, 2 R3; 3 not dormant) have **no extent**. An undrawn place is attached to every section of its water's items, and `gnis:39325` is the whole Fraser mainstem. Both regions' places therefore land on every Fraser section. | 7 (69, 72, 80, 82, 95, 135, 136) | 238 (shard 2: 34, 3: 26, 5: 43, 7a: 110, x: 21; 4 straddle 3/5 or 5/7a) | rows: no (the drawn default answers; the map is `closed` all year in the release). Card: yes, see below | Fraser at Prince George (chain 69, 110 sections): Region 7 default answers |
| **B** same tier, size breaks the tie | Two regions' region-wide or area places on a section that lies in both regions (or near the line) | 10 (108, 117, 127, 138, 254, 283, 284, 286, 290, 307) | 91 | **24 sections**: 254 (3, 5B section opens chinook and coho 366 days vs R1 default closed; 5B wins), 284 (19, 5B section over R6 E, coho differs 61 days), 307 (2, R6 Fraser-watershed closure over 5B section, which opens chinook and coho all year). The other 67 sections have the same status, but the "no fishing in Region X" sentence is the winner's | 108: 5A default beats 7 default only because it covers fewer sections |
| **C** another region's narrower place wins by tier | Region 6 section, closure or water places on sections in 6/7a or 5/6 (and Sustut / Skeena B(i) on 15 sections wholly in 7a) beat the neighbour's region default | 8 (119, 125, 131, 140, 257, 258, 282, 299) | 118 | **63 sections** (140, 282, 299): R6 section E opens chinook 366 days and coho 305 days, while the R7 / 5A default is closed. The others have the same status | 140 (48 sections, 6/7a and 6/7b) |
| **D** other undrawn place spread past its region | Same mechanism as A: 5B Dean River places on 45 sections in shard 5 (under the 5A default, chain 173); R8 Shuswap places on 1 section in shard 3 (chain 132) | 2 | 46 | rows no; card yes | Dean (chain 173) |

Also found:
- Water `6:fraser-river` in `region-6.json` matches the same 13 items as `2:fraser-river`, and its match reason is
  Region 2's text. It has 0 locations, so the app is not affected, but it is misfiled.
- Both active R2 Fraser places are `duplicate_of` a dormant sibling. Neither duplicate is confirmed.

## What an angler sees (class A, from the code)

Fraser at Prince George (chain 69). The rows are Region 7's: "There is no fishing for salmon in Region 7 until
further notice". Because the chain's `r` and the item's `unmapped` include Regions 2 and 3, the same card also shows:
- an "in part / not yet mapped" caveat that lists R2's Mission–Hope place (open: coho 24 days, chum 28 days) and R3's
  Seton–Lillooet place;
- the R2/R3 "daylight only" hours line, which Region 7 does not print;
- the R2/R3 header notes;
- the source line "Region 2 Lower Mainland · Region 3 Thompson-Nicola · Region 7 Omineca-Peace".

The same happens at Mission (R3's Lillooet place in the caveat) and at Lillooet (R2's places in the caveat). Nothing is
duplicated as a row. Instead, the rules of a stretch 500–1,000 km away appear in the caveat, framed as "parts of this
water".

## Stretch split (T517)

The join key `salmonTimelineKey` compares a chain's verdicts, gear and map runs only. It ignores `r`, `l` and `u`.
Sections whose winning text is identical therefore join even when different regions' places sit under them (for
example, chain 69 with the R7-only chain 67). The joined part then reads the union of the chains' regions, so the R2/R3
hours line spreads over the joined stretch. Different winning words ("Region 5A…" vs "Region 7…") split the stretch
correctly. The card never says which region's notice a row shows.

## Review app (T164 / T361)

All 352 location ids are region-prefixed and unique (0 collisions). Marks are keyed by `location_id`, so labelling the
Fraser for R2 and for R3 cannot overwrite each other. Staleness is per location, and a re-scrape stales none (T366).
No defect found here. The UI was not opened.

## Proposed fixes (each one a proposal; nothing changed)

1. **A + D: scope an undrawn place to its own region.** In `build`, attach an undrawn place only to its items' sections
   inside its region's bundle shard(s), and leave `u` regions out of a chain's `r`. This stops the Prince George card
   from showing Mission's caveat and hours. The real fix is drawing the 10 Fraser places (all `pending`; the cuts are
   CPR Bridge Mission, Hwy 1 Hope, Alexandra Bridge, Seton confluence, Lillooet signs).
2. **B: replace the size tie-break across regions.** On a section, a place of the region the section belongs to (its
   shard; for a straddler, the region holding its longer part) beats an equal-tier place of another region. Size stays
   the tie-break within one region.
3. **C: clip area-wide places at the region line, not by touch.** A `within area:region:N` place (a region default, an
   R6 section such as E or the F closure) binds a straddling section only if most of it lies in N. Named-water places
   (Sustut, Skeena B(i)) keep winning across the line, because DFO's page names those waters.
4. **Card: name the region when the page spans more than one.** Each row says whose notice it is (for example
   "Region 6 notice"), and the hours and header notes come only from the regions whose places answer a row or appear
   in the caveat.
5. Hygiene: remove or re-word the `6:fraser-river` water record; confirm or clear the 2 R2 duplicate flags.

## Owner decisions (default if no answer)

- **D1** Undrawn place outside its own region (A, D): scope it to its region (fix 1)? *Default: yes.*
- **D2** Two regions, same tier, one section (B): the section's own region wins (fix 2), or the stricter verdict wins?
  *Default: the section's own region.*
- **D3** Region 6 area places on straddling sections (C, 63 sections where chinook/coho flip): clip by the majority of
  the section's length (fix 3)? *Default: yes; named waters keep winning.*
- **D4** The card names the answering region when a page spans two (fix 4)? *Default: yes.*
- **D5** Draw the 10 Fraser places now (review app) rather than wait for fix 1? *Default: fix 1 first (cheap), drawing
  in the next DFO review pass.*
