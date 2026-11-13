# T686 — Shrink shipped data across the board

Status: OPEN

Source: owner 2026-11-12 (T678 release stopped at the size gate): "Raise now, shrink later, but the ticket should cover
ways to shrink data across the board, not just ponds." RULINGS O5 raised `core`, `conditions` and the web's
`first_search`; this ticket is the "shrink later".

## Survey so far (measured 2026-11-12, live release r-483b295de57c)

52 shipped files, 55,659,734 B raw, 21,823,742 B gzip; plus the atlas tiles (186.9 MB pmtiles) and the basemap (215 MB
dir). Per table: each table copied alone into a fresh SQLite file, vacuumed, gzip -6 (script: session scratchpad
`t686/tables.py`; the sum of tables is a little over the file's gzip).

| file | gz | gate | largest tables (gz alone, rows) |
|---|---:|---:|---|
| water | 2,642,954 | 2,650,000 | part_section 456,709 (65,237), runs 364,687, item 311,646, rule 269,756, part 249,022, str 185,992, static 153,875, display_rule 135,520 |
| near | 2,118,991 | 2,250,000 | place_water 2,119,790 (392,314 rows) |
| climate | 1,433,007 | 1,650,000 | gauge_clim 1,417,274 (2,393 blobs) |
| core | 1,388,309 (cand. 1,508,127) | 1,650,000 | item 420,292 (19,767), item_section 231,563 (65,237; cand. 81,357), item_near 126,844, place 113,558, item_area 111,411, name_source 59,901 |
| lines | 1,284,308 | 1,400,000 | section_line 1,299,334 (303 blocks) |
| regs-sid | 1,219,229 | 1,400,000 | section_akey 946,092, section_place 322,412 |
| down | 1,130,617 | 1,250,000 | section_down 1,162,984 (141,435 rows) |
| conditions | 916,049 (cand. 963,726) | 1,060,000 | section_gauge 881,892 (108,075; cand. 114,140), gauge 86,361 |
| water-shared | 745,781 | 850,000 | the region-wide tables of water (rule, static, display_rule, zdict …) shipped a second time |
| paint | 686,602 | 1,000,000 | binary |
| geo | 597,285 | 700,000 | part_geo 597,138 (21,044) |
| regs-1..8 | 155,247–431,810 | 1,500,000 each | |
| core-<r>, water-<r>, salmon-<r>, method, access, flow-area, salmon | ≤ 330,000 each | | |

Already near a gate: water (99.7 %), conditions after the fold, core after the fold.

## Candidate savings (to measure, nothing decided)
1. **Per-section rows that repeat their water's answer.** `item_section`, `part_section`, `section_gauge`,
   `section_down`, `section_akey` hold one row per section; a water's sections are mostly a contiguous run of sids
   with one value. Store runs (sid range → value) or the exceptions only. The folded ponds (D1) are the newest case:
   a pond section takes its river's item, gauge and key — derivable from the river, not stored.
2. **`section_gauge` columns.** `station` is TEXT on 108k rows (an integer index into `gauge` would do); `trust`
   is a TEXT enum.
3. **`near.place_water`** (392k rows, 2.1 MB gz): distance bands per place and water. Coarser `ckm`, a cap per
   place, or computing "near" from coordinates on the device.
4. **Duplicated payloads.** `water` and `water-shared` (and `core` vs `core-<r>`, `water` vs `water-<r>`) carry the
   same tables for the web's slices; measure what a phone downloads in total versus what it reads.
5. **`climate.gauge_clim`** blobs (1.4 MB gz for 2,393 stations): quantisation of the bands, fewer percentiles, or
   fetched per station when a chart opens.
6. **`lines`, `down`, `geo`**: delta/varint packing and coordinate precision (what accuracy the map needs).
7. **Strings**: `item.name`, `str`, `static`, `display_rule` — interning across files, a shared dictionary, or
   zstd with a trained dictionary instead of gzip where the platform allows.
8. **Tiles and basemap** (≈ 400 MB on disk): zoom ranges, attribute columns per layer, simplification per zoom.

9. **Fewer polygons** (owner 2026-11-12, after closed testing): fold more small widenings into the stream and draw the stream
   line instead of the polygon where it really is the stream (the 456 unnamed lakes a river runs through): T695.

## Work
1. Finish the survey: every shipped file (release files, tiles, basemap, the app bundle itself), raw / gz / who
   reads it and when (`contract.WEB_BUDGET` moments, phone offline total).
2. For each candidate above, measure the saving on the live release with a prototype (no shipped change).
3. Propose to the owner: a ranked list (saving, cost, risk to "one data flow" / the contract), then build what is
   picked as its own tickets.

## Acceptance
- A measured table of every shipped file and each candidate's saving, on one named release.
- The owner's pick recorded; budgets lowered again to fit what ships after the picked changes.

Depends: none (the T678 release ships first with the raised budgets). Size: M (survey + proposal). Owner: yes (the
pick). Default: no shipped change until the owner picks.
