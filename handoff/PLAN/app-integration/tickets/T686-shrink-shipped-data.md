# T686 — Shrink shipped data across the board

Status: IN REVIEW — survey and prototypes done 2026-11-14 (r-f749475d0962), ranked below; owner: OWNER-TODO "T686: pick the shipped-data shrink"

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
8. **Tiles and basemap** (≈ 412 MB on disk): zoom ranges, attribute columns per layer, simplification per zoom. DEFERRED (owner 2026-11-13, with stocked lakes and bathy map): own ticket T699.

9. **Fewer polygons** (owner 2026-11-12, after closed testing; DEFERRED 2026-11-13): fold more small widenings into the stream and draw the stream
   line instead of the polygon where it really is the stream (the 456 unnamed lakes a river runs through): T695 (deferred).

## Survey finished + prototypes (2026-11-14, live release r-f749475d0962; nothing shipped)
Read-only; scripts and raw outputs in the session scratchpad (`pipeline-lane/t686/`, `out/*.json`). The build ships gzip
-9 (`release/build.py`), so every saving below is against -9 (baselines match the manifest within 15 B); per-table
figures are -6 with the table's indexes, which is why `core.item_section` reads 602,817 (its `section_by_item` index is
a second copy of the table).

**What ships.** Phone offline download 463,357,936 B: tiles 197,196,706 + basemap 225,356,297 (together 91 %), synopsis
PDF 11,722,042, map fonts/sprites 11,268,308, release data 17,752,020 gz (OFFLINE_GATE 20 MB). So candidates 1–7 move
the phone total by about 1 % at most; they matter for the web moments and the per-file gates. Web moments (measured /
gate): first_paint 803,585 / 1.15M; cold_load 3,060,555 / 3.35M; first_search 1,508,774 / 1.65M; first_tap 4,155,870
/ 4.2M (99 %); first_tap_low 3,316,297 / 3.4M; first_tap_unnamed 5,974,054 / 6.2M; conditions_open 1,096,128 / 1.15M.
Not priced in any moment: the web JS bundle (1,104,985 gz). App: aab 38,836,898, apk 62,224,024.

| file | gz | gate | read by | biggest tables (gz -6 alone, rows) |
|---|---:|---:|---|---|
| water | 2,573,799 | 2,650,000 (97 %) | phone; web prefetch | part_section 574,841 (81,399), item 310,091, rule 275,509 |
| near | 2,124,065 | 2,250,000 | town search | place_water 2,124,994 (392,344) |
| core | 1,508,774 | 1,650,000 | cold_load, first_search, first_tap | item_section 602,817, item 421,221, place 142,966 |
| climate | 1,433,006 | 1,650,000 | a chart | gauge_clim 1,433,493 (2,393 blobs) |
| lines | 1,283,926 | 1,400,000 | making / opening a link | section_line 1,300,765 |
| regs-sid | 1,244,776 | 1,400,000 | first_tap_unnamed | section_akey 968,841, section_place 321,958 |
| down | 1,168,958 | 1,250,000 | first_tap_unnamed | section_down 1,202,087 (145,779) |
| conditions | 965,642 | 1,060,000 | conditions_open | section_gauge 933,174 (114,408) |
| paint | 715,891 | 1,000,000 | first_paint, cold_load | binary |
| geo | 595,550 | 700,000 | first_tap | part_geo 595,386 (21,010) |

**Ranked** (saving gz; % of the phone's 17.75 MB release data; lossless = a round trip asserted in the prototype)
| # | candidate | saving | lossless | cost | risk |
|---|---|---:|---|---|---|
| 1 | C3 `near.place_water` as one varint blob per place (ckm delta, ord delta) | 990,537 (5.6 %; near −47 %) | yes | M: near writer, contract near/2, app NEAR_PLACE / RINGS / TOWNS queries | M: SQL becomes a decoder; no web moment moves |
| 2 | C5 climate bands transposed + float16 delta | 460,257 (2.6 %; climate −32 %) | yes | S/M: clim/3 writer + decoder | L: no web moment |
| 3 | C6/C1 `section_down` as varint blocks of 4,096 sids | 489,577 (2.8 %; down −42 %) | yes | M: down/2 writer + reader | M: first_tap_unnamed 5.97M → 5.48M |
| 4 | C1 drop the `section_by_item` index of `core.item_section` | 306,453 (first_search 1.51M → 1.20M) | yes | S: build DDL + contract | M: by-ord reads scan 81k rows (device time not measured) |
| 5 | C4 water.item.name = core.item.name (19,756 of 19,782) | 119,325 | only carrying the 26 that differ | M | M: water's reader needs core |
| 6 | C1 `water.part_section` runs by (ord, sid), m varint | 86,227 (water 97 % → 93 % of gate) | yes | M | M |
| 7 | C6 `geo.part_geo` box as corner + size | 80,902 | yes | S | L |
| 8 | C2 `section_gauge` tuple table + one int per sid | 55,284 | yes | S/M | L/M: readers need a join |
| — | C7 brotli -q11 per file, no data change | 3,659,564 (20.6 %; cold_load 3.06M → 2.44M) | yes | M/L | M/L: needs Content-Encoding br on the host; Hermes/OkHttp have no default brotli |
| ✗ | `core.item_section` dropped (same map as water.part_section) | 602,942 | — | — | H: core is read alone on first_search |
| ✗ | `lines` starts rounded to 10 m | 108,272 | no | — | breaks AGENTS 5 durable places |
| ✗ | RLE of regs-sid tables, strict gauge runs | ≤ 17 k or bigger | — | — | gzip already takes the runs |

Lossy variants measured and not recommended: near at 0.1 km (+121 k, 15,566 rows change words), near capped at 100 per
place (drops 172k rows), climate 6-bit mantissa (+228 k, 0.78 % error), geo at 10 µdeg (≈ 1 m). Ranks 1–8 sum to
about 2.59 MB (14.6 % of release data), NOT measured as one combined build, nor brotli on top of them. Not measured:
a trained zstd dictionary (no python zstandard; one solid stream of all files bounds it at 160–250 kB), climate fetched
per station, the folded ponds derived from their river, device decode times. Each format change keeps rule 56 (no fact
moves) and must be taught to the codec / contract (rule 58), with decoders that throw on bad input (W46).

**The owner's pick (asked 2026-11-14, not answered).** Which shrink work becomes tickets? (a) DEFAULT: ranks 1–3 (near
blob, climate bands, down blocks): ≈ 1.94 MB, all lossless, no web moment at risk, then lower those three gates;
(b) (a) plus 4 and 6 (first_search and water's gate get headroom; costs index scans on the device, to measure first);
(c) all lossless ranks 1–8 (≈ 2.59 MB); (d) brotli instead (≈ 3.66 MB, a host and native-client change, no format
change); (e) nothing now: the release data is 3.8 % of the phone download and tiles/basemap (T699) are the real size.

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

2026-11-15: `salmon-6` gate raised 320,000 → 420,000 (owner round 5, "Raise to 420 kB (Recommended)"; measured 379,295 B gz
once the Stikine and Taku places bound, `pipeline/deliver/release/build.py` `SIZE_GATES`). Shrink it back with the rest:
`salmon-6` is a candidate here (lower it again once the picked changes ship).
