# T1150 input for the hardening release (L30, 2026-11-25 ~20:00). Nothing built, nothing committed.

Owner's rulings:
- On scope: "just add all to hardening release".
- On his two accepted rows: "do whatever makes our current state decent until this is fixed in hardening".

Tonight that means: nothing changes, and his rows stay as they are.

## The generator reruns byte for byte

`pipeline.atlas.waters.added_streams.build_dataset.build(["burnaby","squamish","port_moody","osm","manual"], gpkg,
out_dir=<side>)`, run with today's code, rewrites `data/curated/waters/added_streams.json` byte for byte. The
generator writes the codes into that stored file (`streams[].wsc`, `receiver_wsc`). The atlas never recomputes
them, so any fix reaches the map only through a regenerated, reviewed `added_streams.json`: a curated proposal.

## The cause

- `coastal.estimate_coastal_wsc` (called at build_dataset.py ~1112) gives a city line that drains to the sea its
  nearest coastal FWA stream's level-1 and level-2 code.
- Its bump only avoids other ADDED codes (`used_coastal`), so the line can take an FWA line's exact code.
- The atlas then files code-equal pieces together: `graph.names.resolve_names` / `wsc.code_owner`, and
  `registry.build._wsc_gnis_map`.

## The 11 city lines that share an FWA line's exact code (of 36 that drain to the sea)

| group | city line (blk, code) | FWA line with the same code | what the registry does today |
|---|---|---|---|
| A: city line filed INTO a named water | Tunnel Creek (-2000000390, 900-078372) | Seymour River 360886339 | item gnis:6105 Seymour River holds it, alias "Tunnel Creek" |
| A | Turner Creek (-2200000392, 900-057239), with Turner Creek Trib.1 (-2200000393) below it | Noons Creek 360880849 (+ 360244416) | item gnis:23366 Noons Creek holds it, alias "Turner Creek" |
| A | Capitol Creek (-2000000141, 900-053687), with Capitol Trib.1 | unnamed FWA 360570081 (city label "Squatters Creek") | item wsc:900-053687 "Squatters Creek" holds both |
| B: FWA line filed under a city creek's name | Kask's Camp Creek (-2000000243, 900-053873) | FWA 360788071 (city label "Heron Creek") | item wsc:900-053873 "Kask's Camp Creek" holds both |
| B | Village Creek (-2200000395/-401, 900-058384) | FWA 360827018 (city label "Schoolhouse Creek North") | item wsc:900-058384 "Village Creek" holds both |
| B | Mamquam Blind Channel Wetland (-2100000079, 900-103611) | FWA 360767895 (city label "Logger's Lane Creek") | item wsc:900-103611 holds both |
| C: same code, separate items | Kyle Creek (-2200000296, 900-056279) | School House Brook 360728050 | separate items (blk:-2200000296 / gnis:38605) |
| C | Wilkes Creek (-2200000420, 900-057925) | Hett Creek 360724731 | separate (blk:-2200000420 / gnis:34553) |
| C | Aliceville Creek (-2000000091, 900-059187) | unnamed FWA 360731231 | the FWA line is in no item |
| C | Rainbow Creek (-2000000277, 900-076392) | unnamed FWA 360455980 | the FWA line is in no item |
| D: shared ON PURPOSE (probably) | Sheldens Creek (-2400000001, 950-016367, the hand-drawn line) | Sheldens Creek 360846083 | one item wsc:950-016367: the line continues FWA's creek to the road bridge (T806). Keep it shared |

## Two fix variants, measured by regenerating into a side folder

Neither variant changed lines, blks, names or name variants. Only `wsc` / `receiver_wsc` changed.

1. **Plain:** the bump also skips FWA codes. **83 of 320 rows change**, because every later city code along the coast
   shifts by one. `t1150-rowdiff-plain-bump.txt`.
2. **No cascade:** mint every tidal code as today, then move only the codes that ARE an FWA code to the nearest
   slot free of all FWA and all other minted codes (`coastal.off_fwa`, in `t1150-wip.diff`). With the switch off,
   the file is byte-identical. **31 of 320 rows change:**
   - the 11 colliders and their tributaries;
   - Sheldens Creek, which hardening must exclude;
   - the Logger's Lane chain (Loggers Lane Creek, Logger's Lane Creek Trib 4, Finch Creek), shifted by the
     generator's later uniqueness pass (`_bump_wsc`);
   - Village / Schoolhouse, which reshuffle: the 9 Schoolhouse Creek North fragments go from 9 codes to one.

   `t1150-rowdiff-no-cascade.txt`.

## Consequences hardening must handle

- **Five registry items keyed by code would get new ids:** wsc:900-053687, wsc:900-053873, wsc:900-058384,
  wsc:900-103611, and wsc:950-016367 if Sheldens is not excluded. Every curated match or mark naming them needs a
  resync (memory "Registry change -> entry resync").
- **Two owner-accepted rows in `data/curated/waters/unplaced_piece_calls.json` go stale:**
  - (`r2:noons_creek@2-8`, `noons_creek.r1`, `-2200000392:0`, flows_to_the_sea_unjoined)
  - (`r2:seymour_river@2-8`, `seymour_river.r1`, `-2000000390:0`, same)

  Once those pieces are their own waters, those rules no longer meet them. A stale accepted row STOPS the reach run,
  so both rows must be removed in the same change, with his OK.
- **Still to build:** the build-time check from the planning chat. It would refuse an added stream filed under an
  item whose own FWA line it neither touches nor comes within the generator's connect limit of (`_CONNECT_TOL` =
  80 m). Today's distances: Tunnel 1,068 m from Seymour, Turner 678 m from Noons (and 1,082 m from 360244416).
  Capitol and the group B lines were not measured. Also still to build: tests, the GOTCHAS entry and owner line, a
  side atlas, parity, and the binding diff.

## Side find, not judged

A city "West Noons Creek" duplicate name sits on the unnamed FWA line 360244416, which carries Noons Creek's code
and is in the Noons Creek item. FWA's gazetted West Noons Creek is 360595504.

## For the naming lane (0.10.0)

The names reach the bundle only per ITEM, never per section:
- `bundle.sqlite` `alias` (item_id, alias) and `name_source` (item_id, name, src, label): gnis:23366 → "Turner Creek"
  (City of Port Moody ESA streams layer); gnis:6105 → "Tunnel Creek" (City of Burnaby waterways layer).

The per-section link exists at build time:
- `section_measure` (sid, blk) gives sid 22 → blk -2200000392 (0-2198 m) and sid 86 → blk -2000000390 (0-215 m).
- `data/curated/waters/added_streams.json` `name_variants[]` with `kind: "added"` and `target_blk` equal to that blk
  gives "Turner Creek" / "Tunnel Creek". The same name is in `streams[].name` by `blk`.

So deliver can name the part from that blk, as a build-time read of the curated artifact. Nothing at run time.
