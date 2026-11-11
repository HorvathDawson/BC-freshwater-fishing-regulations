# T637 — Okanagan River Oxbows: the owner's hand-matched side channels must bind again

Status: OPEN

Source: owner 2026-11-11, on T632 item 3: "what you mean they never had an item if they are unnamed side channels? i hand matched them?"

## Facts (measured 2026-11-11)
- The override `OKANAGAN RIVER OXBOWS` (data/curated/regulations/book_name_links.json) holds 40 waterbody keys and 27 watershed codes. Its note says "32 watershed codes, 40 waterbody keys, and 1 linear feature ID": 5 codes and the linear feature id are already gone from the file.
- The 40 polygons resolve. Of the 27 codes, none is in `section_handles.txt`; one is Park Rill's own code (300-432687-516449; the owner kept Park Rill separate 2026-11-03).
- The T632 agent's reading: the v2 registry gives an item only to named water, so a code of an unnamed line has nothing to point at. Not confirmed that the 26 lines exist as unnamed graph sections.
- Since T632 (8fdcb79d4) a dead override id stops the reach run and fast-fail, so builds stop here until this is fixed.

## Work
1. Find when the 5 codes and the linear feature id left the override (git history) and whether that was the owner's edit. Report; restore nothing without the owner.
2. Confirm in the graph that each of the 26 codes is a real unnamed line beside the Okanagan River, and its length.
3. Make a hand-matched watershed code of an unnamed line bind (registry items for these lines, or an override that binds sections by code). The owner's matches are kept, never deleted to make the check pass.
4. Park Rill's code: propose its removal from this override to the owner (it contradicts the 2026-11-03 decision), with a map.
5. Binding diff before and after; map of the 40 polygons and the channel lines for the owner.

Depends: T632. Size: M. Model: Opus. Owner: one tick (Park Rill code), one look at the map.

## Owner 2026-11-11 (later)
- "they should have name variants too of the oxbow name?": the owner expects the side channels to carry a curated name variant with the oxbow name. Measured: `data/curated/waters/name_variants.json` has no oxbow variant today (its one hit is "OXBOW LAKE", another water). Work item 6: search git history of the name-variant files (and any older variant store) for oxbow variants on these lines; report when and why they left. If they existed, restoring them is likely the fix (a variant names the line, so it gets an item) and goes to the owner as a proposal.
- Park Rill's code: the owner says it can be dropped from this override. Item 4 needs no further tick; drop it with the fix.

## Findings 2026-11-11
Investigation only: no code or curated data changed. Maps: `handoff/PLAN/app-integration/T637-shots/` (5 images over
Esri World Imagery, 800x1100. Blue = the 40 oxbow polygons, magenta = the 26 channel lines, orange = Park Rill, thin
white = the Okanagan mainstem. Lines come from the FWA `streams` layer in `data/source/bc_fisheries_data.gpkg`.)

**1. The 5 codes and the linear feature id.**
- The owner made the match on 2026-02-08 (ecba04070), as a `DirectMatch` in `scripts/fwa_modules/linking/name_variations.py`.
  It held 32 codes, 40 wbks and lfid 707367498. It moved to `regulation_mapping/name_variations.py` and then to
  `linking_corrections.py` (52b43fe63, 2026-02-25, "update name variation -> direct match").
- 25e70275c (2026-03-05) commented out 5 codes and the lfid with "# unresolved — need BLK", plus a TODO to add them back.
  It was one bullet of a 98-file refactor ("Comment out unresolved Okanagan River oxbow corrections", next to "Make
  linker -m resilient to DirectMatchError"). So a tool failure caused it, not a decision about the water.
- 87a3f9005 (2026-03-18, "ready for staging") migrated the corrections to `pipeline/matching/overrides.json`. The
  migration did not carry the commented lines over, but it copied the note unchanged. The entry has been 27 codes + 40
  wbks ever since: the same through the renames to `data/curated/regulations/overrides.json` (5c3319c01) and
  `book_name_links.json` (73c04dbc2).
- What left:
  - 5 child codes, each a short unnamed line that drains into one of the channels. Per FWA today:
    `…461418-400917` (97 m), `…463105-427876` (97 m), `…461418-565942` (1,350 m), `…466472-328926` (1,697 m),
    `…476281-770576` (424 m).
  - lfid 707367498: blk 356440949, 153 m, edge 1400, watershed code `999-…` (none). It sits inside wbk 329461526 near
    Oliver (49.189, -119.551), and that wbk is not one of the 40. The graph has no section on blk 356440949.

**2. Name variants.** None of the variant stores ever held a variant for these lines. I checked the history of
`data/curated/waters/name_variants.json` (from `stream_sections/name_variants.json`, 85a092647, 2026-07-26),
`pipeline/name_variants.json`, `pipeline/docs/name_variants_proposed.*`, and a pickaxe over all history for "oxbow".
The one oxbow variant is "OXBOW LAKE" (wbk 329459766), a different water. What the owner remembers is most likely the
v1 file *named* `name_variations.py`, where the hand match lived as a DirectMatch (above). The 2026-08-16 design
(`DESIGN-regs-to-sections.md` at 939990015, Layer B) already gave the fix: "nameless feature (oxbow side-channels) →
a `name_variants` entry names it → it becomes an item". Nobody wrote that entry.

**3. Did they ever bind?**
- In v1 and the March v2 pipeline, almost certainly yes. Both resolved override codes straight to stream lines by exact
  code (`_resolve_by_fwa_watershed_codes` in `regulation_mapping_v2/enrichment/feature_resolver.py`, later
  `pipeline/enrichment/`). The 27 codes stayed in after the "unresolved" sweep, so they resolved.
- ac3476d00 (2026-08-14) moved that resolver to `archive/`; the registry design replaced it. 939990015 (2026-08-17)
  made the registry keep named items only. Since then an unnamed line has no item, and the matcher sends these codes to
  `feature_pin`, a resolver that was never built.
- `matched` on `r8:okanagan_river_oxbows@8-1`: `gnis:22618` (Park Rill) only at 91afaa122 (09-02); 40 wbk + Park Rill
  from 296bfc14c (09-10); 40 wbk from bd0de3925 (10-07). It never held a code.

**4. In the current graph.** All 26 are real unnamed FWA lines. 38.3 km in all; each one's own lines start or end on
the Okanagan mainstem. They run from Osoyoos Lake (49.07) north to below Vaseux Lake (49.33).
- Their blks are graph sections: every blk has 1 to 5 handles in `section_handles.txt`, except 356370231 (part of
  558447), which has none. Earlier notes said "not in section_handles": that file lists blk:measure, not codes.
- In `registry.json` those sections belong only to area items (basin/MU). No stream item holds them, so the oxbow rule
  never reaches them; zone rules do. Park Rill's blk 356558044 is item `gnis:22618`.
- Length per code, by the last 6 digits of the code:

  | Code | Length | Code | Length | Code | Length |
  |---|---|---|---|---|---|
  | 459615 | 2,980 m | 461418 | 3,962 m | 463105 | 1,155 m |
  | 466472 | 3,081 m | 469486 | 4,508 m | 476281 | 1,190 m |
  | 476812 | 264 m | 478730 | 1,773 m | 480401 | 511 m |
  | 482026 | 307 m | 483280 | 459 m | 485563 | 1,120 m |
  | 487483 | 1,439 m | 494632 | 2,396 m | 499499 | 497 m |
  | 500162 | 2,453 m | 503666 | 549 m | 505976 | 807 m |
  | 517221 | 658 m | 518583 | 531 m | 521786 | 709 m |
  | 555555 | 2,049 m | 558447 | 3,651 m | 560023 | 163 m |
  | 564690 | 471 m | 565796 | 575 m | | |

- Not all of it is oxbow. 7.1 km lies more than 400 m from the mainstem and more than 150 m from any oxbow polygon.
  The worst cases: 500162 (1.8 km) and 466472 (1.5 km) climb the east slope; 459615 and 461418 run out into Osoyoos
  Lake; 469486 has 0.7 km of old meander. See `oxbows-south-osoyoos.png` and `oxbows-oliver.png`.

**5. Proposed fix.**
- **A (recommended): name the lines with a curated variant.** Add one `name_variants.json` entry: target
  `{"kind": "stream", "wscs": [the 26 trimmed codes]}`, name "Okanagan River Oxbows", source `regulation`,
  `display: true`. This is the Jeperson Side Channel / McArthur Island Slough pattern.
  - Effect: each line gets an item `wsc:300-432687-…` named "Okanagan River Oxbows". The override's codes resolve
    through `id_index` with no code change, and the lines get a map label, which is what the owner asked for.
  - The `kind: stream` filter matters. The channels thread lakes (for example wbk 329724406), and a wsc target without
    it renames the lake as well (the Long Lake / Docee case in `names._node_matches`).
  - Touches: one curated entry. Then an atlas rebuild (the name_variants input hash moves), a reach run and the
    binding diff.
  - Risks: (a) the slough fold (`registry/flowing.py`, T322) may join some of the 40 polygons to the newly named
    lines, so the slough gate and the binding diff must be read. (b) The off-floodplain reaches above get named too,
    unless the variant uses `blks` + `reach` windows instead.
- **B: let the override name its own lines.** One line in `pinned_by_override` (`pipeline/atlas/registry/build.py`):
  accept `wsc:` as well as `wbk:`. This is the Bluey-potholes mechanism (1f3aa6122), so it is general for any
  override-named unnamed line. It needs no curated edit, but it gives no map label, and it carries the same rebuild,
  slough-fold and off-floodplain risks as A.
- **C: the override binds sections by code in the reach run.** New code in the matcher and the reach run (the
  never-built `feature_pin` resolver). Largest change and test surface; not recommended for 26 lines.
- With any of these: drop `300-432687-516449` (Park Rill; the owner said 2026-11-11). Restore nothing else without
  the owner. The 5 child codes and lfid 707367498 are proposals only: they are short drains, and the lfid lies in a
  wbk outside the 40. Leave the note's "32 … 1 linear feature ID" count alone, or correct it to 26/40/0, as the owner
  says.

**Not established**
- Whether a v1 or March-v2 build that bound these lines was ever shipped. No build output from that period is in git.
- Why v1 could not resolve the 5 child codes; all 5 exist in FWA today.
- The 2026-08-17 `pipeline/docs/coverage/overrides_audit.md` marks 26 of the codes live (no ✗). The design doc in the
  same commit calls them dead. I did not rebuild that registry to settle which is right.
- Whether option A's names change any binding outside the oxbows. Needs the rebuild and the diff.
- Whether the book means the off-floodplain reaches to be oxbow water; that is an owner look at the maps.
