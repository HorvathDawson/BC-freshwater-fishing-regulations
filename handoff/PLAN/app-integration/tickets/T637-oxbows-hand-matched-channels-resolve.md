# T637 — Okanagan River Oxbows: the owner's hand-matched side channels must bind again

Status: IN PROGRESS — f4069759f, 216a7e60a, 070230ff5, 298e77cb4; owner ANSWERED 2026-11-12 (Whiteswan: every inlet closed, T690); left (Claude): restamp matched (oxbows), one rebuild with T623/T660, reach + binding diff, promote

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

## Built 2026-11-11 (f4069759f) — owner decisions 1–4
Owner 2026-11-11: fix = option A; drop Park Rill's code; Osoyoos lines whole, east-slope lines cut as a proposal;
the 6 March pieces as a proposal.

**What changed**
- `data/curated/waters/name_variants.json`: one entry, target `{"wscs": [the 26 trimmed codes], "kind": "stream"}`, name
  "Okanagan River Oxbows", source `regulation`, `display: true`. No registry code change.
- `data/curated/regulations/book_name_links.json`: `300-432687-516449` (Park Rill) out of the override. No other hand
  match removed. The note's "32 … 1 linear feature ID" count is left as it was (now 26 codes / 40 wbks / 0 lfid).
- `data/curated/regulations/synopsis/region-8.json`: `r8:okanagan_river_oxbows@8-1` `matched` restamped
  (`backfill_matched --restamp`, side registry). Needed: the reach binds the entry's `matched`, not the override, so the
  name alone bound only the lines that swallowed a polygon (first run: +31). Ids only; no rule or value changed
  (region files backed up first, diff = id lines only).

**Measured (side atlas `data/generated/atlas/full_next`, reach run in the scratchpad)**
- Registry: 26 new stream items `wsc:300-432687-…` named "Okanagan River Oxbows", 65 sections (47 stream, 18 polygon).
  The slough fold (T322, `slough_calls.json`) now JOINS 18 of the 40 oxbow polygons to the named line threading them
  (they were "unnamed line" items before): `wbk:328985783, 329460050, 329460329, 329460670, 329460748, 329461070,
  329461388, 329461584, 329461983, 329462043, 329462114, 329462220, 329462306, 329462319, 329462395, 329462424,
  329462438, 329462470` are now aliases of the line items. The other 22 polygons keep their own
  "OKANAGAN RIVER OXBOWS" items. One more polygon the line 459615 threads, `lake:329461947` (not one of the 40), joined
  the 459615 item the same way and is now bound by the oxbow rule.
- The name lands on no lake: no lake/wetland item was renamed or gained a variant (registry diff: 26 added, 18 removed
  as above, 6 changed — all area items).
- The graph keeps 24 sections it pruned before (`section_handles` 1,958,282 → 1,958,306, none lost): 4 on the oxbow
  lines themselves, 20 on unnamed child lines draining into them (the leaf prune keeps a line that reaches named water).
  Boundaries 27,422 → 27,427.
- Oxbow rule `okanagan_river_oxbows.r1` (exempts from the spring stream closure): 40 → 88 sections (+48, −0); all 65
  sections of the 26 items bound. Park Rill (gnis:22618) was not bound before (the entry's `matched` never held it)
  and is not now.
- Binding diff outside the oxbows (3,376 rules): 89 rules +24 and 1 rule +1 (`zp:superior_closures.r3`, Hayne's
  Lease ecological reserve) — exactly the 24 new sections, nothing removed, no other change: Region 8 zone rules,
  provincial rules, the parks/WMA/IR areas holding them. DFO: `8:default:region` gains the same new sections. Licensing,
  steelhead presence, tidal, outside-BC, unresolved and extents tables are byte-identical.
- Reach run on `full_next`: passes (rc 0, digest d27eaf60cac16a60; dead-override check empty; the 2 unplaced pieces
  are the accepted ones). `python -m pipeline.regs.matching.matcher --registry …/full_next/registry.json` passes. The
  default (live registry) run still fails until the atlas is promoted.

**What the binding means (owner, read once).** The oxbow row is an EXEMPTION from the spring stream closure, so the 48
added sections are now open in spring: all 26 lines, whole, from Osoyoos Lake (49.07) to below Vaseux Lake, including
the two east-slope reaches below. The book's words are "isolated oxbows adjacent to Okanagan River downstream of the
McIntyre Dam and upstream of Vaseux Lake"; the hand match runs well south of McIntyre Dam. That is the owner's match,
kept as is; flagged only.

**PROPOSAL A — cut the east-slope parts (owner ticks from the maps).** No curated file has a proposed-vs-accepted
split for name variants or cuts, so both lines are bound WHOLE now. Proposed cut points (last point within 400 m of
the mainstem or 150 m of an oxbow polygon):
- `300-432687-500162`: blk 356502967 at 685 m (49.15650, −119.54386); keep 0–685 m, drop 685–2,452 m (1.77 km).
  Map `T637-shots/fix-cut-500162.png`.
- `300-432687-466472`: blk 356559193 at 1,542 m (49.09022, −119.52724); keep 0–1,542 m, drop 1,542–3,080 m (1.54 km).
  Map `T637-shots/fix-cut-466472.png`.
- On a tick: two cuts in `cuts.json` at those measures, and the variant's target for those two codes becomes
  `{"blks": [...], "reach": {"from_m": 0, "to_m": <cut>}}` (a `reach` window needs a `blks` target and a cut at its
  bound); then atlas rebuild + reach + diff. 459615 and 461418 (into Osoyoos Lake) stay whole, as decided.

**PROPOSAL B — the 6 pieces commented out 2026-03-05 (not live).** The only way is live-or-absent, so they stay
absent. Maps: `T637-shots/fix-restore-south.png` (5 child codes), `T637-shots/fix-restore-lfid.png` (the lfid).
Exact diff on a tick:
- `book_name_links.json` OKANAGAN RIVER OXBOWS `fwa_watershed_codes` += the full 21-group codes of
  `300-432687-461418-400917`, `300-432687-463105-427876`, `300-432687-461418-565942`, `300-432687-466472-328926`,
  `300-432687-476281-770576`; and `name_variants.json` oxbow entry `wscs` += the same 5 trimmed codes; then
  `backfill_matched --restamp r8:okanagan_river_oxbows@8-1`, atlas rebuild, reach, diff.
- lfid 707367498 (blk 356440949, 153 m, edge 1400, code 999-…, inside unnamed lake wbk 329461526 near Oliver): the graph
  has NO section on that blk, so no name or override can bind it. Restoring it would mean `waterbody_keys` +=
  `329461526` (the lake that holds it) — a different water from the line; owner's call.

**What promote needs (not done; no release, tiles or staging touched).** `python -m pipeline.atlas.promote
data/generated/atlas/full_next` (parity report: DIFFERS = exactly the items above), then `pipeline build` (reach +
deliver; tiles for the new labels/sections), then the release binding diff. Until then the live registry has no item
for the codes, so `scripts/fast-fail.sh` (its matcher step) and a live reach run still stop on the oxbows.

**Not verified**
- No bundle, verdicts, answers, export or tiles were built on `full_next`; the app's view of the oxbows is unseen.
- The cut points are a distance rule, not a field check; the owner's map look decides them.
- Full pytest not run; ran test_name_variants, test_names, test_matcher, test_backfill_matched, test_check_curated,
  test_slough_fold, test_registry_flowing, test_reach_drops_nothing (157 passed).

## Owner ticks 2026-11-11 — applied to curated files (216a7e60a), NOT built
Owner: (1) both east-slope cuts ACCEPTED; (2) restore the five child codes, "but one will need a hill cut"
(461418-565942) — that cut accepted in advance ("i accept your cut for new hillside"); (3) the lfid / pond near Oliver:
LEAVE OUT. Coordinator: no atlas rebuild, reach run or promote here (one rebuild later with T623/T660).

**Applied**
- `cuts.json`: three point cuts, one waterbody group per line (`applies_to: {blk}`), ids
  `okanagan_river_oxbows__500162_foot_of_east_slope`, `…__466472_foot_of_east_slope`,
  `…__461418_565942_foot_of_west_slope`. Same rule for all three (last point within 400 m of the mainstem or 150 m of an
  oxbow polygon); each coord projects onto its blk at the stated measure (checked against the gpkg, ≤ 0.2 m off):
  | line | blk | cut at | kept | dropped | map |
  |---|---|---|---|---|---|
  | 300-432687-500162 | 356502967 | 685 m | 0–685 m | 1.77 km (685–2,452) | `T637-shots/fix-cut-500162.png` |
  | 300-432687-466472 | 356559193 | 1,542 m | 0–1,542 m | 1.54 km (1,542–3,080) | `T637-shots/fix-cut-466472.png` |
  | 300-432687-461418-565942 | 356530115 | 612 m | 0–612 m | 0.74 km (612–1,350) | `T637-shots/fix-cut-461418-565942.png` |
- `name_variants.json`: the wsc entry loses 500162 and 466472 and gains the four uncut child codes
  (461418-400917, 463105-427876, 466472-328926, 476281-770576); three `{"blks": [blk]}` + `"reach": {"from_m": 0,
  "to_m": <cut>}` entries name the three cut lines from the mouth to the cut.
- `book_name_links.json`: the five child codes back in the override (31 codes, 40 wbks); the note now reads
  "Includes 31 watershed codes, 40 waterbody keys, and 0 linear feature IDs." (counts only).
- lfid 707367498 (blk 356440949) and its pond wbk 329461526: left out by the owner; nothing added.
- `check_curated --waters`: 0 errors (the 4 "not in the current atlas registry" warnings are the new targets).
  `load_split_defs` reads the three cuts.

**STOP — the cuts will not take the hillside out of the oxbow rule (proved from the code, not built).** A cut + a
reach-window name ends the LABEL at the cut, but the registry still holds the hillside piece in the line's item:
- `registry/build.py` `item_id`: a stream node with no gnis is keyed `wsc:<its code>`; the hillside piece and the named
  valley piece share the code, so both land in the same group (`build_registry`, "group nodes by item_id").
- `split_distinct_names` moves only a node with a display name of its own (`movable` needs `n.display_name`); the
  hillside piece has none (no gnis, no gazetted sibling of its code, and `follow_connections` skips curated blks).
- `build_registry` keeps every node of the group in `section_ids`; nothing drops a nameless node from a named item.
- The leaf prune does not remove it either: it sits 1 hop above named water (`min_hops` 3).
So after the rebuild the oxbow row (a spring-closure exemption) still binds the three hillside reaches (≈4.05 km). The
curated part is in and harmless; the binding effect needs one of:
  a. a registry change: a `wsc:`/`blk:`-keyed item (no gnis) keeps only the nodes that carry a name; its nameless
     nodes go back to no item, as before T637 (zone rules only). Needs a full rebuild + binding diff across all wsc items.
  b. the oxbow rule's extents bounded by the three cuts — a corpus change to a rule, not Claude's to make.
Option A was implemented as decided; per the brief I stopped here instead of switching mechanisms.

**Matched restamp waits for the rebuild.** The entry's `matched` (restamped in f4069759f against the first side
registry) does not hold the five child items; it must be restamped against the NEW registry or they will not bind.

**Single rebuild → reach → promote (for the coordinator, once T623/T660 are in):**
```
cp data/curated/regulations/synopsis/region-*.json <scratch>/backup/          # AGENTS ⛔2
.venv/bin/python -m pipeline.tools.audit.check_curated --waters
scripts/heavy.sh atlas 50 -- .venv/bin/python -m pipeline.atlas.build --full --out data/generated/atlas/full_next
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched \
    --registry data/generated/atlas/full_next/registry.json --dry-run          # oxbow: matcher-only = the new items
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.backfill_matched \
    --registry data/generated/atlas/full_next/registry.json --restamp r8:okanagan_river_oxbows@8-1
git diff data/curated/regulations/synopsis/region-8.json                       # id lines only
.venv/bin/python -m pipeline.regs.matching.matcher --registry data/generated/atlas/full_next/registry.json
scripts/heavy.sh reach 50 -- .venv/bin/python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_next \
    --out <scratch>/reach_next --against <a reach run on the live atlas with the same corpus>
.venv/bin/python -m pipeline.atlas.promote data/generated/atlas/full_next --dry-run   # parity
.venv/bin/python -m pipeline.atlas.promote data/generated/atlas/full_next
scripts/heavy.sh build 50 -- .venv/bin/python -m pipeline build                # reach + deliver + tiles
```
Expect in the diff: the 5 child items (`wsc:300-432687-461418-400917`, …) bound by `okanagan_river_oxbows.r1`; 3 new
cut boundaries; the three hillside pieces STILL bound unless (a) is done first.

**Seen, not acted on:** line 461418 (one of the two the owner kept whole as "into Osoyoos Lake") also climbs the west
slope at its upper end — 0.73 km of m 3,048–3,962 lies beyond the same distance rule (visible top-left of
`fix-cut-461418-565942.png`). 459615's upper end has 1.0 km beyond it too (m 1,792–2,980). Owner's call.

## Fix (a) built + two more cuts (2026-11-11; 070230ff5, 298e77cb4) — still not built into an atlas
Coordinator 2026-11-11: make (a) (the owner decided the outcome: hillside parts leave the oxbows; (b) would change the
corpus). Owner also ticked "Cut both" for the upper ends of 461418 and 459615 ("lake ones okay").

**The registry change** (`pipeline/atlas/registry/build.py`, `CURATED_NAME_ITEMS_KEEP_ONLY_NAMED`,
`drop_nameless_from_curated_items`, run right after `split_distinct_names`): an item keyed `wsc:` / `blk:` (no gnis
on its code, so it exists only because a curated name named some of its pieces) keeps only the pieces that carry a
name (display name or any name tuple). Its nameless pieces go back to no item (zone rules only). `gnis:` items and
lakes are untouched; a group with no named piece is left to the named-items filter as before.
Tests (`test_registry.py`): named-below / nameless-above-the-cut; an uncut whole-line name keeps every piece; a
gazetted river's nameless piece stays; mutation pin (constant False puts the hillside back). With the rule switched
off in the file, the cut test FAILS (checked), the others pass.

**What else it touches today** (dry computation on the live atlas `graph.pkl`: same grouping as `build_registry` —
`item_id`, `drop_ditches`, `split_distinct_names` — no registry written): 4 items, 5 pieces leave.
| item | pieces leaving | what they are | non-zone rules on them today |
|---|---|---|---|
| `wsc:920-627155-045026-744802` Quinsam Diversion (gauge name on blk 354089001) | 2 (blk 354141307, 3.2 km) | another unnamed FWA line of the same code | `quinsam_river.r3`, `.r4` (Quinsam River ✱ row, tributaries) — the walk is graph-based, so these most likely stay bound by the walk; NOT verified |
| `wsc:300-625474-814939-312002` Whiteswan Lake outlet stream (reach 0–3,023.5 m) | 1 (`356560775:8623`, the INLET above the lake) | the curator's note says this piece "must not be called the outlet" | **`whiteswan_lakes_inlet_outlet_streams.r1` (No Fishing except below the falls) and `white_river.r2–r4`** — see below |
| `wsc:900-076392` Rainbow Creek (Burnaby added stream) | 1 (FWA blk 360455980, 1.5 km) | a different creek 1.5–3.0 km away that shares the code | none (zone only) |
| `wsc:900-059187` Aliceville Creek (Burnaby added stream) | 1 (FWA blk 360731231, 1.1 km) | a different creek 0.9–1.9 km away that shares the code | none (zone only) |

**Doubt — Whiteswan inlet.** "Whiteswan Lake's inlet/outlet streams: No Fishing EXCEPT the outlet stream downstream of
the falls" binds the item `wsc:…312002` upstream of the falls split. Today the inlet piece above the lake rides in that
item only through the shared code, so the closure covers it by accident. Under (a) it leaves the item, and the No
Fishing would no longer reach the inlet stream (a closure losing water). `white_river.r2–r4` reach it as a White River
tributary and most likely keep it (graph walk; not verified). Fix options for the owner: name the inlet piece (a
`name_variants` blk + reach entry for 8,623 m → end, e.g. "Whiteswan Lake inlet stream") and add it to the entry's
`matched`, or accept the loss. The single rebuild's diff must be read for this rule.

**The two new cuts (accepted; lake ends stay whole):**
| line | blk | cut at | kept | dropped | map |
|---|---|---|---|---|---|
| 300-432687-461418 | 356528832 | 3,232 m | 0–3,232 m | 0.73 km (3,232–3,962) west slope | `T637-shots/fix-cut-461418.png` |
| 300-432687-459615 | 356529638 | 1,967 m | 0–1,967 m | 1.01 km (1,967–2,980) east slope | `T637-shots/fix-cut-459615.png` |
(459615's upper end climbs the EAST slope, not the west as said earlier.) Coords project onto their blks at the
stated measures (≤ 0.2 m off). Five cuts in all now; `name_variants.json`: 26 codes named whole (22 channels + 4 child
codes), 5 blk + reach windows (500162, 466472, 461418-565942, 461418, 459615).

**What the single rebuild's binding diff should show** (vs a live-atlas reach run on the same corpus, after the
`matched` restamp): `okanagan_river_oxbows.r1` binds the 22 whole channels, the 4 whole child codes, and the five cut
lines only up to their cuts; the five hillside pieces (≈5.8 km: 1.77 + 1.54 + 0.74 + 0.73 + 1.01) are NOT bound by it
and carry Region 8 zone rules (spring stream closure) instead; 5 new cut boundaries; the 20-odd unnamed child streams
kept by the leaf prune carry zone rules only. Outside the oxbows: the 5 pieces above leave their items — expect
`whiteswan_lakes_inlet_outlet_streams.r1` to lose `356560775:8623` unless the inlet is named first, and check
`quinsam_river.r3/r4` and `white_river.r2–r4` keep their pieces.
The command sequence above stands; (a) runs inside the atlas build, no extra step.

## Whiteswan inlet kept under its closure; Quinsam checked (2026-11-11)
Coordinator: a closure must not lose water as a side effect of (a).
- `name_variants.json`: blk 356560775 from 8,623 m (the lake edge; the section `356560775:8623`) up is named
  "Whiteswan Lake inlet stream" (source regulation, display; the book row is "inlet/outlet streams"). Named, it is no
  longer a nameless piece, so (a) keeps it, and it STAYS in the outlet's item `wsc:300-625474-814939-312002`: a gnis-less
  item never splits a differently named piece out (`split_distinct_names` moves only when the item has a gnis). So
  `r4:whiteswan_lake_s_inlet_outlet_streams@4-24` `matched` needs NO change (no region file written) and
  `whiteswan_lakes_inlet_outlet_streams.r1` should bind exactly what it binds today. Pinned:
  `test_a_second_curated_name_on_a_curated_name_item_stays_in_it`. Side effect: the outlet item now also answers to
  "Whiteswan Lake inlet stream" as a variant, and the inlet section is labelled with its own name.
- Quinsam Diversion's 2 pieces (blk 354141307): `quinsam_river.r3/r4` bind them only through the tributary walk (the
  entry's `matched` is `gnis:27883` alone; the run's diagnostic: walk added 294 of 309 sections). The walk is a function
  of the graph, the entry's own water and `kind_of`; (a) changes none of them for these pieces (same nodes and edges,
  not the Quinsam's own water before or after, and `kind_of` falls back from the item's kind `stream` to the node's kind
  `stream`). So they stay bound. Read from the code and the run's diagnostics, not by a walk run.
- Rebuild check: `whiteswan_lakes_inlet_outlet_streams.r1` and `quinsam_river.r3/r4` unchanged in the binding diff.

## Build 27 verification (2026-11-11, read-only; atlas full_next, reach $TMPDIR/b27/reach_next vs live reaches/full)
- `okanagan_river_oxbows.r1`: 40 → 89 (+49, −0). All 31 "Okanagan River Oxbows" items (22 whole channels, 4 whole child
  codes, 5 cut lines) are bound section for section. The +49 are those items' sections plus `lake:329461947` (the
  polygon the 459615 line threads, joined by the slough fold — as predicted); nothing else.
- Cuts resolved at 685.03 / 1541.77 / 611.93 / 3231.68 / 1967.71 m. Above the cut, unbound: `356502967:685`
  (685–2,453), `356559193:1541`/`:1938`/`:2264` (1,542–3,081), `356530115:611` (612–1,350), `356528832:3231`
  (3,232–3,962). Below the cut, bound.
- **Not predicted — 459615 above 1,516 m is not in the graph at all.** The leaf prune removes the unnamed head of
  blk 356529638 (1,516–2,980 m) because reach-window names are applied AFTER the leaf prune (`atlas/build.py`: plain
  variants before the prune, `_nv_reach` after). So the valley part 1,516–1,967 m (≈0.45 km) is not bound by the
  oxbow rule, and the hillside above it carries no rule at all. That is how the live atlas already was (the same 5
  sections, ending at 1,516 m), so nothing is lost against live; it falls short of "mouth to cut". Options: (i) accept;
  (ii) the leaf prune protects the blks a reach-qualified variant names (`prune_leaves(..., protected=…)`, an atlas
  code change and a rebuild): the valley piece then binds and the hillside stays as a nameless zone-rule piece;
  (iii) nothing curated can fix it alone.
- z8 zone rules +21, −0: all 21 are new section ids. 4 are the hillside pieces the cuts made
  (`356502967:685`, `356559193:1541`, `356528832:3231`, `356530115:611`; the piece below keeps its old id); 2 are oxbow
  line pieces the prune now keeps (`356533490:89`, `356567044:593`); 15 are unnamed child streams the prune now keeps
  because the channels they drain into are named. None of the 21 is owned by any water item except the 2 oxbow pieces.
- Whiteswan `whiteswan_lakes_inlet_outlet_streams.r1`: 2 → 2, identical. Quinsam `r3`/`r4`: 309 → 309, identical;
  `white_river.r2`: 2,420 → 2,420.
- Fix (a)'s side effects: exactly the predicted pieces left their items (Quinsam Diversion 2, Rainbow Creek 1,
  Aliceville Creek 1; the Whiteswan inlet stayed, named). The 138 rules binding those pieces or the inlet bind them
  identically before and after.

## Owner answer on the Whiteswan inlet (2026-11-12) — ANSWERED
Verbatim: "It is this (no Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No
Fishing Aug 1-Mar 31, rainbow ) so not the inlet. The outlet is below the falls. Inlet is all no fishing (every inlet)
trout daily quota = 5 (catch and release all other species) when open)". The name and the piece were right (an inlet, so
closed); the closure now covers EVERY inlet of the lake, not only this one: ticket T690 (c41d54742), RULINGS E26.

