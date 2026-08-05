# Manual-review runbook — batch confirmation of `14-locators-to-curate.json`

**Fail-safe handoff.** This lets another agent (or a fresh, low-token session) continue the
human-in-the-loop batch review of point/boundary curation without re-deriving context. Everything
needed is either in this doc or already inlined in each locator row's `notes`.

> **For the COMPLETENESS pass, work waterbody-by-waterbody:** see
> **`docs/18-waterbody-split-curation.md`** + `oneoff/waterbody_splits.py`, which group every reg
> entry's splits together and flag any reg boundary with no curated row (`MISSING`). Use that to
> finish a waterbody's *whole* split set before authoring; this per-row runbook still governs *how*
> to resolve each row.

## What "review" means here
Most un-curated rows now carry an **auto-proposal** in `notes` (written by research subagents):

```
[auto-proposal H|M|L] <feature> on <water>. Candidate coord [lon,lat]. OSM: <url>. Verify: <one line>. <research notes>.
```

The job: present each proposal to the human with map links, ask a **multiple-choice** confirm,
and write the confirmed result back. The human decides; the agent never self-confirms.

## Current state (2026-07-30, post schema-migration)
- **Schema migrated** (commit 4a0c58c): anchor_kind = split type; old buckets moved to `resolver_hint`; `split_id` removed. Filter batches by `--hint`.
- **not_applicable audit DONE — verdict SOUND**: all 45 n/a rows are correctly classified. The tributary-SET rows ("X'S TRIBUTARIES") show mainstem splits in their reg text, but every such split IS captured as its own dedicated row (verified Campbell/Salmo/Brunette/Granby/Little Qualicum/Trout Lake/Lake Revelstoke/Premier). No reclassification needed — the earlier "issues" were mainstem rows still in `todo` (uncurated auto-proposals), not missing splits.
- **confluence** (hint=tributary) — DONE (92 curated / 42 not_applicable / 13 deferred / 3 manual, 0 todo).
- **point/dam_weir_fence** (43) — enriched, `todo`. 14 H reservoir-outlets, 9 M, 20 L (fences/hatchery weirs, coord=map-only).
- **point/bridge_road_km** (~99) — enriched, `todo`. 11 M FSR∩stream crossings, ~88 L public road/rail bridges (coord=map-only).
- **lake** (hint=lake_reach) — CLASSIFIED + auto-resolved (commit pending): 32 **auto** (bounded only by lake edge(s)/mouth/border — handled for free by the combine-phase lake split per docs/04, NO authored split needed), 8 **not_applicable** (whole/set-of-lakes), 15 **deferred** (9 lake-internal + 4 reservoir + 2 vague-map), 6 **todo** (lake bound is auto but an OFFSET or boundary-signs point still needs authoring: forsyth/jewel/yakoun/zymoetz + ruby/atnarko), 2 reclassified out (asher-creek→confluence, thompson-signs→point). Lake bucket now 69% done. Each row carries a `[lake-class: ...]` / `[auto-lake-split]` note.
- Still bare `todo`: `other_reach`, `line_between_signs`, `map_or_vague`, `radius_buffer`, `except_negative`, `area_park_polygon`, `boundary_signs_generic`, a few `falls_canyon_obstacle`.
- **Authored splits**: `stream_sections/splits.json` (12). 18 locators reference them via a
  `authored split: <id>` line in `notes` (curated). Those authored coords/targets are VERIFIED and
  should be reused to fill any matching un-curated locator rather than re-resolved.

## Schema note (post-migration)
`anchor_kind` now equals the split `anchor.type` (`point | confluence | line | lake |
area_boundary | mu_boundary | lake_io | buffer | not_a_split | unclassified`). The old granular
bucket moved to `resolver_hint` (`falls_obstacle | dam_weir_fence | bridge_road_km |
boundary_signs | tributary | lake_reach | between_signs | park_polygon | …`). The `point` bucket
is large, so filter batches by `--hint`. The `split_id` field is gone — authored-split links live
in `notes` as `authored split: <id>`.

## Tooling
```bash
# progress table + next 10 open items (deferred sort last)
.venv/bin/python -m stream_sections.oneoff.curation_status
.venv/bin/python -m stream_sections.oneoff.curation_status next 12 --hint dam_weir_fence
.venv/bin/python -m stream_sections.oneoff.curation_status review not_applicable --kind confluence   # audit a bucket
.venv/bin/python -m stream_sections.oneoff.curation_status defer <id> --reason "..."                  # park unsolved -> back of queue
.venv/bin/python -m stream_sections.oneoff.curation_status show <id>
```
Statuses: `todo | curated | manual | not_applicable | deferred`. Done = curated+manual+not_applicable.

## Offline labelling (no agent / no API needed) — for QGIS sessions & travel
Two front-ends, both produce results an agent consumes on return. Scripts live in `stream_sections/oneoff/`.

**A. Interactive CLI (QGIS-friendly).** Walks the queue **easiest-first** (rows with a candidate
coord, best confidence first), shows *what to find* + OSM/Google/Satellite links + `target`
(blk/wsc/wbk to locate in QGIS/FWA), and you skip or update inline. Decisions go to a **separate
file** (`output/review_decisions.json`) — the live doc is **not** touched, so you apply them
deliberately on return.
```bash
.venv/bin/python -m stream_sections.oneoff.curation_status label --hint dam_weir_fence --easy
#   y=accept candidate · c <lon,lat>=set coord (paste from QGIS) · d/x/m=defer/not-a-split/manual
#   t wbk=..|blk=..|wsc=..=set target · n <note> · a <text>=flag for agent · u=undo · enter=skip · q=quit
#   --easy hides rows without a candidate; --out overrides the decisions path
```
(Reading the coord via a prompt avoids the argparse `--coord=-118..` negative-number gotcha.)

**On return**, feed the decisions back into the doc:
```bash
.venv/bin/python -m stream_sections.oneoff.curation_status apply decisions.json
#   correct->curated(+coord) · wrong+coord->curated · not_a_split->not_applicable · defer->deferred
#   'wrong' with no coord is reported as NEEDS AGENT for hand-finishing.
```
Rows flagged `[for-agent]` (CLI `a`) or reported by `apply` are the ones an agent finishes (two-boundary
splits, confluence `wsc`, offsets) — grep the notes for `[for-agent]`.

## The review loop (batches of ~4)
1. Pull the next `todo` rows for a bucket that have a `Candidate coord` in `notes`.
2. For each, parse the candidate `[lon,lat]` from `notes` and build **all three** links (remote BC
   streams are often blank in OSM, so always include Google + satellite):
   - OSM: `https://www.openstreetmap.org/?mlat=LAT&mlon=LON#map=15/LAT/LON`
   - Google pin: `https://www.google.com/maps/search/?api=1&query=LAT,LON`
   - Satellite: `https://www.google.com/maps/@LAT,LON,15z/data=!3m1!1e3`
3. In the assistant message, show each item: reg wording (`locator_text` / `full_regulation`),
   the candidate, the three links, and the "Verify:" hint. Then call **AskUserQuestion**, one
   question per item (`multiSelect:false`), options `[Correct, Wrong point, Skip/Defer]`
   (recommend `Correct` first for H-confidence). Template:
   ```json
   {"question":"<reg>: <feature> at [lon,lat]. Correct?","header":"<≤12 chars>","multiSelect":false,
    "options":[{"label":"Correct","description":"Write it (curated)."},
               {"label":"Wrong point","description":"Off — investigate."},
               {"label":"Skip","description":"Defer."}]}
   ```
4. Write per the answer (rules below). Commit every batch or few: `rtk git add …; rtk git commit -m "…"`.

## Writing rules (by row kind)
- **single point** → set `coord`, `status="curated"`, `label`; `target` optional.
- **two boundaries** ("from A to B") → REPLACE the row with distinct `-a`/`-b` rows (copy all
  fields, one coord each). The sectionizer emits "between A and B" automatically. No reference rows.
- **confluence** → set `target:{blk,wsc}` of the **tributary** (authored as a `confluence` anchor
  keyed on `tributary_wsc`, which self-validates); coord is only for the human's eyeball.
- **point/confluence + offset** ("N m up/down of X") → `target` = the anchor, plus record
  `offset_m` + `offset_dir` in the note (authored as anchor + the along-channel offset op, see
  `splits.schema.md`). One row per distinct boundary point.
- **lake boundary / whole lake** → `target:{wbk}` (a `lake` anchor; the lake already splits the
  BLK, so it's a no-op cut kept for the label — cf. `adams_lake`). **Lake-INTERNAL** splits (a lake
  divided by a bridge/line) are NOT implemented yet (docs/15) → record the coord but flag.
- **not a split** → `status="not_applicable"` + reason. This covers: whole-stream regs (reg applies
  to the entire named creek), tributary-SET regs ("X's TRIBUTARIES"), and EXCEPT/"see X, a
  tributary" exclusions.
- **unresolvable** → `status="deferred"` (sorts to back of queue) with a `[deferred] <reason>` note.

## Gotchas (hard-won)
- **Name collisions**: many names repeat across BC (Salmon/White/Thompson/Goat/Glacier/Bear/Coal).
  A candidate may be the wrong same-named feature — the note flags the correct one; confirm by region/MU.
- **`[Includes Tributaries]`** on an A→B reach STILL splits the mainstem at each named confluence
  (the tributaries just inherit the reg). Pure "X's TRIBUTARIES" sets do NOT split → not_applicable.
- **EXCEPT / "see X, a tributary"** = a tributary EXCLUSION, not a split → not_applicable.
- **Shared split**: several reg rules can name the SAME boundary → write the same coord/target on
  each and note the shared split; author ONE split.
- **Mis-bucketing**: a row whose SUBJECT is a lake ("HIGH LAKE (… north of Bridge Lake)") is a lake
  target, not a bridge — reclassify `anchor_kind` to `lake_reach`. "Bridge/CPR bridge/Hwy N bridge"
  in a *lake* row usually means a lake-internal divider, not a stream crossing.

## Re-resolving from scratch (if you need coords the notes don't have)
The confluence resolver = tributary **mouth** (min-`DOWNSTREAM_ROUTE_MEASURE` segment's `coords[0]`,
which FWA orients mouth→source) validated by **parent-at-mouth** (nearest named stream ≠ self):
```python
from data.data_extractor import FWADataAccessor            # data/bc_fisheries_data.gpkg, streams layer, EPSG:3005
from stream_sections.cutting import line_coords_2d          # handles MultiLineString
from pyproj import Transformer                              # 3005 -> 4326, always_xy=True
```
Offsets: project the anchor point onto the target's merged blue line, walk `offset_m` along it
(+ = upstream since geometry is mouth→source), clamp to [0, length]. See `stream_sections/anchors.py`
`_apply_offset` and the `point`/`confluence` branches. UTM in reg text is **zone 9** for most of BC
(verify: zones 10/11 land in the wrong place).

## Commit convention
Branch `redesign/stream-sections`. Small logical commits; co-author trailer
`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. `name_variants.json` is a real
tracked artifact (compiled from `oneoff/name_variants_compile.py`, incl. its `_MANUAL` list) —
commit changes to it normally alongside the compiler edit that produced them.
