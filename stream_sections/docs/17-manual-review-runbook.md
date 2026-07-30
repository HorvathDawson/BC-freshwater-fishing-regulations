# Manual-review runbook — batch confirmation of `14-locators-to-curate.json`

**Fail-safe handoff.** This lets another agent (or a fresh, low-token session) continue the
human-in-the-loop batch review of point/boundary curation without re-deriving context. Everything
needed is either in this doc or already inlined in each locator row's `notes`.

## What "review" means here
Most un-curated rows now carry an **auto-proposal** in `notes` (written by research subagents):

```
[auto-proposal H|M|L] <feature> on <water>. Candidate coord [lon,lat]. OSM: <url>. Verify: <one line>. <research notes>.
```

The job: present each proposal to the human with map links, ask a **multiple-choice** confirm,
and write the confirmed result back. The human decides; the agent never self-confirms.

## Current state (2026-07-30)
- **confluence_tributary** — DONE (92 curated / 42 not_applicable / 13 deferred / 3 manual, 0 todo).
- **dam_weir_fence** (43) — enriched, `todo`. 14 H reservoir-outlets, 9 M, 20 L (fences/hatchery weirs, coord=map-only).
- **bridge_road_km** (99) — enriched, `todo`. 11 M FSR∩stream crossings, ~88 L public road/rail bridges (coord=map-only).
- **lake_reach** (~68 after reclassifies) — a subagent is/was classifying each as lake_split / whole_lake / point → `lake_proposals.json`; merge into notes the same way.
- Still bare `todo`: `other_reach`, `line_between_signs`, `map_or_vague`, `radius_buffer`, `except_negative`, `area_park_polygon`, `boundary_signs_generic`, a few `falls_canyon_obstacle`.
- **Authored splits**: `stream_sections/splits.json` (12). 18 locators are already linked to them (`split_id` set, curated).

## Tooling
```bash
# progress table + next 10 open items (deferred sort last)
.venv/bin/python -m stream_sections.oneoff.curation_status
.venv/bin/python -m stream_sections.oneoff.curation_status next 12 --kind dam_weir_fence
.venv/bin/python -m stream_sections.oneoff.curation_status review not_applicable --kind confluence_tributary   # audit a bucket
.venv/bin/python -m stream_sections.oneoff.curation_status defer <id> --reason "..."                            # park unsolved -> back of queue
.venv/bin/python -m stream_sections.oneoff.curation_status show <id>
```
Statuses: `todo | curated | manual | not_applicable | deferred`. Done = curated+manual+not_applicable.

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
`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Never commit `name_variants.json`
(a pre-existing unrelated working change).
