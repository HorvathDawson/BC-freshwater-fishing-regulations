# Linking hydro gauges to FWA waterbodies — investigation notes

Working notes on the best way to link `hydro.db`'s `stations` table (ECCC
Wateroffice gauges) to a real FWA `waterbody_key` / stream, so gauge
readings can eventually be attached to the app's own waterbody records the
same way stocking/bathymetry/markers already are. Nothing here is
implemented yet — this is groundwork before writing a `match_fwa.py` for
this directory.

## What we already have

`stations` has 447 rows; 381/447 (85%) already carry `lat`/`lon` from
`enrich_coordinates()`'s Current Conditions KML pull (`hydro_poc.py:417`).
The remaining 66 have no coordinate yet (older/discontinued stations not in
the live KML feed — a separate problem, see "Open questions" below).

Station `name` follows ECCC's own fixed convention: `<WATERBODY NAME>
<QUALIFIER> <PLACE>`. Checked against all 447 live names:

| Qualifier | Count |
|---|---|
| NEAR | 176 |
| AT | 136 |
| ABOVE | 72 |
| BELOW | 55 |
| ABOVE THE | 2 |
| *(no match)* | 6 |

A regex split on the first occurrence of `NEAR`/`AT`/`ABOVE`/`BELOW` as a
whole word cleanly isolates the waterbody name for **441/447 (98.7%)** of
current stations — e.g. `"ADAMS RIVER NEAR SQUILAX"` → `"ADAMS RIVER"`,
`"ATLIN LAKE AT ATLIN"` → `"ATLIN LAKE"`. The 6 that don't split
(`"COQUITLAM LAKE FOREBAY"`, `"COTTONWOOD CREEK HEADWATERS"`, `"NICOLA
RIVER SOUTH OF SHACKAN"`, `"QUINSAM RIVER DIVERSION HEADPOND"`,
`"THEODOSIA RIVER DIVERSION BYPASS"`, `"TWO MILE CREEK IN DISTRICT LOT
4834"`) still have the waterbody name as a clean leading token run — just
need a couple more qualifier keywords (`SOUTH OF`, `IN`) or a manual
override table entry, same as the curated-override tier every other
matcher in this codebase falls back to for stragglers.

So the shape of the problem is: **name (parseable) + real independent
coordinate (85% of rows)** — no external identifier scheme at all (no WDIC
wbid, no FIDQ waterbody_id). That's exactly the `map_markers` population in
`pipeline/recurring/anglerinfo/match_wbid_gazette.py` (own lat/lng, no
identifier to join on), not the `stocking`/`bathymetry` population (which
has an identifier but no coordinate) — so the pattern to copy is that one:
gazetteer name search, disambiguated by distance to the row's own point,
with point-in-polygon/point-on-line fallback when the name search comes up
empty.

## The one real difference from every existing matcher here

Every existing matching chain in this repo (`anglerinfo`'s
`waterbody_matcher.py::LakeIndex`, `build_lake_index()`) only indexes
**lakes/wetlands/manmade** (`POLYGON_LAYERS`) — because stocking/bathymetry/
markers are all lake-type data. Hydro gauges are mostly **on rivers**
(`"ADAMS RIVER NEAR SQUILAX"`, `"ANDERSON CREEK NEAR NELSON"` — anything
whose parsed name ends in RIVER/CREEK, the clear majority of the 447). None
of the existing lake-only matchers can resolve those at all. Whatever we
build has to search **streams**, not just polygons.

## What to reuse instead of rebuilding an index

Per steer: reuse the gazette/atlas machinery already in the codebase rather
than re-deriving a stream index from scratch, and keep all GPKG access
behind `FWADataAccessor` (`data/data_extractor.py`) — no direct
geopandas/pyogrio reads, matching how every other matcher here
(`waterbody_matcher.py`, `base_entry_builder.py`, `FreshWaterAtlas`) already
does it.

Two existing candidates carry both streams and polygons with names:

1. **`pipeline/matching/base_entry_builder.py::_build_metadata()` /
   `_natural_search()`** — the main pipeline's own synopsis→FWA linker.
   Builds a `NameIndex` across streams (grouped by `gnis_id`, from the raw
   graph pickle) *and* lakes/wetlands/manmade (grouped by `waterbody_key`,
   via `FWADataAccessor`), with zone/MU filtering built in. `_natural_search()`
   already does variant fallback + dedup. Correct data, but it's built for
   a different disambiguation signal (zone/MU overlap) than what we have
   here (a real lat/lon), and `_build_metadata()` re-runs a full spatial
   zone-assignment pass over every stream/polygon group on every call —
   expensive to pay for just a name index.

2. **`pipeline/atlas/freshwater_atlas.py::FreshWaterAtlas`** — the
   production atlas, already built and cached as a pickle
   (`FreshWaterAtlas.load(atlas_path)`, path from
   `cfg["output"]["pipeline"]["atlas"]` in project config — see
   `pipeline/enrichment/builder.py:149,175`). Loads fast (no rebuild), and
   exposes exactly the geometry + name we need per feature, already keyed
   by stable id:
   - `atlas.streams: Dict[fid, StreamRecord]` — `display_name` (gnis_name),
     `geometry` (real per-segment LineString), `blk` (blue_line_key),
     `waterbody_key` (if under a lake).
   - `atlas.lakes` / `.wetlands` / `.manmade`: Dict[waterbody_key,
     PolygonRecord]` — `display_name`, real `geometry`.

   This is the better fit: real geometry per feature (so point-to-line /
   point-to-polygon distance, and point-in-polygon fallback, both work
   directly via shapely `.distance()`/`.contains()`, no reprojection setup
   beyond what the atlas already stores in EPSG:3005), no recomputation
   cost, and it's the same atlas the live production build already trusts.

**Recommended approach:** load the cached atlas, build a small
title-cased `display_name → [(layer, id)]` index over
`atlas.streams.values()` + `atlas.lakes/.wetlands/.manmade.values()` — same
shape as `base_entry_builder.py::_add_name()`/`_search_index()`, reused as
a pattern rather than pulled in wholesale (that module's version carries
zone/MU fields we don't have a use for) — then reuse `normalize_name()`
(`pipeline/matching/base_entry_builder.py:137`, already shared by
`anglerinfo`'s own matchers) for the key, so gauge names normalize the
identical way every other matcher's names do.

## Proposed matching stages (mirrors the `anglerinfo` per-stage convention)

1. **Parse** — split station `name` on the qualifier keyword → candidate
   waterbody name. The 6 that don't split get a small manual keyword
   addition or land in a curated override table (same "override always
   wins, keyed by station_id" pattern as `wbid_overrides.json`).
2. **Name search** — `normalize_name()` the parsed name, look up the atlas
   name index (streams + all three polygon layers together — a gauge
   parsed as "ATLIN LAKE" should only ever hit a polygon, one parsed as
   "ADAMS RIVER" only a stream, but there's no reason to pre-split the
   search by expected type).
3. **Disambiguate by distance** — for the 381 rows with a real lat/lon:
   reproject the station point to EPSG:3005 (matching the atlas's own CRS)
   and keep whichever deduplicated name-search candidate's geometry sits
   closest (`Point.distance(geom)`, 0 if inside for polygons) — the exact
   `_closest()` logic `match_fwa_gazette.py`/`match_wbid_gazette.py` already
   use, generalized to accept a LineString candidate the same way it
   already accepts a Polygon.
4. **Point-in-polygon / nearest-stream fallback** — for rows the name
   search misses entirely (gazetteer spells it differently, or the parse
   was wrong): fall back to a direct nearest-feature-to-point lookup across
   the same atlas geometries — `match_wbid_gazette.py`'s own
   `map_markers` handling (`locate_at_point()` for polygons) is the
   precedent; for streams this is just "closest LineString within some
   radius," no separate helper exists yet but it's the same STRtree
   approach `FreshWaterAtlas`/`waterbody_matcher.py` both already build.
5. **Coordinate-less rows (66/447)** — no anchor point to disambiguate
   with at all. Options, not yet decided: (a) leave name-search hits
   unresolved when >1 candidate (flag for manual review, same as
   `match_fwa_override.py`'s `unmatched` status — don't guess blind on a
   common name), (b) try to backfill lat/lon for these from a different
   ECCC source (HYDAT station metadata has coordinates for
   discontinued/historical stations too — not yet fetched by
   `hydro_poc.py`, would need its own fetch step), or (c) accept a
   single-candidate name-search hit unconditionally (safe only when the
   parsed name is unique across the whole index — need to check how often
   that's true before relying on it).

## WSC station-number prefix vs. FWA watershed codes — tested

Built a real (throwaway) prototype of the plan above — name search over
`streams`/`lakes`/`wetlands`/`manmade` loaded via `FWADataAccessor`
(columns only, no raw geopandas/pyogrio calls outside the accessor), keyed
by `normalize_name()`, disambiguated by distance to each station's own
lat/lon (EPSG:3005) when the name search returned more than one candidate
— to get a real population of station→FWA matches to test the WSC idea
against, since there was nothing to compare it to before.

**Match yield (all 447 stations, no max-distance cap, no manual overrides
yet — this is the naive first pass, not the tuned version):**

| method | count |
|---|---|
| `closest` (name search + distance pick) | 361 |
| `single_hit` (name search, only one candidate) | 21 |
| `ambiguous_no_coord` (>1 candidate, no coordinate to pick with) | 37 |
| `no_candidates` (name search found nothing) | 28 |

**382/447 (85.5%) got a chosen match** on the very first pass with no
overrides — consistent with the plan being sound. `no_candidates` (28) is
mostly gazetteer name-spelling mismatches worth checking individually
later; `ambiguous_no_coord` (37) overlaps heavily with the 66
coordinate-less stations from above.

**The actual WSC question — does station-number prefix predict FWA
`WATERSHED_GROUP_CODE` (or the `_50K` variant)?** Grouped the 382 matched
stations by WSC prefix length and checked whether stations sharing a
prefix land in the same FWA watershed group:

| prefix length | meaning | stations sharing a prefix that agree with that prefix's majority code |
|---|---|---|
| 2 (`08`) | major drainage area | ~0% — far too coarse, dozens of FWA groups per major drainage |
| 3 (`08L`) | major + sub-drainage | 27% |
| 4 (`08LD`) | major + sub + sub-sub-drainage | 71% |
| 5 (`08LD0`) | + first digit of sequence | 71% |

(`WATERSHED_GROUP_CODE` and `WATERSHED_GROUP_CODE_50K` gave **identical**
purity numbers throughout — at least for this correlation, the two columns
carry the same mnemonic.)

**Verdict: real correlation, not a clean crosswalk.** At the WSC
sub-sub-drainage level (4 chars), 71% of stations sharing a prefix agree on
FWA watershed group — clearly not coincidence — but 29% don't, and at least
one disagreement checked by hand is a genuine geographic split, not noise:
`08NJ` (Nelson-area) stations split between `KOTL` (Kootenay Lake
tributaries — Anderson/Duhamel/Redfish Creeks) and `SLOC` (Slocan River
tributaries — Lemon Creek, Slocan River itself) — two real, adjacent FWA
watershed groups that WSC's own numbering apparently doesn't distinguish at
the sub-sub-drainage level. **No formula or lookup table will make this
1:1** — WSC's Canada-wide numbering and BC's FWA watershed grouping were
built independently and don't nest the same way.

**Conclusion: use it as a soft plausibility check, not an identifier.**
Exactly the same spirit as `override_correction_is_plausible()`
(`match_fwa_gazette.py`) — a candidate whose FWA watershed group actively
contradicts the *majority* group already seen for that WSC prefix is worth
flagging for review, not silently trusted, and definitely not worth
rejecting outright (given the genuine `08NJ`-style splits, a contradiction
is inconclusive, not proof of a bad match — same reasoning
`_group_code_agrees()` already uses for a *missing* group code). Don't
build a station_id→watershed_group_code table; there's nothing to gain over
just computing majority-agreement per prefix from the matches themselves.

## Round 1b — re-tested the 4-letter group code after fixing the Cedar Creek bug

Before moving on to the hierarchical code, went back and fixed the actual
bug round 1 surfaced (no max-distance cap on "closest wins") and re-ran
*just* the `WATERSHED_GROUP_CODE`/`WATERSHED_GROUP_CODE_50K` correlation —
the 4-letter mnemonic embedded in a WDIC-style identifier's suffix (e.g.
`"01184LFRA"` → `LFRA`), same field bathy/stocking's own
`WATERSHED_GROUP_CODE_50K` tier ultimately traces back to — to see whether
the ~30% disagreement in round 1 was really just noise from bad matches
like that one.

Added a 15km cap: if the nearest name-search candidate is farther than
that from the station's own coordinate, treat it as no plausible match
(`capped_out`) instead of accepting it anyway. `08MH166` (Cedar Creek) now
correctly lands in `capped_out` rather than matching Similkameen. Overall
yield dropped slightly (382 → 379 matched; 3 stations that were previously
mismatched via a too-far pick are now honestly unmatched instead) — but the
group-code correlation itself **barely moved**:

| field | WSC prefix=3 | prefix=4 | prefix=5 |
|---|---|---|---|
| `WATERSHED_GROUP_CODE` (before cap) | 26.5% | 69.5% | 69.7% |
| `WATERSHED_GROUP_CODE` (after cap) | 26.7% | 69.9% | 69.9% |

**So the ~30% disagreement at the WSC-4 level is not matcher noise — it's
real.** Fixing the one confirmed bad match (and whatever else the cap
quietly caught) only bought +0.4 points. `WATERSHED_GROUP_CODE` and
`WATERSHED_GROUP_CODE_50K` remain identical to each other throughout, cap
or no cap. The `08NJ` (KOTL/SLOC) and `08H` (ALBN/COMX-etc.) genuine splits
documented in round 1 are the real explanation, not noise — WSC's own
numbering doesn't subdivide as finely as FWA's group codes do, full stop.
This is a ceiling for the 4-letter mnemonic specifically, not a matching
quality problem: even a perfect matcher would still land at ~70% agreement
at the WSC-4 level, because WSC's own drainage numbering doesn't carry
enough resolution to predict FWA's group code the rest of the way.

## Round 2 — the hierarchical watershed code (not just the group mnemonic), and active-vs-discontinued

Round 1 above only tested `WATERSHED_GROUP_CODE` / `WATERSHED_GROUP_CODE_50K`
(the 4-letter mnemonic, e.g. `SIML`, `KOTL`). Re-ran the same
majority-agreement test against the *other* watershed field FWA carries —
`FWA_WATERSHED_CODE` / `WATERSHED_CODE_50K`, the long hyphen-delimited
hierarchical code (e.g. `300-432687-380566-...`) — using just its **leading
block** (the top-level basin segment) as the coarse analog, restricted to
the 362 matched stations that have a real coordinate:

| field | WSC prefix=3 | prefix=4 | prefix=5 |
|---|---|---|---|
| `WATERSHED_GROUP_CODE` (normal) | 26.5% | 69.5% | 69.7% |
| `WATERSHED_GROUP_CODE_50K` | 26.5% | 69.5% | 69.7% |
| `WATERSHED_CODE_50K` (leading 3 digits) | 55.2% | 88.5% | 88.7% |
| **`FWA_WATERSHED_CODE` (normal, leading block)** | **93.6%** | **95.0%** | **95.0%** |

**The leading block of the *normal* `FWA_WATERSHED_CODE` is a much better
signal than the group mnemonic** — 93.6% agreement at just a 3-character WSC
prefix (major + sub-drainage), where the group code only managed 26.5%.
Printing the actual WSC-3 → leading-block table makes it obvious why: it's
almost a clean lookup (`08J/08K/08L` → `100` = Fraser, `08N` → `300` =
Columbia/Kootenay, `08E` → `400`, `09A` → `800` = Yukon/Atlin, `10*` → `200`,
etc.) — WSC's own major/sub-drainage numbering and FWA's top-level basin
code are clearly built around the same real drainage boundaries, just in
two different numbering schemes, whereas `WATERSHED_GROUP_CODE` subdivides
*within* those same basins far more finely than WSC's numbering does.

The handful of `08H`/`08M`-prefix stations that don't agree with their
prefix's majority leading block are almost all genuine (not errors) —
`08MH153`/`08MH156` ("...AT INTERNATIONAL BOUNDARY") and `08MH155`
("NICOMEKL RIVER") sit in real, distinct small coastal/border basins
(leading block `970`/`900`) despite sharing the Fraser-area `08MH` prefix
with 15 other Fraser-basin stations (`100`); `08H`'s `920`/`930` split is
Vancouver Island's own two coastal drainage groupings. **The one exception
that is a known error**: `08MH166` ("CEDAR CREEK ABOVE THE MOUTH" — the bad
match from round 1) shows leading block `300`, disagreeing with its
prefix's `100` majority — this signal independently flags the exact same
bad match round 1 found by hand, which is good corroboration that it works
as a trip-wire.

**Updated recommendation:** use `FWA_WATERSHED_CODE`'s leading block (not
`WATERSHED_GROUP_CODE`) as the plausibility/corroboration signal — a
candidate whose leading block disagrees with the majority already seen for
its WSC-3 (or WSC-4) prefix is a much stronger red flag than a group-code
disagreement, precisely because genuine disagreements are rare and mostly
explainable (border/coastal edge cases), unlike the group-code version
where disagreement is closer to a coin flip.

### Active vs. discontinued

Restricting to `operation_schedule='Continuous' AND data_available='Yes'`
(the closest proxy available in `hydro.db` today) drops only 14 of the 362
coordinate-having matched stations and barely moves the numbers (e.g.
group-code prefix=4 purity: 69.5% → 69.3%) — not a meaningful confound
either way.

That said, **neither field is actually "Active/Discontinued"** —
`operation_schedule` is Continuous-vs-Seasonal (does it run all year or
just part of it) and `data_available` just means "reported data in the
last 2 hours" (could be a temporary outage, not decommissioning). More
importantly, `stations` was populated entirely from ECCC's **Real-time
Station Search** (`hydro_poc.py::fetch_stations()`, filtered to province
BC) — per that endpoint's own purpose, it should only ever list currently
operating real-time stations, so this table likely contains **no
genuinely-discontinued stations at all** already. None of the 66
coordinate-less stations show `data_available='No'` either, so the missing
coordinates aren't an active/discontinued issue — just a Current
Conditions KML snapshot completeness gap (a seasonal station currently in
its off-period, e.g., wouldn't report a live position that day).

The CSV you opened (`~/Downloads/metadata_20260723T0619.csv`) is ECCC's
*other* station metadata schema — the one that actually carries a real
`Status` (Active/Discontinued) column, plus its own authoritative
`Latitude`/`Longitude` per station (would also fix the 66-missing-coord gap
in one shot, no KML scrape needed). It only has 2 sample rows right now
(both named "Cedar Creek" — looks like a manual search/export, not the full
BC list) — not enough to run the active-filter or backfill coordinates at
scale. To actually do this properly we'd need the same export for the full
BC station list (whatever UI action produced that file, re-run with
province=BC and no name filter) — worth doing since it would double as the
authoritative-coordinate fix for the 66 rows Round 1 flagged as
coordinate-less.

## A real bug this testing surfaced (fix before productionizing)

Spot-checking the `08MH`/`SIML` disagreement (a `08MH` — Lower Mainland —
station apparently landing in `SIML`, the Similkameen group, ~250km away)
found an actual matcher bug, not a WSC quirk: **`08MH166` ("CEDAR CREEK
ABOVE THE MOUTH", coords 49.381, -122.777 — Maple Ridge area) matched to a
Similkameen-region "Cedar Creek"** because the name search returned 117
province-wide candidates for the generic name "Cedar Creek" and the
prototype's "closest wins" logic has **no maximum-distance cap** — so with
a common name and no genuinely-nearby candidate available in the search
radius it still confidently returns whatever's least-far, even at 250km.

This confirms the plan needs the same safeguard `match_fwa_gazette.py`
already applies to its own override tier
(`_MAX_OVERRIDE_ANCHOR_KM`/`override_correction_is_plausible()`): cap
"closest wins" at some sane radius (a few km, generously accounting for a
gauge sitting well upstream/downstream of the named reach) and demote
anything beyond it to `ambiguous`/needs-review rather than accepting it —
the WSC-majority check above is a good second-line trip-wire for exactly
this failure mode (generic name, high candidate count, no independent
signal caught it).
- Whether a single-candidate name hit is safe to trust unconditionally for
  the 66 coordinate-less rows depends on how many parsed names are
  ambiguous province-wide even after qualifier-splitting — not measured
  yet.
- Under-lake streams (`atlas.under_lake_streams`) — worth including in the
  stream search too, since some gauges named e.g. "X RIVER AT OUTLET OF Y
  LAKE" may sit exactly on that boundary.

## Not yet built

No matcher code exists yet — this file is the plan. Next step, if we go
ahead: a `live-data/hydro/match_fwa.py` that loads the cached atlas, builds
the name index described above, and writes a `station_fwa_match` table
into `hydro.db` (columns: `station_id`, `layer` [`streams`/`lakes`/
`wetlands`/`manmade`], `fwa_id` [fid or waterbody_key], `blue_line_key`,
`method`, `distance_m`, `confidence`) — same shape as `anglerinfo`'s
`match_final`.
