# waterbody_db

One shared SQLite file (`waterbody_db.db`) holding stocking, marker,
bathymetry, and WDIC 1:50K reference data together, so `match.py` can match
everything against WDIC with a single local connection and no cross-directory
DB paths. This package started as a testbed in `live-data/waterbody_db/`
(recurring, non-static data feeds not yet in production) alongside three
standalone POCs — `stocking/`, `bathymetry/`, and `common/` — and absorbed
them once its own matching chain reached 100% resolution across both source
populations (see "Results" below): `fetch_stocking.py`/`fetch_bathymetry.py`
began as replicated copies of those POCs' own fetch scripts, and
`waterbody_matcher.py` began as a trimmed copy of the pieces of
`common/waterbody_matcher.py` this package's own matching scripts use
(`LakeIndex`/`build_lake_index()`, `locate_at_point()`, `_confirmed_by_name()`,
`build_override_index()`) — see that file's own docstring. The three
originals have since been removed. This package has since graduated out of
`live-data/` into `pipeline/recurring/waterbody_db/` — its own matching chain
replaced `pipeline/matching/bathymetry_matcher.py` entirely (see
`pipeline/enrichment/waterbody_accessor.py`), and feeds waterbody enrichment
(names, depth-map PDFs, marker amenities) into the main regulations build,
plus a recurring stocking-info pipeline (`pipeline/recurring/stocking_resolver.py`).
Everything still depends on the main pipeline (`pipeline/`, `data/`,
`project_config`) the same way every other `pipeline/` package does.

## Why WDIC, not FWA

See `match.py`'s own module docstring for the full rationale (FWA's
generalized polygons can drift from what a 1:50K identifier actually means —
confirmed live, PEAR LAKE's FWA `WATERSHED_GROUP_CODE` changed after FIDQ's
own snapshot was taken). `match.py` itself matches stocking/bathymetry/marker
identifiers directly against WDIC — the identifier scheme's own source of
truth — with no FWA join, name fallback, or override at that stage;
`match_fwa_gazette.py`/`match_fwa_identifier.py`/`match_fwa_override.py`/
`match_wbid_gazette.py` layer FWA matching on top afterward (see below).

## Files

- **`fetch_stocking.py`** — replicated copy of `../stocking/fetch_stocking.py`,
  DB_PATH repointed at `waterbody_db.db`. Writes `fidq_waterbodies`,
  `map_markers`, `fidq_stocking_records`, `stocking_records`.
- **`fetch_bathymetry.py`** — replicated copy of
  `../bathymetry/fetch_bathymetry.py`, DB_PATH repointed at `waterbody_db.db`.
  Writes `bathy_surveys`; `--csv-only` skips the (slower, and not needed by
  `match.py`) survey-polygon WFS fetch into a local GPKG.
- **`fetch_wdic.py`** — fetches the *entire* `WHSE_FISH.WDIC_WATERBODY_POLY_SVW`
  layer (BC Data Catalogue `414be2d6-f4d9-4f32-b960-caa074c6d36b`), paginated
  over all ~322k records that carry a `WATERBODY_IDENTIFIER` (out of 347,630
  total — the rest are unidentified river/generic polygons). Self-contained:
  no dependency on the other two fetchers' tables. Writes `wdic_cache`
  (identifier, poly_id, wbody_id, gazetted_name, approx lat/lng). See its own
  docstring for why this has to be WFS `GetFeature`, not WMS (a map-image
  service with no bulk vector/attribute mechanism) — the catalogue page
  itself only lists WMS + a KML loader, but the same server's WFS endpoint is
  the actual data service underneath, confirmed live.
- **`fetch_all.py`** — runs all three, in order, into `waterbody_db.db`.
- **`match.py`** — reads `fidq_stocking_records`, `bathy_surveys`,
  `map_markers`, and `wdic_cache` from `waterbody_db.db`, matches every
  stocking/bathymetry/marker identifier against the WDIC cache, writes
  `match_wbid`. Three sources, one output row shape:
    - `stocking` — from `fidq_stocking_records`, keyed by `waterbody_id`,
      identifier = FIDQ's `waterbody_identifier`.
    - `bathymetry` — from `bathy_surveys`, keyed by `identifier` (its own
      WSA `WATERBODY_IDENTIFIER_WSA_50K`).
    - `map_markers` — from `map_markers`, keyed by `marker_id`, identifier =
      the marker's own `wbid` (gofishbc's map data, matched independently of
      any stocking history — a marker can exist with no FIDQ record at all).
  `match_wbid.name_variants` carries every other known name for a row
  alongside the primary `name`/`wdic_gazetted_name` pair — FIDQ's own
  `aliases` for stocking (itself comma-separated, e.g. "STIRLING ARM, TAYLOR
  ARM, TWO RIVERS ARM" — split into individual names, not stored as one
  blob), `MAP_TITLE` for bathymetry, empty for markers (`title` already
  fills that role as `name`). Stored `"; "`-joined (a plain TEXT column —
  sqlite has no array type). Display/review here, and match_fwa_gazette.py's
  own search input below — not matched on by match.py itself.
- **`match_fwa_gazette.py`** — second pass, reading **only** `match_wbid`
  (never the raw per-source tables): for every row match.py itself resolved
  against WDIC, an independent *current* FWA `waterbody_key`, two tiers:
    - **`override`** — checked first, "override always wins" (same
      philosophy as the main regulations pipeline's own
      `pipeline/matching/match_table.py` `MatchTable`), against
      `common/waterbody_matcher.py::build_override_index()` — the *same*
      `pipeline/matching/overrides.json` table the production pipeline
      matches against, reused unchanged rather than re-derived. That helper
      already indexes by normalized name alone (no region/MU key, since
      FIDQ/WSA/gofishbc records carry neither) — it already runs "all
      regions at once" by construction; nothing needed to add there. Gated
      by distance to the row's WDIC anchor (`_MAX_OVERRIDE_ANCHOR_KM`,
      50km) rather than trusted unconditionally — mirrors
      `override_correction_is_plausible()`'s own reasoning (a common-name
      collision like "Rose Lake" once silently "corrected" a bathymetry
      survey 587km away).
    - **`gazetteer_search`** — for whatever the override tier didn't
      resolve: gazetteer name search (`name` + every `name_variants` entry)
      against `lakes`/`wetlands`/`manmade` (`data/bc_fisheries_data.gpkg`),
      disambiguated by picking whichever deduplicated candidate's FWA
      polygon sits physically closest to the row's own WDIC-derived
      `approx_lat`/`approx_lng` — not by watershed-group code, the way
      `common/waterbody_matcher.py`'s own T3 tier does it, since a real
      independent coordinate is available here instead. Includes a
      `name_variant_index` fallback (gofishbc marker titles routinely drop
      the generic type word — "Abbott" vs FWA's "Abbott Lake" — which a
      plain gazetteer search alone can't bridge; without that fallback
      markers matched ~1.2% instead of ~83%) and its own
      `_expand_lake_abbreviations()` fallback — "L."/"LK"/"LKS."/"CR." ->
      "LAKE"/"LAKES"/"CREEK", **deliberately narrower** than
      `pipeline/matching/bathymetry_matcher.py::normalize_map_title()`
      (which this module used to call directly): that function also strips
      "#N"/"NO. N" numbering, which risks a *false* match between two
      genuinely different numbered lakes (confirmed live: "COLLIERY #3" and
      plain "COLLIERY" are two distinct, separately-overridden identifiers
      in `wbid_overrides.json`, not the same lake) — this module's own
      version only ever expands the abbreviation, never touches numbering.
      Confirmed live: WSA bathymetry MAP_TITLE "TATLATUI L." only resolves
      after expansion to "TATLATUI LAKE", landing on the exact same
      waterbody_key `bathymetry/bathy_overrides.json` already curated for
      this identifier independently).
  Writes `match_fwa_gazette` (includes `tier`, plus `approx_lat`/`approx_lng`
  carried through from `match_wbid` for match_fwa_identifier.py's own use
  below). See the module's own docstring for the full rationale.
- **`match_fwa_identifier.py`** — third pass, reading **only**
  `match_fwa_gazette` (`status = 'unmatched'`): for whatever the override
  and gazetteer tiers couldn't resolve, tries the row's own 50K identifier
  again — this time joined directly against FWA's *own*
  `WATERBODY_KEY_GROUP_CODE_50K` column (`index.group_code_map`) instead of
  WDIC's frozen one. This is `common/waterbody_matcher.py`'s own
  `T1_identifier` tier — normally the *first*, most reliable tier in that
  module's full cascade — deliberately run *last* here instead: match.py
  already spent the identifier once against WDIC, and match_fwa_gazette.py
  corroborated independently by name; only what neither of those settled
  reaches this final identifier-only check. A group code shared by >1 FWA
  `WATERBODY_KEY` is resolved by the same closest-to-WDIC-anchor distance
  pick match_fwa_gazette.py uses (imported and reused, not reimplemented).
  Writes `match_fwa_identifier`.
- **`match_fwa_override.py`** — fourth and final pass, reading **only**
  `match_fwa_identifier` (`status = 'unmatched'`): applies this
  directory's own hand-curated `wbid_overrides.json` — a *different* file
  from the one `match_fwa_gazette.py`'s own override tier already checks
  (`pipeline/matching/overrides.json`, the production regulations table),
  kept separate the same way `bathymetry/bathy_overrides.json` is kept apart
  from production, since these entries fix problems specific to this
  testbed's own remaining stragglers, not regulation-matching corrections.
  Keyed by **identifier**, not name (several rows share one identifier
  across sources with different names — e.g. bathymetry's "MCIVOR LAKE" and
  stocking's "MCIVOR", both `00095CAMB` — one entry resolves both
  correctly). `waterbody_keys` is a list, not a single value — usually one
  key, but >1 for a genuine multi-part waterbody (e.g. "SPECTACLE - SWAN
  LAKES" covers two distinct FWA lakes, Spectacle Lakes + Swan Lake;
  aggregated the same comma-joined way `common/waterbody_matcher.py`'s own
  `build_resolved()` handles a multi-part tie, not silently truncated to the
  first key). Every entry was confirmed against a real geographic signal,
  never a name guess alone — a manually-looked-up GNIS name, a
  point-in-polygon lookup at the row's own WDIC anchor for an entirely
  unnamed FWA polygon (`common/waterbody_matcher.py::locate_at_point()`,
  reused not reimplemented), or the WDIC anchor landing exactly inside one
  specific candidate out of several same-named ones. Entries with
  `"skip": true` record a row confirmed to have no current FWA polygon at
  all. Writes `match_fwa_override` with a three-way `status`, kept
  distinct rather than lumping unresolved rows together:
    - `matched` — resolved to a real `waterbody_key`.
    - `ignored` — a curated entry exists, but it's a confirmed
      `"skip": true` (no current FWA polygon at all — not a matching
      failure to keep chasing).
    - `unmatched` — no curated entry exists yet at all; still genuinely
      open for review.
- **`match_wbid_gazette.py`** — a fifth stage, but for the *opposite*
  population from the other three: `match_wbid` rows `match.py` never
  matched to WDIC at all (`status = 'unmatched'`), so there's no WDIC anchor
  to disambiguate by — these rows never even entered the
  gazette→identifier→override chain above, which only processes what WDIC
  *did* match. Two different strategies, by source:
    - `map_markers` — has its **own** lat/lng (`map_markers.lat`/`.lng`,
      independent of WDIC), the only exception to this chain's "read only
      the prior stage" rule since there's no prior-stage coordinate to read
      at all for these rows. A name search's ties are disambiguated by
      `_closest()` (imported from match_fwa_gazette.py) against the
      marker's own point; when a name search finds *nothing at all* (a real
      but entirely unnamed FWA polygon — confirmed live for all 4 markers in
      this population), falls back to a direct point-in-polygon lookup
      (`common/waterbody_matcher.py::locate_at_point()`) at the marker's own
      coordinate instead — `stocking/match_waterbodies.py`'s own documented
      T1_geo/T2_geo precedent, reused not reimplemented.
    - `stocking`/`bathymetry` — no coordinate of their own at all, unlike
      `map_markers` above (which has a real point `_closest()` can validate
      a name hit against) or every other stage in this chain (which has the
      WDIC anchor). Two independent signals instead, **cross-validated
      against each other** rather than either one ever trusted alone — a
      gazetteer name hit is checked against the row's own 50K identifier
      suffix (e.g. `50002SQAM`'s suffix is `SQAM`) via `_group_code_agrees()`
      (drops a candidate whose own `WATERSHED_GROUP_CODE_50K` actively
      disagrees; a candidate with *no* group code recorded at all passes as
      inconclusive, not a contradiction — confirmed live, "DEADMANS LAKE" has
      none at all, a real FWA lake entirely outside the 1:50K scheme, so
      requiring an exact match would wrongly reject it), and a unique
      `index.group_code_map` hit is checked against the row's own name via
      `wm._confirmed_by_name()` (the exact function
      `common/waterbody_matcher.py::match_row_t1()` itself calls — reused,
      not reimplemented). Confirmed live: stocking's "GREEN TIMBERS"
      (`01184LFRA`) has no gazetted name at all (inconclusive, passes) and
      resolves via the identifier-join side alone, landing on the *exact
      same* polygon a marker sharing the identical identifier independently
      located by point-in-polygon — real cross-confirmation between two
      unrelated methods, not a guess.
  Writes `match_wbid_gazette` (`method`: `identifier+name` (both signals
  independently agreed) | `identifier_join` | `marker_anchor` |
  `marker_point_in_polygon` | `single_hit` | `watershed_group_code`).
- **`match_final.py`** — the aggregated result of every table above: one row
  per (source, source_id), the single answer the whole pipeline landed on,
  and which stage produced it. The one deliberate exception to "read only
  the prior stage" — its entire job is aggregation, so it reads
  `match_wbid` plus all four downstream tables and, per row, walks whichever
  of the two disjoint chains applies (`match_wbid.status = 'matched'` rows go
  through gazette → identifier → override; `'unmatched'` rows go through
  `match_wbid_gazette` instead — never both), taking the first stage with a
  real answer. `resolved_by` records which table won; `method` carries that
  stage's own tier/method value through verbatim. A `match_fwa_override` row
  with `status = 'ignored'` becomes `status = 'no_polygon'` here — kept
  distinct from `'unmatched'` (nothing has resolved it at all). Coordinate:
  `match_wbid`'s own WDIC anchor when present, else `match_wbid_gazette`'s
  own (a marker's coordinate) when that's what resolved it instead — same
  "whichever stage actually has one" approach as everywhere else in this
  chain. Writes `match_final`.

`source` (`stocking` | `bathymetry` | `map_markers` — literally the
originating table name for the latter two, `fidq_stocking_records`/
`bathy_surveys`/`map_markers`), `approx_lat`/`approx_lng` (WDIC's own anchor
point, or the marker's own coordinate in `match_wbid_gazette`), and
`approx_map_url` (an OpenStreetMap link centered on that point,
`match.py::osm_url()` — imported by every downstream stage rather than
reimplemented) are carried through **every** table in the chain —
`match_wbid` → `match_fwa_gazette` → `match_fwa_identifier` →
`match_fwa_override`, plus the parallel `match_wbid_gazette` — for any row
that has one, not just recomputed or dropped at each stage. Confirmed live:
5,222/5,222 `match_fwa_gazette` rows carry a non-null coordinate + link.

## CLI

Run as a package from the project root (not `cd`-and-`python file.py` — these
are proper package-relative imports now, not script-relative ones):

```bash
python -m pipeline.recurring.waterbody_db.fetch_all                 # stocking, bathymetry, wdic — all into waterbody_db.db
python -m pipeline.recurring.waterbody_db.fetch_all --skip stocking  # skip one or more (repeatable)

# or individually:
python -m pipeline.recurring.waterbody_db.fetch_stocking update
python -m pipeline.recurring.waterbody_db.fetch_bathymetry --csv-only
python -m pipeline.recurring.waterbody_db.fetch_wdic

python -m pipeline.recurring.waterbody_db.match                      # match all, write match_wbid to waterbody_db.db
python -m pipeline.recurring.waterbody_db.match --dry-run            # match all, print only, no DB writes

python -m pipeline.recurring.waterbody_db.match_fwa_gazette          # run after match; write match_fwa_gazette
python -m pipeline.recurring.waterbody_db.match_fwa_gazette --dry-run
python -m pipeline.recurring.waterbody_db.match_fwa_gazette --gpkg /path/to/bc_fisheries_data.gpkg

python -m pipeline.recurring.waterbody_db.match_fwa_identifier       # run after match_fwa_gazette; write match_fwa_identifier
python -m pipeline.recurring.waterbody_db.match_fwa_identifier --dry-run

python -m pipeline.recurring.waterbody_db.match_fwa_override         # run after match_fwa_identifier; write match_fwa_override
python -m pipeline.recurring.waterbody_db.match_fwa_override --dry-run

python -m pipeline.recurring.waterbody_db.match_wbid_gazette         # run after match; write match_wbid_gazette
python -m pipeline.recurring.waterbody_db.match_wbid_gazette --dry-run

python -m pipeline.recurring.waterbody_db.match_final                # run after everything else; write match_final
python -m pipeline.recurring.waterbody_db.match_final --dry-run

python -m pipeline.recurring.waterbody_db.export                     # run after match_final; writes
                                                                       # data/bathymetry_pdfs/waterbody_matches.json
                                                                       # (see pipeline/enrichment/waterbody_accessor.py)
```

## Results (confirmed live)

`fetch_wdic.py`: 311,652 unique `WATERBODY_IDENTIFIER`s cached (33 WFS pages,
~6 minutes). `match.py` against the full replicated fetch: 2,316/2,318
stocking + 2,147/2,147 bathymetry + 759/763 markers = **5,222/5,228 (99.9%)**
matched.

`match_fwa_gazette.py` against every one of those 5,222 matched rows:
**4,658/5,222 (89.2%)** independently corroborated against *current* FWA by
name — 29 via the curated-override tier, 4,629 via gazetteer search
(including the `name_variant_index` and `_expand_lake_abbreviations()`
fallbacks — see above). 1,969 rows had more than one candidate before the
distance-to-WDIC-anchor pick settled it.

`match_fwa_identifier.py` against the 564 rows that left unmatched:
**550/564 (97.5%)** resolved by the direct FWA group-code identifier join
alone — confirming this project's own documented pattern that
identifier-join is the single most reliable tier, even run last here.

`match_fwa_override.py` against the final 14 rows: **9 matched** (curated,
each confirmed against a real geographic signal — see above, including one
multi-part waterbody resolved to 2 keys, and "CLARK #1 (WEST) LAKE" —
surfaced by the "#N"-stripping removal above, resolved by point-in-polygon
at WDIC's own record for that identifier, whose gazetted name was itself
blank) and **5 confirmed to have no current FWA polygon at all** (recorded,
not left unexplained) — **0 rows left unreviewed**. **5,217/5,222 (99.9%)
resolved with a real `waterbody_key`** across the main four-stage chain;
counting the 5 confirmed-no-polygon rows as explained rather than
unresolved, **5,222/5,222 (100%) accounted for**.

`match_wbid_gazette.py` against the 6 `match_wbid` rows that never matched
WDIC at all: **6/6 (100%) matched** — 2 stocking (1 via a plain gazetteer
hit, "DEADMANS LAKE"; 1 via the identifier join it had never gotten to try,
"GREEN TIMBERS") and 4 map_markers (all 4 via point-in-polygon at the
marker's own coordinate — none had a gazetteer name to search by at all).

`match_final.py` aggregates all of the above into one row per (source,
source_id): **5,223/5,228 (99.9%) matched** (4,658 via match_fwa_gazette +
550 via match_fwa_identifier + 9 via match_fwa_override + 6 via
match_wbid_gazette) and **5 `no_polygon`** — **0 rows left `unmatched`**,
**5,228/5,228 (100%) accounted for** across the entire pipeline, from every
source combined.

## History

`fetch_stocking.py`, `fetch_bathymetry.py`, and `waterbody_matcher.py` here
started as **copies** of `stocking/fetch_stocking.py`,
`bathymetry/fetch_bathymetry.py`, and `common/waterbody_matcher.py` — not
imports, since this package was deliberately kept self-contained within
`live-data/` while it proved itself out alongside those three originals.
Those originals have since been removed (this package's own matching chain
fully superseded them — see "Results" above), so these are now simply this
package's own copies of that logic, not a fork to keep in sync with anything.

This package itself has since graduated from `live-data/waterbody_db/` (a
POC directory) into `pipeline/recurring/waterbody_db/` (production): its
matching chain fully replaced `pipeline/matching/bathymetry_matcher.py`
(deleted — bathymetry resolution is now precomputed once via `export.py`
rather than re-derived on every build, see
`pipeline/enrichment/waterbody_accessor.py`), and its stocking match results
feed a recurring pipeline (`pipeline/recurring/stocking_resolver.py`,
modeled on `pipeline/recurring/in_season_scraper.py`/`in_season_resolver.py`)
that isn't yet cron-scheduled. Imports were converted from bare/script-style
(`from fetch_wdic import DB_PATH`, relying on the script's own directory
being on `sys.path[0]`) to proper package-relative imports
(`from .fetch_wdic import DB_PATH`) — run every module here via
`python -m pipeline.recurring.waterbody_db.<module>` from the project root,
not `cd`-and-`python <file>.py`.
