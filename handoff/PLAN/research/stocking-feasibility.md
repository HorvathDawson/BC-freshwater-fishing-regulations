# Stocked lakes: feasibility, matching and data shapes

Research only, 2026-11-13. No product code, ticket or ledger changed. Builds on T200, T608 D
(`handoff/PLAN/app-integration/T608D-findings.md`, `T608D-followup.md`) and T634 (third pass, stocking proposals).
Facts are marked **[measured]** (counted here today), **[read]** (read in code or on a page), or **[not verified]**.
Recommendations are marked **Opinion**.

## Summary
- **What production does today (`main`)**: it scrapes the Province's FIDQ stocking pages (lakes only). It then
  turns each record's lake code (WBID, e.g. `00755LFRA`) into a rough map point using the old 1:50K lake layer
  (WDIC). It searches the current lakes **by name** and takes the same-named lake **nearest to that point, with no
  distance limit**. The code is only checked directly as a later fallback.
  Matches are keyed to one build's reach ids. A weekly GitHub cron republishes the records.
- **New finding**: the Province's open (OGL) layer "Known BC Fish Observations" holds **74,879 stocking-release
  points** (`SOURCE = "Releases Database: release_id N"`). Each has a coordinate, the WBID, the FWA watershed code and
  the same waterbody id FIDQ uses. **2,317 of the 2,318 stocked FIDQ lakes are in it.** It gives every stocked
  lake an independent, open coordinate. It has no counts, strain or weight.
- **Measured matcher**: lake code first, then the open point, then the name as a check. That gives **2,164 of 2,318
  (93.4 %) auto-match candidates**: 1,674 where code, name and point all agree, and 490 unnamed lakes where code and
  point agree. **154 (6.6 %) go to review.** For lakes stocked since 2020: 734 of 773 (95 %); 39 to review.
- **Freshness**: both Provincial sources stop at **2026-06-29** today. Autumn 2026 stocking is not in them yet.
  GoFishBC is current but "All Rights Reserved", and it put a Cloudflare challenge in front of a scripted request today.
- **Licence is still the blocker**: the stocking reports and FIDQ are "Access Only" in the catalogue. The owner's
  word "OGL" (T634) has no link yet. The FISS points and every FWA layer are OGL.
- **Opinion, first step**: add the open FISS point to the existing `pipeline.stocking.propose` as evidence. Freeze
  the reviewed matches (one curated file keyed by FIDQ waterbody id), then publish one release-independent feed keyed by
  item id.

## 1. Production today (`main`)

### The chain [read]
| Step | File on `main` | What it uses |
|---|---|---|
| Fetch | `pipeline/recurring/anglerinfo/fetch_stocking.py` | Scrapes FIDQ HTML (`searchFishStocking.do` by watershed-code prefix, `waterbodyType=L`; `infoFishStocking.do` per waterbody, embedded `var dataStocking` JSON). Lakes only. Also GoFishBC map markers (`/wp-json/wpgmza/v1/markers`) and its report as a cross-check |
| 1. Code to WDIC | `match.py` | WBID joined to `WHSE_FISH.WDIC_WATERBODY_POLY_SVW` (1:50K). Gives an approximate lat/lng (the 50K polygon's representative point). 2,316 of 2,318 stocking rows hit |
| 2. Name search | `match_fwa_gazette.py` | First `pipeline/matching/overrides.json` by name, capped at 50 km from the anchor. Then a **gazetteer name search** (name + FIDQ aliases + "L." expansions + a `name_variant_index` fallback) over FWA lakes/wetlands/manmade. When several match, `_closest()` picks the nearest to the WDIC point **with no distance cap** (lines 316-345). 1,969 rows had more than one candidate |
| 3. Code to FWA | `match_fwa_identifier.py` | Only for rows still unmatched: WBID = FWA `WATERBODY_KEY_GROUP_CODE_50K`; several keys are settled by nearest |
| 4. Hand overrides | `match_fwa_override.py`, `wbid_overrides.json` (12 rows) | Keyed by WBID; list of keys for multi-part lakes; `skip` for no polygon |
| 5. Never hit WDIC | `match_wbid_gazette.py` | Name search checked against the WBID's 4-letter group suffix |
| Aggregate | `match_final.py` | One answer per source row |
| To the map | `pipeline/recurring/stocking/resolver.py` | `stocking_matches.json` = `{reach: {reach_id: [fidq_id]}, waterbody: {wbk: [fidq_id]}}` via the build's `poly_reaches.json`; `records/<fidq_id>.json` per lake |
| Refresh | `.github/workflows/update-stocking.yml`, `scripts/update-stocking.sh` | Weekly GitHub cron (Mon 09:00 UTC), pushes records to R2 `cron/stocking/` |
| Shown | `webapp/src/components/InfoPanel.tsx` 1426-1457, `services/waterbodyDataService.ts` 886-940 | A "Stocking" tab: per release, date, count, species · life stage · average grams, newest first |

Match rate claimed by main's README: 5,223 of 5,228 rows across stocking, charts and markers (99.9 %), 5 with no
polygon. **[read, not re-run]** The README counts "accounted for", not "right": there is no truth set behind it.

### Where it goes wrong [read, Opinion on severity]
| Case | What happens on `main` |
|---|---|
| **Same-named lakes** | The name decides and the code only gives a point. "Nearest Trout Lake to the 50K point" is usually right, but nothing caps the distance, so a missing local polygon silently takes a same-named lake anywhere. The code, the strongest signal, is spent before the name and is only rechecked if the name finds nothing |
| **Unnamed lakes** | The name search finds nothing; they fall to step 3 (code alone, several keys picked by nearest). 550 rows matched this way with no check |
| **Name variants** | Substring and "variant index" fallbacks (Abbott → Abbott Lake) widen the name test. The T634 audit called these unsound and the cause of `lake-must-not-answer-to-its-river`-type errors |
| **Streams** | Not fetched (`waterbodyType=L`). Stocked streams never show |
| **Lakes split into parts** | Mapped to every reach of the polygon via `poly_reaches.json`. This works, but only for the build it was made on: reach ids are build output, so the match file must be rebuilt with every build |
| **Failure** | `_loadStockingMatches` and `_stockingRecord` turn any fetch error into an empty list with a `console.warn`. The lake simply shows no Stocking tab, which is a silent fallback (AGENTS W46) |
| **Refresh** | A GitHub cron, now ruled out (`no-github-schedules`) |
| **Licence** | FIDQ / `FISH_RELEASES_PUBLIC_VW` is "Access Only". Main's attribution cites the Province's copyright page, which forbids reproduction of access-only material without permission |

## 2. Sources

| Source | Ids carried | Coordinates | Release fields | Cadence / depth | Access | Licence |
|---|---|---|---|---|---|---|
| **FIDQ** stocking pages (front end of `WHSE_FISH.FISH_RELEASES_PUBLIC_VW`) | FIDQ `wbody_id`, WBID (`waterbody_identifier`), 50K watershed code, gazetted name, aliases, `stock_id`, region code/name | **none** | date, species, strain, brood year, source hatchery, origin, life stage, mark, genotype, count, average weight [measured: a fetched row] | catalogue: "daily"; 1900 to present. Newest row in our fetch: **2026-06-29** [measured] | HTML scrape with a session cookie; there is no file or API. The bulk option e-mails a CSV | **Access Only** [read, catalogue `provincial-fish-stocking-reports`, `fish-information-data-query`] |
| **Known BC Fish Observations** (`WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP`) | `WBODY_ID` (= FIDQ `wbody_id`), WBID, `NEW_WATERSHED_CODE` (FWA code), gazetted name, `release_id` in `SOURCE` | **yes**, UTM point; one point per waterbody (0 of 2,317 lakes have two) [measured] | date, species, life stage, agency (FFSBC). **No count, strain or weight** | Public WFS, no login. Releases: 74,879 rows (lake 66,487, stream 8,333, wetland 59); about 1,060 a year; newest **2026-06-29**, oldest 1894 [measured] | `openmaps.gov.bc.ca/geo/pub/wfs` | **Open Government Licence – BC** [read, catalogue `known-bc-fish-observations-and-bc-fish-distributions`] |
| **GoFishBC** stocking report (FFSBC), `/wp-json/ffsbc-stocking/v1/releases` | WBID (`wbid`), lake name, nearest town | none in the report (map markers have a point) | date, species, strain, stage, size (g), count (from T608 D's one fetch) | Most current: rows within days. Their page says it updates every couple of days in season [not verified] | 2026-11-10: one 200 then one 403. **2026-11-13: a Cloudflare "Attention Required" challenge** to one plain request [measured]. Not worked around | "Copyright © 2003 - 2026 … All Rights Reserved"; "You require written consent to copy or display any portion of this website" (T608 D) |
| **FWA lakes / manmade** (`FWA_LAKES_POLY`, `FWA_MANMADE_WATERBODIES_POLY`) | `WATERBODY_KEY` (our `wbk:` item id), **`WATERBODY_KEY_GROUP_CODE_50K` = the WBID**, GNIS ids/names, watershed codes | polygons | — | Already in `bc_fisheries_data.gpkg` | local | OGL [read] |
| **WSA 50K water polygons (WDIC)** | WBID, 50K poly id | polygons | — | static | WFS | OGL [read, `wsa-water-polygon-features-50-000`] |
| **FWA 20K–50K waterbody cross-reference** (`FWA_WATERBODIES_20K_50K`) | 20K waterbody key ↔ 50K ids, primary + secondary matches | — | — | static | **Custom download only** (not on the public WFS) [measured] | OGL [read] |
| HabitatWizard | — | — | — | — | A map viewer over the same warehouse layers | not checked separately [not verified] |

**The province publishes the WBID ↔ waterbody-key crosswalk in two forms.** It is a column on the FWA lakes layer
itself (`WATERBODY_KEY_GROUP_CODE_50K`), which is what T634's `propose` already uses. It is also the 20K–50K
cross-reference table (OGL, not fetched). A WBID can sit on several FWA polygons: a lake in pieces, or small ponds
under a lake's code.

**Under the self-host rule** (Opinion): FISS and FWA can be copied and served today with the OGL credit line. FIDQ
counts and strains, and anything from GoFishBC, need the licence answer first. GoFishBC also needs FFSBC's written
consent.

## 3. Matching design for this branch

### What already exists [read]
- `pipeline/stocking/propose.py` (T634): code picks, name confirms, writes `wbk:` ids, never a name alone, no
  distance fallback, every row a proposal. Its output `data/generated/stocking/proposals.json`: clean 1,751,
  unnamed 521, no_lake 32, two_named 9, name_disagrees 4, variant_circular 1.
- `python -m pipeline.bathymetry.accept --set stocking` is the one writer of `data/curated/stocking/matches.json`
  (not created). Bulk accept is limited to `clean`.
- Gauges set the pattern: `data/curated/gauges/matches.json` rows carry the coordinate, the keys, the evidence and a
  `trust`. Build and bundle read the file and neither matches.
- `pipeline/stocking/generate/match.py` + `identifiers.py`: the unsound old matcher. Retire it.

### Proposed matcher (Opinion)
Signals in order. The name is a confirmation, never a picker.
1. **Source id**: FIDQ `wbody_id`. It is stable across FIDQ, FISS and the Releases database, so the frozen file keys
   on it and a feed refresh never re-matches a known id.
2. **Code**: WBID → FWA polygons with that `WATERBODY_KEY_GROUP_CODE_50K`.
3. **Point** (new): the FISS point for the same `wbody_id` (OGL). It must fall inside, or within **100 m** of, one of
   the code's polygons (the T634 chart limit). This is the evidence that does not come from the code.
4. **Name**: the FIDQ gazetted name or alias against the lake's GNIS names (equal / word order / independent curated
   variant). It is a confirmation only.
5. **Region / watershed**: shown in review as evidence (FISS `NEW_WATERSHED_CODE` against the lake's FWA code). It
   is not a gate [not measured].

### Confidence classes, measured on the 2,318 FIDQ stocked lakes [measured]
FISS points were fetched from the public WFS (8 requests, scratch only) and tested point-in-polygon against the
local FWA lakes + manmade layers.

| Class | Rule | All | Stocked since 2020 | Lane |
|---|---|---|---|---|
| A `id_name_point` | code → one named lake, name agrees, point inside (644) or ≤ 100 m (1,030; median 18 m, p90 53 m) | **1,674 (72.2 %)** | 602 (77.9 %) | bulk accept + sample |
| B `id_point_unnamed` | code → one lake with no GNIS name, point inside or ≤ 100 m | **490 (21.1 %)** | 132 (17.1 %) | bulk accept + sample (owner call) |
| C point disagrees | name and code agree, but the point is nearer another polygon (28, e.g. Nicola, Alouette, Lac Le Jeune: likely lakes in pieces) or > 100 m from any lake (49) | 77 | | review |
| D two named lakes | code covers 2+ named lakes; the point picks one in 4 of 9 (Buntzen, Mara, Moffat, Illusion) | 9 | | review with the suggestion |
| E no lake for the code | the point lands in some lake in 7 of 32 | 32 | | review with the suggestion |
| F other | name_disagrees 4 (3 point-agree), variant_circular 1, unnamed code with 2-3 polygons 11, no point 1 | 17 | | review |
| **Auto (A+B)** | | **2,164 (93.4 %)** | **734 (95.0 %)** | |
| **Review (C-F)** | | **154 (6.6 %)** | **39** | |

Not verified: the 77 class-C rows were not looked at on a map. The 100 m limit was carried over from charts, not
tuned. FISS points sit near the shore (median 18 m outside the polygon), so a point is a check on the code, not a
picker on its own.

### Hard cases (Opinion)
| Case | Rule |
|---|---|
| Same-named lakes | The name never picks. The code picks and the point confirms. A name used by several lakes needs nothing extra |
| Unnamed lakes | Class B: the code and the point agree. FIDQ often names them by alias ("LAKE #2085"); keep the sourced words as shown |
| Lake split into parts | Match to the lake's item (`wbk:`). Every part the reading release holds shows it. A FIDQ lake whose point falls in a sibling polygon (class C) is one owner tick |
| Several FIDQ ids on one lake | All attach. Rows merge in the feed, each keeping its `fidq` id |
| Hatchery ponds / not public | Owner marks `refused: not_public` once. It stays refused across refreshes |
| **Streams** (657 waterbodies, 8,333 rows, 266 since 2020) | The FISS point is a release site, not the whole water. Match it as a **place**: FWA watershed code → blue line key, plus the point's measure along it (AGENTS 5 allows blk + metres). The feed then says "stocked at km X". Not measured; FIDQ was fetched for lakes only |
| First-time id in a refresh | Not shown. Goes to the proposal list and the owner is told. Never guessed |

## 4. Data shapes (Opinion)

### Curated match row: `data/curated/stocking/matches.json`, format `stocking-matches/1`
```json
{"format": "stocking-matches/1",
 "matches": {
  "fidq:101": {"wbid": "00755LFRA", "kind": "lake", "item": "wbk:329532606",
               "class": "id_name_point", "status": "matched",
               "evidence": {"names": ["TROUT LAKE"], "lake_names": ["Trout Lake"], "point": [-122.1, 49.2],
                            "point_m": 0, "code_polygons": ["wbk:329532606"], "fwa_wsc": "100-…"},
               "accepted": {"by": "owner", "at": "2026-…", "via": "bulk:id_name_point"},
               "licence": "owner says OGL, catalogue says Access Only, unconfirmed"},
  "fidq:310999": {"wbid": "00000LFRA", "kind": "stream", "place": {"blk": 356566667, "m": 4681}, "...": "..."},
  "fidq:52": {"status": "refused", "reason": "not_public", "accepted": {"...": "..."}}}}
```
- It is keyed by the source's stable id, and the value is a durable FWA id (`item` for a lake, `place` for a stream
  site). It never holds a section id.
- One writer (`accept`). A re-run only reports new ids, vanished ids, and accepted rows whose evidence moved
  (`gauge-regenerate-drifts`).

### Feed: `feeds/stocking/index.json`, format `stocking-feed/1` (registered in `pipeline/common/feed_formats.py`)
It follows `inseason-links/4`: keyed by durable item id, no release, no section id, so any release can read it.
```json
{"format": "stocking-feed/1", "digest": "…", "as_of": "2026-06-29", "checked": "2026-11-13T09:00Z",
 "source": "…", "credit": "…", "licence": "…",
 "dict": {"species": ["Rainbow Trout", "…"], "stage": ["Yearling", "…"], "strain": ["Pennask", "…"]},
 "waters": {"wbk:329532606": {"last": "2026-05-14", "n": 67, "first": "1939-01-01",
                               "rel": [["2026-05-14", 0, 0, 1500, 34.0, 3]]},
            "blk:356566667": {"places": [{"m": 4681, "last": "…", "rel": [["…"]]}]}},
 "unplaced": 12}
```
- `rel` row: `[date, species, stage, count, grams, strain]`, newest first. FISS-only fields (no count, strain or
  grams) carry `null` and the app says "count not published", never 0.
- **Sizes** from the 63,927 fetched FIDQ rows on the 2,272 class A+B lakes [measured]: full history 2.0 MB raw,
  **354 KB gzip**; the last 5 years only (751 lakes, 5,584 rows) 188 KB, **35 KB gzip**; a last-stocked index alone
  64 KB, 12 KB gzip. Opinion: one file with 5 years plus `n`/`first`, and full history per lake on tap
  (`feeds/stocking/w/<item>.json`, median a few KB) if the owner wants history beyond 5 years.
- The release's `lakes/1` stocked day per section and the bundle's `stock_water` / `release` tables would then be a
  second copy of the same fact. Opinion: the feed is the one source for stocking; retire `release` (keyed by name)
  and `stock_water`, keep `lakes/1` for depth only. The Stocked colouring reads the feed through the item ids the
  release holds.

### Who publishes
- FIDQ is an HTML scrape with a session (250 group requests, about 5 minutes at 1 s apart). Opinion: run it as a
  pipeline step on the Mac (`publish.py` with the in-season guards: refuse unmatched ids, a stale stamp, an unknown
  format), weekly or monthly, since the source itself lags months. The feeds worker serves the file, checks its age
  (heartbeat) and e-mails when it goes stale. No GitHub cron.
- If only the OGL FISS layer is used, the worker could fetch it itself: one WFS request with a date filter, JSON.

### What the app shows, and where
- **Lake tab** (T634 decision; the fourth tab): a Stocking card. "Last stocked 14 May 2026: 1,500 Rainbow Trout
  (yearling, 34 g)". Then the last 5 years grouped by year, then "Records as of 29 Jun 2026 (Province of B.C.)".
  A lake with nothing says "No stocking record", never "not stocked".
- **Map**: the existing Stocked colouring (`STOCK_RECENCY` bands in `core/lakeFacts.ts`), fed from the feed. T158's
  colour-blind ramp clash is still open.
- **Streams**: a card on the section whose line holds the place (the T673 touch rule).
- **Failures**: an unknown format is refused and reported. A feed older than its stale limit shows "may be out of
  date (last checked …)". A failed fetch is reported (`reportFailure`), never an empty list. Words and rows come
  from `core`, so web and phone match. No matching in the app.

## 5. Effort, first step, risks

| Piece | Size | Note |
|---|---|---|
| Matching + review: add FISS point evidence and the A-F classes to `propose`; owner bulk-accepts A (+B?), ticks about 154; retire `stocking/generate/` | **M** | The propose/accept chain exists. A small review page beside the map would keep the owner's time under an hour |
| Streams as places | **M** | FIDQ stream fetch, plus blk/measure from the FISS point; not measured |
| Feed publish: format, publisher with guards, worker heartbeat / status, R2 path | **M** | Copies the `inseason-links/4` pattern |
| App: Stocking card in the Lake tab, Stocked colouring on, stale/as-of words, tests | **M** (card alone); the Lake tab itself is **L** (T634 item 6, shared with depth charts) | |

**Recommended first step** (Opinion): extend `pipeline.stocking.propose` with the FISS point (open data, no
licence question) and the classes above. Regenerate the proposals and show the owner the 154 review rows on a page.
This costs no credits, publishes nothing, and makes the frozen file ready for the day the licence is settled.

**Risks**
- Licence: three records are "Access Only" and the owner's OGL link is still owed (T634). Nothing derived from FIDQ
  counts may be published until it arrives.
- Freshness: the Province's data was 4.5 months behind on 2026-11-13. Anglers may expect GoFishBC's near-live
  report, and the app would look stale in autumn.
- FIDQ is a scrape: an HTML change breaks it. A broken fetch must show as a failed run, not an empty feed.
- FISS ↔ FIDQ row equivalence is assumed (66,487 FISS lake rows against 64,991 fetched FIDQ rows) [not verified 1:1].
- 109 FIDQ groups returned no data block in T634's fetch and were not checked one by one.

## Questions only the owner can answer
1. **Licence**: the OGL link for "Provincial Fish Stocking Reports" / FIDQ, or should we ask the Province (and FFSBC
   for GoFishBC's current report)?
2. If the licence stays open: ship an **open-only first version** from FISS (date, species, life stage; no counts or
   strain), or wait?
3. Class B (490 unnamed lakes where code and point agree): bulk-accept with a sample, or tick each one?
4. Stocked streams: in scope for the first version, or lakes only?
5. History depth in the app: 5 years, or everything back to 1900 on tap?
6. Is a 4-month lag acceptable, or is a near-live source (GoFishBC, which needs consent) a must?

## Sources
- FIDQ fetch and stocking display on `main`: `pipeline/recurring/anglerinfo/{README.md,fetch_stocking.py,match.py,
  match_fwa_gazette.py,match_fwa_identifier.py,match_fwa_override.py,match_final.py}`,
  `pipeline/recurring/stocking/resolver.py`, `.github/workflows/update-stocking.yml`, `scripts/update-stocking.sh`,
  `webapp/src/services/waterbodyDataService.ts`, `webapp/src/components/InfoPanel.tsx` (read with `git show main:`).
- B.C. Data Catalogue API `package_show` (read 2026-11-13): `provincial-fish-stocking-reports` (Access Only, "daily",
  "1900-present"), `known-bc-fish-observations-and-bc-fish-distributions` (OGL), `freshwater-atlas-lakes` (OGL),
  `wsa-water-polygon-features-50-000` (OGL), `freshwater-atlas-20k-50k-waterbody-cross-reference-table` (OGL),
  `fish-information-data-query` (Access Only). https://catalogue.data.gov.bc.ca/
- FISS WFS: `https://openmaps.gov.bc.ca/geo/pub/wfs?…typeName=pub:WHSE_FISH.FISS_FISH_OBSRVTN_PNT_SP`,
  `CQL_FILTER=SOURCE LIKE 'Releases Database%'`.
- OGL-BC terms: https://www2.gov.bc.ca/gov/content?id=A519A56BC2BF44E4A008B33FCF527F61. The access-only copyright page
  and the GoFishBC terms are quoted in `T608D-findings.md` §2.
- This branch: `pipeline/stocking/propose.py`, `data/generated/stocking/proposals.json`,
  `data/curated/gauges/matches.json`, `pipeline/deliver/bundle/schema.sql` (stocking tables),
  `app/packages/core/src/lakeFacts.ts`, `pipeline/common/feed_formats.py`, T673 (`inseason-links/4`).
- Measurement scripts and the fetched FISS pages are in the session scratchpad only; nothing was added to the repo.
