# Lake depth charts on the map: feasibility, sources, matching and data shapes

Research only, 2026-11-13. No product code, data, ticket or ledger was changed. Branch `redesign/stream-sections`.
Builds on what is already done here: the T608 D audit (`handoff/PLAN/app-integration/T608D-findings.md`), T634's three
passes (`tickets/T634-lake-tab-depth-and-stocking.md`), `handoff/PLAN/bathymetry/SOURCES.md`, and the deferred tickets
T201, T218, T219 and T158. Where this note repeats a number from those, it says so.

Labels: **[fact]** checked today against a file, the web or an API. **[repo]** taken from an earlier pass in this repo
and not re-run. **[unverified]** from search snippets or memory. **[opinion]** my view.

## 1. Summary

- **Production (`main`) shows depth charts today**, as a "Bathy map" tab that opens the government PDF scan from our
  own storage. A lake is tied to its charts by FWA `WATERBODY_KEY`. The match chain behind it searches **by name
  first**: the nearest lake with that name wins, with no distance limit. The chart's own lake code is only tried
  after that. Nobody reviews the matches. [fact]
- **The best source by far is still the Province's 2,600 scanned chart PDFs** (Open Government Licence – BC). Each one
  carries the Province's 50K lake code (`00078OKAN` for Okanagan Lake), which our FWA lakes also carry. [fact]
- **Vector depth data does exist, but it is patchy.** The Province has 28 lakes as traced contours and 510 old vector
  files, of which 298 fit their lake strictly. Both are marked "Access Only" in the catalogue, and the owner still
  owes the licence link. Outside the Province: CHS NONNA covers only Harrison, Pitt and the lower Fraser among
  interior waters. Hakai/CIOOS has multibeam surveys of a few lakes (Seton, Anderson, Owikeno, Horne) under CC BY.
  OBWB has near-shore lidar of the Okanagan lakes, licence unknown. **No open full-depth vector dataset exists for
  Okanagan Lake.** Its only full-lake depths are the Province's 6-sheet scan, which is not vectorised. [fact, details
  in section 3]
- **A matcher already exists and follows the owner's rules** (`pipeline/bathymetry/propose.py`). The lake code
  picks the lake; name, position and size only confirm it. It proposes 1,995 of 2,294 chart codes as clean (87 %)
  and sends 299 codes plus 44 code-less sheets to the owner. [repo; counts re-read from
  `data/generated/bathymetry/proposals.json` today]
- **Recommendation [opinion]:** do not write a new matcher. Rank `propose.py`'s output into confidence classes. The
  owner reviews it into one frozen file. Then ship the 200 dpi PDFs from our own storage behind a "Depth chart" card
  in the Lake tab. Vector contours in the tiles come later, as their own layer, lake by lake, once the licence is
  settled.

## 2. What production does today (`main`)

### How it works [fact, read with `git show main:<path>`]

| Step | File on `main` | What it does |
|---|---|---|
| Fetch | `pipeline/recurring/anglerinfo/fetch_bathymetry.py`, `fetch_wdic.py` | Open chart table (CSV) into `bathy_surveys`; the whole WDIC 1:50K waterbody layer (about 311,652 identifiers) into `wdic_cache` |
| Match 1 | `match.py` | Chart code (`WATERBODY_IDENTIFIER_WSA_50K`) joined to WDIC, which gives an approximate point per code. README: 2,147 of 2,147 charts |
| Match 2 | `match_fwa_gazette.py` | (a) name overrides from `pipeline/matching/overrides.json`, gated at 50 km from the WDIC point; (b) **gazetteer name search over FWA lakes, wetlands and man-made polygons, then the candidate closest to the WDIC point wins** (`_closest`, lines 266–345). Tier (b) has **no distance cap** |
| Match 3 | `match_fwa_identifier.py` | Only for rows still unmatched: the code joined to FWA's own `WATERBODY_KEY_GROUP_CODE_50K`; ties broken by the same nearest pick |
| Match 4 | `match_fwa_override.py` + `wbid_overrides.json` | Hand rows keyed by code, each with a list of `waterbody_keys` (one code can name two lakes, e.g. Spectacle–Swan) |
| Aggregate / export | `match_final.py`, `export.py` | `anglerinfo_matches.json`: `wbk_bathymetry: {"<WATERBODY_KEY>": [{pdf, title, name}]}`, plus `wbk_names` (chart titles become name variants of the lake) |
| Build | `pipeline/enrichment/anglerinfo_accessor.py`, `reach_builder.py` lines 434–490, `builder.py` lines 313–329 | Every reach (polygon) on that `WATERBODY_KEY` gets `bathymetry: [{pdf, title}]`; the PDFs are copied to `deploy/bathymetry/` |
| Serve / show | `r2-worker/src/index.ts` line 226, `webapp/src/components/InfoPanel.tsx` lines 213–245 and 1463–1496, `mobile/src/config.ts` line 26 | "Bathy map" tab, opened in a new browser tab from `<R2>/bathymetry/<pdf>`; a lake with several sheets has them merged into one PDF in the browser |

**Match key:** FWA `WATERBODY_KEY`. It is reached by WDIC code, then **name + nearest**, then code. The README
reports 5,223 of 5,228 rows matched across stocking, charts and markers [fact: README text; not re-run].

### What breaks [fact for the mechanism; examples counted from today's proposals]

| Case | Why it breaks on `main` | Size of the problem |
|---|---|---|
| Same-named lakes | The name search runs **before** the code join. The nearest same-named lake to the WDIC point wins, with no limit. If that point is off, or the right lake has no polygon under that name, a neighbour of the same name takes the chart | 252 chart titles are shared by 587 chart codes (e.g. "SUMMIT LAKE" ×7, "BEAVER LAKE" ×6) |
| Lakes with no gazetted name | The gazetteer finds nothing, so these fall to the code join (stage 3). That works, but nobody looks at it | 143 codes sit only on unnamed FWA lakes today |
| One code, several lakes | The hand rows give a list of keys, and the export fans the chart out to every key in it. Without a hand row, the nearest-pick tie-break chooses one | 16 codes cover two or more named lakes; 31 more also cover unnamed ponds |
| Multi-sheet / multi-basin lakes | Sheets are grouped by key and merged in the browser. A lake split into several FWA polygons gets the chart on each reach (fine). A sheet covering only one arm cannot say which arm | 384 codes have more than one sheet (2,790 sheets in all); Mabel Lake has 10, Okanagan 6 |
| Circular names | Chart titles are written back as name variants of the lake (`wbk_names`). A later name match then "confirms" itself | 69 codes agree only through such variants (`variant_circular` in `propose.py`) |
| Silent fallback | `load_anglerinfo` "never raises": a missing match file just gives no charts, with only a log warning | Against this branch's no-silent-fallbacks rule |
| No review | Matching is automatic plus hand JSON. There is no proposal → owner review → frozen file step | Against the gauge rule (one frozen, reviewed file) |

Not checked: whether the live production site still serves these PDFs today. It was not opened.

## 3. Sources

### 3a. Provincial (BC Data Catalogue) — the main source

| Source | Format | Coverage | Identifiers carried | Licence (catalogue `license_title`, read 2026-11-13 via CKAN `package_show`) | Freshness |
|---|---|---|---|---|---|
| [Bathymetric Maps – Open](https://catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2) | One scanned PDF per sheet (1-bit or grey raster with an OCR text layer, **not georeferenced**) plus a CSV | 2,741 CSV rows; 2,599 PDFs in our 200 dpi set (201 MB) [repo] | `LAKE_ID_WSA_50K`, `WATERBODY_IDENTIFIER_WSA_50K` (= FWA `WATERBODY_KEY_GROUP_CODE_50K`), `WATERSHED_CODE_WSA_50K`, `GAZETTED_NAME`, `MAP_TITLE`, `DRAFT_DATE`, `DATUM`, `SHEET_NO`, PDF file name. **No coordinates.** | **Open Government Licence – British Columbia** [fact] | Last modified 2023-08-23; status "underDevelopment" [fact]. Surveys mostly 1950s–1990s [repo] |
| [Bathymetric Maps](https://catalogue.data.gov.bc.ca/dataset/bathymetric-maps) (sheet layer `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW`) | WFS/WMS polygons: each sheet's lake outline | 2,744 sheets, 2,265 codes [repo] | `waterbody_identifier`, `lake_id`, `gazetted_name`, `map_title`, `map_scale`, `vectorized_flag`, sheet outline **geometry** | **Access Only** [fact]; the owner says OGL, link owed (T634) | Record touched 2026-11-12, "onGoing" [fact] |
| 2001 vector files (E00 zips linked from that layer) | ARC/INFO E00: shoreline, contour lines, soundings, in sheet units (not georeferenced) | 510 zips (187 MB) for 516 of 517 flagged codes; 298 fit their FWA lake strictly, 117 loosely, 83 poorly [repo, T634 third pass] | Code in the file name or sheet layer | No record of its own; reached only through the Access Only layer [repo] | 2001 |
| [Bathymetric Maps of Surveyed Lakes](https://catalogue.data.gov.bc.ca/dataset/bathymetric-maps-of-surveyed-lakes) (`WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP`) | Georeferenced contour polygons (EPSG:3005) | 28 lakes, 137 polygons; 25 clean against FWA [repo] | **FWA `WATERBODY_KEY`**, 50K code, gazetted name; max/mean depth, volume, survey date | **Access Only** [fact] | "completed" |
| [Lake Inventory Surveys](https://catalogue.data.gov.bc.ca/dataset/lake-inventory-surveys) | Table | Lakes with a survey | 50K code | OGL-BC [repo] | — |
| FIDQ / HabitatWizard | Web query pages over the same data | — | WBID (= 50K code) | Access Only [repo] | — |

Terms quoted: the BC licence permits you to "Copy, modify, publish, translate, adapt, distribute or otherwise use the
Information in any medium, mode or format for any lawful purpose", with the attribution line "Contains information
licensed under the Open Government Licence – British Columbia." For Access Only records, the Province's copyright
page says reproduction "is not permitted" without written permission [repo, T608 D].

### 3b. Wider hunt: other digital or vector depth data (owner ask: "find em all")

| Source | Lakes | Kind / resolution | Date | Access | Licence for rehosting and showing on our map | Status |
|---|---|---|---|---|---|---|
| **CHS NONNA-10 / NONNA-100** ([open.canada.ca](https://open.canada.ca/data/dataset/d3881c4c-650d-4070-bf9b-1e00aabf0a1d), portal [data.chs-shc.ca](https://data.chs-shc.ca/)) | Interior BC: **only Harrison Lake, Pitt Lake and the lower Fraser (Mission–Hope)**. No tile at Okanagan, Kalamalka, Skaha, Osoyoos, Kootenay, Arrow, Shuswap, Quesnel, Babine, Stuart, François, Williston, Kinbasket, Cowichan, Great Central or the Windermere/Columbia lakes | Gridded depth at 10 m / 100 m (GeoTIFF, XYZ, BAG, CSAR), WCS/WMS | Updated 2026-04-01 | Free download | Listed as OGL-Canada, but the attached **CHS NONNA Licence** governs: "shall not be sold, licensed, leased, assigned or given to a third party" (clause 2). A derivative product is allowed if it carries a set notice, including "contains intellectual property of the Canadian Hydrographic Service… does not meet the requirements of the Navigation Safety Regulations, 2020" (clause 7). So: contours we make from it can probably be shown, but the grid cannot be rehosted. **Needs the owner's reading** | [fact]: coverage tested today against the NONNA-10 tile index (72,305 tiles; 17 east of 122.2° W, all Harrison/Pitt/Fraser) |
| **CHS nautical charts** | 3050 Kootenay Lake, 3052 Okanagan Lake (1:50,000), 3053 Shuswap Lake (1:50,000), 3061 Harrison Lake (1:40,000); RM-PAC04 "Lakes and Rivers of BC" | Paper / raster / ENC | Various | $50 per ENC or PDF chart | "A licence is required to use CHS intellectual property"; "VARs are not permitted to digitize CHS paper charts" ([CHS copyright page](https://chs-shc.gc.ca/copyright-droitdauteur/index-eng.html)). **Not usable** without a paid licence | [fact] for terms; chart numbers [unverified, retailer and raster-table snippets] |
| **OBWB Okanagan Lakes topobathymetric lidar** ([NV5 report, 2022](https://www.obwb.ca/newsite/wp-content/uploads/Study_7_Okanagan_Lakes_Bathymetry.pdf)) | Okanagan, Kalamalka, Wood, Ellison, Skaha, Vaseux, Osoyoos | 1 m topobathymetric DEM (COG) plus LAZ points; **near-shore only** (green lidar stops at clear-water depth, a few metres) | Sept–Oct 2021 | Delivered to OBWB; no public download found | **Unknown**: ask OBWB | [fact] from the report text; depth reach and licence [unverified] |
| **Hakai / CIOOS Pacific** | [Seton + Anderson](https://catalogue.hakai.org/dataset/ca-cioos_d7ffd737-5725-4a56-9134-a9ad91c2734d) (2024 multibeam, with GSC); [Horne Lake](https://catalogue.hakai.org/dataset/ca-cioos_7e49967a-b5ab-443d-860b-e3c0eb0f709c) (2023 multibeam); [Owikeno](https://catalogue.hakai.org/dataset/ca-cioos_27ba6c11-2421-4e85-bc11-1c1083514ed9) (2019 multibeam); [six Calvert/Hecate Island lakes](https://catalogue.hakai.org/dataset/ca-cioos_b52d5602-f81d-4565-9574-e448e99bc997) (2016, 1 m raster) | Full-lake multibeam grids | 2016–2024 | Request form (hecate.hakai.org) | **CC BY 4.0** on the Seton/Anderson and Horne pages ("Appropriate credit must be given to Hakai Institute…"); one search snippet said CC BY-NC for Horne | Seton/Anderson [fact]; Horne licence conflicts [unverified]; Owikeno CC BY [unverified] |
| **Columbia Basin Water Hub (Living Lakes Canada)** | Kootenay Lake FIMP 2021; Arrow Lakes FIMP 2022 (DOI 10.48511/qzwt-2h33) | Foreshore mapping. Bathymetry not confirmed | 2021–22 | Open portal | Not read | [unverified] |
| **UNBC Quesnel River Research Centre** | Quesnel Lake (studied after Mount Polley) | Possibly multibeam | — | "On agreement" | Unknown | [unverified] |
| **GSC Open File 8963** | BC coast topo-bathymetric DEM (3 m / 10 m) | Coastal | 2021 | Open | OGL-Canada | Probably no interior lakes [unverified] |
| **LidarBC** | Province-wide land lidar | Topographic only; water surface, no lake bottom | ongoing | Open | OGL-BC | No depth found [fact: no lake bathymetry product listed] |
| **HydroLAKES** | Every lake ≥ 10 ha | **Modelled** mean depth and volume per lake, no shape | 2016 | Open | CC BY 4.0 [unverified] | Modelled, not surveyed: never show as a chart |
| **GLOBathy** ([paper](https://doi.org/10.1038/s41597-022-01132-9)) | 1.4 M lakes | **Modelled** depth grid from shape and HydroLAKES max depth | 2022 | Figshare / Earth Engine | "assumed CC0" in catalogues [unverified] | Modelled: a "rough depth" layer at most, clearly labelled |
| GEBCO | Global | Ocean grid; lakes flat or missing | — | Open | Public | Not useful |
| OpenStreetMap / OpenSeaMap | Crowd tracks | Raw sounder tracks | — | Open | ODbL (share-alike on derived data) | BC lake coverage near zero [unverified] |
| Navionics / Garmin Quickdraw / C-MAP / LakeMaster / Fishidy | Many popular lakes | Proprietary charts | — | Inside their apps only | **No reuse.** Extraction is not permitted; Quickdraw shares only inside Garmin devices [repo, SOURCES.md; unverified today] | Do not use |
| Backroad Mapbooks, FFSBC / GoFishBC "depth map" links | Stocked lakes | Redrawn copies of the provincial scans | — | — | © all rights reserved | Do not copy; the provincial originals are the open version |

### 3c. Popular lakes, one row each (Okanagan first)

Provincial chart status comes from `proposals.json`. Vector fit comes from `~/data-offgit/bc-bathymetry-vectors/
placing_all.json` (rigid fit to the FWA outline; strict = IoU ≥ 0.9, median ≤ 25 m, area 0.8–1.25).

| Lake | Provincial PDF chart (OGL) | Provincial vector (Access Only) | Other digital source | Best path to contours on the map |
|---|---|---|---|---|
| **Okanagan** | 00078OKAN, 6 sheets, clean | none (not vectorised) | OBWB lidar, near-shore only; CHS chart 3052 (licensed) | Trace the 6 scans (T219 method: multi-sheet fits are the hard case) or license CHS 3052. **No open vector exists** |
| Kalamalka | 00209OKAN, 3 sheets, clean | yes, **loose** (IoU 0.90, median 27 m; units not stated) | OBWB lidar, near-shore | Owner tick, then ship |
| Wood | 00199HARR / 00418OKAN, clean (one sheet titled "wood lake fake": check) | — | OBWB lidar, near-shore | Trace scan |
| Skaha | 01367OKAN, 4 sheets, clean | yes, **strict** (IoU 0.98) | OBWB lidar, near-shore | Owner tick, then ship |
| Osoyoos | 01699OKAN, 3 sheets, clean | yes, **poor** (IoU 0.43) | OBWB lidar; US-side survey (IJC) | Manual placing |
| Kootenay | **no provincial chart** | — | CHS chart 3050 (licensed); no NONNA | Licence CHS, or none |
| Upper Arrow / Lower Arrow | 00087UARL clean / 00099LARL **no FWA lake under code** | Lower: zip exists, not placed | CHS (RM-PAC04, licensed) | Review the Lower Arrow code first |
| Shuswap | 00672SHUL clean (5 sheets), 01089SHUL two_named | — | CHS 3053 (licensed) | Trace scans |
| Mabel | 00116USHU, 10 sheets, clean | — | — | Trace scans |
| Harrison | 00081HARR, 2 sheets, clean | — | **NONNA-10** (derivative contours with CHS notice) | Contour NONNA, if the owner accepts the NONNA terms |
| Pitt | 00288LFRA, clean | — | **NONNA-10** | As Harrison |
| Seton / Anderson | 00533SETN / 00571SETN, clean | — | **Hakai 2024 multibeam, CC BY 4.0** | Request the grid, contour it, credit Hakai |
| Stave / Alouette | clean / clean | Stave: not fitted; Alouette: **strict** | — | Alouette: owner tick |
| Cowichan | 00408COWN, clean | **strict** (0.96, metres) | — | Owner tick |
| Sproat | 01128ALBN, clean | poor (feet) | — | Manual |
| Christina | 01206KETL, clean | loose | — | Owner tick |
| Slocan | 00115SLOC, clean | loose (median 106 m) | — | Manual check |
| Kamloops | 00293THOM, clean | poor (IoU 0.30) | — | Manual |
| Lac la Hache / Sheridan / Moberly | clean | **strict** (Sheridan in feet) | — | Owner tick |
| Babine / Stuart / François / Fraser L. / Chilko / Adams / Nicola | clean | — | — | Trace scans |
| Quesnel | **not found in the chart titles** | — | UNBC (by agreement) [unverified] | Ask UNBC |
| Williston / Kinbasket | none / 00028CANO position_or_size | — | BC Hydro (none found public) | None today |

**Coverage counts:**
- PDF charts on about 2,000 lakes.
- Placed vector depths: 298 strict + 117 loose, about 413 distinct lakes, owner's tick needed. A few more could come
  from NONNA (2 lakes) and Hakai (about 10 lakes).
- Full-depth open vector data for the big Okanagan, Kootenay and Shuswap lakes: **none**.

## 4. Matching design against this branch's atlas

### 4a. Reuse, do not add [opinion, following the owner's 2026-11-11 rule "do not write a second matcher"]
`pipeline/bathymetry/propose.py` already does what the brief asks:
- The **source code → FWA crosswalk** is exact. The 50K code is `WATERBODY_KEY_GROUP_CODE_50K` on the FWA lake.
- A name never picks a lake; it only confirms or refuses one.
- **Position and size** are tested against the sheet outline: centre within 100 m of the lake's box, area ½–2×.
- Ten named outcomes: `clean`, `held`, and eight review lanes.
- `pipeline/bathymetry/accept.py` is the only writer of the frozen file. Only `clean` can be accepted in bulk.

Items 1–3 below are proposed refinements. Item 4 is a second front end for sources that have no code.

1. **Confidence classes on top of `rule`** (proposed):

   | Class | Meaning | Today's count (chart codes) |
   |---|---|---|
   | A — code + name exact | `clean` with name evidence `equal` or `word_order` | 1,739 |
   | B — code + name weaker | `clean` via `contains` (title holds the name, e.g. "WILSON L. LITTLE WILSON L.") or an independent curated `variant` | 256 |
   | C — owner review | `two_named` 16, `unnamed` 143, `no_lake` 17, `name_disagrees` 2, `gazetted_only` 8, `variant_circular` 69, `no_outline` 27, `position_or_size` 17, plus 44 sheets with no code | 343 rows |

   Auto-proposable: **1,995 of 2,294 codes (87 %)**. Class A alone: 76 %. Under the owner's rule, everything is still a
   proposal, so A and B become owner bulk ticks in the review app, not silent accepts.
2. **Point-in-polygon instead of box centre** [proposed]. The open CSV has no coordinates, so the sheet outline (Access
   Only layer) is the only geometry. Test its interior point inside the FWA polygon, and polygon IoU where the outline
   is a real shoreline. Today's check compares box centres. Class A could then require IoU ≥ 0.8.
3. **Many-sheet lakes:** keep the sheets as an ordered list on one match row. Never one row per sheet.
4. **Sources without a 50K code** (NONNA, Hakai, OBWB, a future traced contour): match by **footprint**. Take the
   survey's coverage polygon or 0 m contour. Candidate lakes are the FWA lakes it overlaps. The score is the share of
   the lake covered and the share of the survey inside the lake. Name is a tie-break only. All of these go to class C:
   they are few, and each brings its own licence line.

Stored by `wbk:<WATERBODY_KEY>` (the durable lake id, AGENTS rule 5), never by section id. The release resolves it
to sections, as `lake_facts.bind` already does.

### 4b. The curated match row (proposed shape for `data/curated/bathymetry/matches.json`)
Modelled on `data/curated/gauges/matches.json` (`trust`, `evidence`, `resolved_by`) and T634's proposal rows.
```json
{
  "_about": "Chart matches, FROZEN. Written only by pipeline.bathymetry.accept on the owner's word.",
  "format": "bathy-matches/1",
  "charts": {
    "00209OKAN": {
      "chart_id": "00209OKAN",
      "item_id": "wbk:329xxxxxx",
      "status": "matched",
      "trust": "reviewed",
      "class": "A",
      "rule": "clean",
      "evidence": {"name_evidence": "equal", "title": "KALAMALKA L.", "area_ratio": 1.01,
                   "centre_outside_m": 0, "outline_iou": 0.93, "wsc_agrees": true},
      "sheets": [{"sheet_no": 1, "pdf": "00133001.pdf", "title": "KALAMALKA L.", "drafted": "1974", "units": "feet"}],
      "source": "bc-bathy-open",
      "licence": "OGL-BC",
      "reviewed": "2026-11-xx"
    }
  }
}
```
- `status`: matched | none (no such water) | wrong (worklist).
- `trust`: bound | reviewed | auto (if the owner ever allows it) | wrong | none.
- One row per source id. A multi-lake code becomes several rows only by the owner's decision (`item_id` a list).

## 5. Data types: release shape, sizes, offline

| Piece | Shape | Size | Notes |
|---|---|---|---|
| Chart list in the bundle | The existing `chart` table (`pipeline/deliver/bundle/schema.sql` line 1146: `item_id, name, chart_id, title, kind, drafted, scale, area_km2, pdf`). Proposed added columns: `sheet_no`, `bytes`, `sha256`, `units`, `licence`, `source`; meta `chart_format = "charts/1"` | About 2,450 rows for the clean set; **40–60 KB** [estimate, T608 D] | Bundle is the only source for readers; the app never matches |
| Lake survey state | Existing `lakes/1` (`pipeline/deliver/lake_facts.py`): per section, survey 0 none / 1 sheet / 2 digitised | Under 10 KB | Feeds the already-built Depth map colouring |
| PDFs | On our storage (R2) under content names, `bathymetry/<sha256-12>.pdf`; never bundled | 2,599 files, **201 MB** at 200 dpi 1-bit [repo]; median about 60–80 KB [estimate] | Fetched on tap; size shown first; kept on the phone after first open; "save this lake offline" pre-fetches |
| Max/mean depth | Columns on `chart` or a small `lake_depth` table | Tiny | From the 25 contour lakes and the PDF statistics text layer (T219 showed this is readable) |
| Vector contours (later) | A **separate** PMTiles file, `depth/1`: line + polygon layer `depth_contour` (`wbk`, `depth_m`, `src`, `units_src`), z10–z15 | 25 lakes: about 2.5 MB GeoJSON raw, well under 1 MB tiled; about 415 placed lakes: **10–30 MB** [estimate, not measured] | Separate file, so the basemap and atlas tiles keep their own vintage (tiles-and-bundle rule) and offline packs can skip it |

No silent fallbacks:
- A chart row whose PDF fails to load shows the failure and goes through `reportFailure`.
- A release whose `chart_format` the app does not know is refused, not read loosely.
- A lake with no chart shows nothing. It never says "no chart exists".

## 6. Display options and effort

| Option | What the angler gets | Size | Licence fit | Notes |
|---|---|---|---|---|
| 1. Link out to the government PDF (a100.gov.bc.ca) | Tap opens the Province's page | S | Allowed | Breaks the self-host rule, no offline use, Province URLs have moved before. Not recommended |
| 2. **Rehost the 200 dpi PDFs** + "Depth chart" card in the Lake tab | In-app viewer (react-native-pdf on phone, browser tab on web), offline after first open | **M** (S storage + M app card + owner review hours) | OGL-BC, fine | Production already proved the idea; the matching is what changes |
| 3. Georeferenced raster overlay (scan on the map) | Chart image draped on the lake | L | OGL-BC for the scans | Needs a fit per sheet. The 415 vector placings give a transform for their sheets; the rest need T219's shoreline fit. Owner has deferred "scan over the map" |
| 4. True vector contours in tiles | Depth lines and shading, zoomable, offline | **L** for the about 400 placed lakes; **XL** to trace scans at scale | Blocked on the Access Only answer; NONNA/Hakai lakes each need their own credit line | Needs a unit tick (110 not stated), an owner tick per placing, a new tile layer and style (T158 colour exists) |
| 5. Depth colouring only (`lakes/1`) | Lakes tinted "has chart" / "digitised" | S | Fine | Already built and greyed; switches on with option 2 |

**Recommended first step [opinion]:**
- Option 2 plus option 5, in the order T634's "path to ship" gives: owner review of the chart matches (classes A and
  B in bulk, the 343 one by one), the PDFs to staging R2, then the Lake tab card.
- Option 4 starts with the 25 clean contour lakes plus the strict placings (Skaha, Cowichan, Alouette, Lac la Hache,
  Moberly…), once the licence answer is in.
- Okanagan Lake specifically needs either a CHS licence or tracing its 6 scans. Neither is cheap.

## 7. Risks

- **Licence:**
  - The sheet layer, the placed contours and the 2001 vector files say "Access Only" today. The owner's word is OGL;
    the link is still owed.
  - The NONNA licence forbids giving the data to a third party, and only allows derivative products with its notice.
  - CHS charts are paid and may not be digitised.
- **Wrong lake, shown as fact:** a chart on a same-named neighbour sends an angler the wrong depths. The reviewed
  frozen file is the guard.
- **Old surveys:** 1950s–1990s charts in feet or metres, NAD27, reservoir shores that have moved. Show the survey year
  and the units on the card, plus "not for navigation".
- **Units:** 110 vector files do not state units. A feet chart drawn as metres is dangerous for boaters and anchors.
- **Size creep:** contour tiles must stay a separate, optional pack.
- **Upstream drift:** the open table says "underDevelopment". A file name can change, and content naming plus a weekly
  record-date check (T412 slow-feed pattern) catches that.

## 8. Questions only the owner can answer

1. The licence link for the three Access Only records (sheet layer, placed contours, 2001 vector files). Without it,
   nothing derived from them ships to production.
2. NONNA for Harrison and Pitt: is "derivative contours with the CHS notice, raw grid never rehosted" acceptable to
   you?
3. Okanagan, Kootenay and Shuswap: pay for CHS chart licences, trace the provincial scans (Okanagan only; Kootenay has
   no provincial chart), or leave them PDF-only?
4. Write to OBWB (Okanagan near-shore lidar), Hakai (Seton/Anderson grids, CC BY) and UNBC (Quesnel)? Each is an
   email only you can send.
5. Review lanes: may classes A and B be bulk-ticked in the review app after a spot check of, say, 50? Or must every
   row be looked at?
6. Show modelled depth (GLOBathy / HydroLAKES) at all? My default is no, because it is not a survey.

## Not verified

- Live production behaviour: the site was not opened.
- Horne and Owikeno licences; whether the Columbia Basin Water Hub FIMP sets hold bathymetry; Quesnel data; OBWB lidar
  depth reach and licence; HydroLAKES and GLOBathy licences; OpenSeaMap BC coverage; CHS chart numbers beyond
  snippets.
- NONNA coverage was checked against the NONNA-10 tile index only, at lake centre points and an interior box. The
  NONNA-100 and package indexes were not read.
- Every vector, tile and PDF size marked "estimate". The confidence-class counts reuse `propose.py`'s output; the
  proposed IoU test (item 2) was not run.
