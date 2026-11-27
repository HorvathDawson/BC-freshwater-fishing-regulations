# Lake depth charts: what is built, what the research found

Depth charts are not in the app (open tickets T201, T218, T219, T634). The code that picks the lake for a chart is
written and kept, not fed to any build stage (`pipeline/bathymetry/README.md`). This page holds the sources and the
decisions that remain.

## What is built

- `pipeline/bathymetry/propose.py` maps a chart's waterbody id to its lake and writes proposals under `data/generated/`.
  The lake code picks the lake (FWA `WATERBODY_KEY_GROUP_CODE_50K`); name, position and size only confirm it. It proposes
  1,995 of 2,294 chart codes as clean (87 %) and leaves 299 codes plus 44 code-less sheets for the owner. Do not write a
  second matcher.
- `accept.py` is the only writer of the frozen `data/curated/bathymetry/matches.json` (owner only; nothing is accepted yet).
- `fetch_vectors.py`, `e00.py`, `place.py`, `place_more.py`, `contours.py`, `compress.py` read the 2001 vector zips,
  fit them to our lake outlines and grade each placing, and shrink chart PDFs to 200 dpi 1-bit (outside git).
  `pipeline/tests/test_bathymetry_t634.py` pins them on synthetic inputs.
- The app already has the pieces waiting: the bundle's `chart` table (`pipeline/deliver/bundle/schema.sql`, required by
  `app/tools/bundle-schema.test.ts`), per-section survey state in `lakes/1` (`pipeline/deliver/lake_facts.py`) and the
  Depth colouring, greyed out.
- To feed it: the owner's accepts, `CURATED.bathymetry.matches` (declared, the file absent until then), a bundle writer for `chart`, lake values
  in `lakes/1`.

## Sources

| Source | What it is | Licence |
|---|---|---|
| BC "Bathymetric Maps - Open" (catalogue `1427d389-cd21-4fe2-8ed9-282d9bdcb7e2`) | 2,740 sheets for 2,216 lakes; raster scans (1-bit, about 8,800 x 6,500 px) with an OCR text layer, not georeferenced, no vector contours; mostly 1950s to 1990s, many on NAD27; CSV table with the WSA waterbody id and the PDF URL (`a100.gov.bc.ca/pub/fidq/downloadBathymetricMap.do`) | OGL-BC |
| `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW` | FWA lake polygons carrying the PDF URLs: the clean join to a waterbody key | access-only per the catalogue |
| `WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP` (BC "Bathymetric Maps of Surveyed Lakes") | 26 lakes, 137 contour polygons (`CONTOUR_DEPTH_M`, EPSG:3005) plus max, mean depth and volume; the Province georeferenced scans to the FWA outline, so the method works. Owner: in the Data Catalogue, no licence email needed | OGL-BC (owner), catalogue says access-only |
| 2001 vector depth zips | 510 old vector files; 298 fit their lake strictly; 110 do not state units (feet drawn as metres is dangerous) | access-only; the owner owes the licence link |
| CHS NONNA | only Harrison, Pitt, lower Fraser among interior waters; the licence forbids passing the data to a third party | restricted |
| Hakai / CIOOS | multibeam grids of Seton, Anderson, Owikeno, Horne | CC BY |
| OBWB | near-shore lidar of the Okanagan lakes | unknown |
| GoFishBC "Depth Map" links | the same ministry scans; no bulk feed | all rights reserved |
| Navionics, C-MAP, Garmin, Lakemaster, Fishidy, Backroad Mapbooks | proprietary or user-contributed | do not scrape or copy |

No open full-depth vector dataset exists for Okanagan Lake: its only full-lake depths are the Province's 6-sheet scan.
FISS_LAKE_SURVEYS_SVW lists which lakes were surveyed, with no extra geometry. Modelled depth (GLOBathy, HydroLAKES) is
not a survey; the default is to not show it.

## Measured on the scans

- The PDFs have a readable statistics block (max, mean, volume match the digitised layer for Fox Lake).
- No graticule. A similarity plus ICP fit of the chart shoreline to the FWA polygon worked on 4 of 5 single-lake sheets
  (median 5 to 13 m, scale within 5 % of print); it failed on a changed reservoir shore and on both multi-sheet cases.
- Traced stroke geometry agrees with the government's digitised contour to about 5 m. Depth assignment by nesting
  failed (label gaps, soundings on lines), so labelling needs a review page.
- Estimate: 2 to 2.5 weeks to build the tracing, then about 15 min of QA per lake (top 100 is about 25 h).

## Display options

| Option | Effort | Note |
|---|---|---|
| Link out to the government PDF | S | breaks the self-host rule; URLs have moved; not recommended |
| **Rehost the 200 dpi PDFs** and a "Depth chart" card in the Lake tab | M | 2,599 files, 201 MB on R2 under content names (`bathymetry/<sha256-12>.pdf`), fetched on tap, kept after first open; the recommended first step |
| Georeferenced scan on the map | L | a fit per sheet; the owner deferred "scan over the map" |
| Vector contours in tiles | L for the about 400 placed lakes, XL to trace scans | a separate optional PMTiles file (`depth/1`, layer `depth_contour`), 10 to 30 MB estimated; blocked on the access-only answer |
| Depth colouring only (`lakes/1`) | S | already built; switches on with the card |

First step: owner review of the proposals (confident classes in bulk after a sample, the 343 leftovers one by one), the
PDFs to staging R2, then the card. A chart row whose PDF fails to load shows the failure; an unknown `chart_format` is
refused; a lake with no chart shows nothing and never says none exists. Show the survey year, the units and "not for
navigation". Vector contours start with the 25 clean contour lakes and the strict placings (Skaha, Cowichan,
Alouette, Lac la Hache, Moberly).

## Open owner questions

1. The licence link for the three access-only records; nothing derived from them ships without it.
2. NONNA for Harrison and Pitt: derivative contours with the CHS notice, raw grid never rehosted?
3. Okanagan, Kootenay, Shuswap: pay for CHS licences, trace the provincial scans (Okanagan only), or PDF only?
4. Emails to OBWB, Hakai, UNBC and FFSBC (about the roughly 800 stocked lakes' depth links).
5. May the confident classes be bulk-ticked after a 50-row spot check?
