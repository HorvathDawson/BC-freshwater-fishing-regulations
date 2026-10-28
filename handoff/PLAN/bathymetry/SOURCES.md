# T201 — BC lake bathymetry: digital sources (research, 2026-10-27)

Verified = fetched/queried today. Unverified = from search snippets or general knowledge; check before relying.

## Ranked recommendation

1. **BC "Bathymetric Maps - Open" scanned charts (legal to ship; broadest coverage; needs our own georef + vectorise).**
   2,740 CSV rows / 2,216 distinct lakes (verified). Each row has the lake id, gazetted name, WSA waterbody id, draft date, datum
   (mostly NAD27) and a PDF URL. Licence is OGL-BC, so redistribution (including derivatives) is allowed with attribution.
2. **BC "Bathymetric Maps of Surveyed Lakes" (already-digitised contour polygons).** Verified: only **26 lakes / 137 polygons**
   (Fraser, Pinchi, Wahleach, Cunningham, Charlie, Gwillim, Maxhamish, Tezzeron...), mostly Omineca/Peace. Useful as ground truth
   for checking our own vectorisation, and as the first lakes shipped. Catalogue licence says "Access Only" (public view and download),
   with no explicit OGL line. Confirm with FISH.Issues@gov.bc.ca before bundling. It is a government dataset, so OGL-BC is likely.
3. **Per-lake depth stats in the same layer** (max depth, mean depth, volume, survey date and agency). Cheap, and enough to feed the
   existing per-lake Depth colouring slot before any contours exist. Also in the scanned-map metadata headers.
4. **Licence-required or unclear: Navionics, C-MAP, Garmin, Fishidy, Lakemaster, Backroad Mapbooks.** Do not scrape or ship.
5. **Not useful for inland BC lakes: NONNA / CHS, OSM.**

## Sources

### 1. BC Data Catalogue — Bathymetric Maps - Open (verified)
- Coverage: ~2,216 lakes, 2,740 sheets (some lakes are multi-sheet). Mostly FISS / ministry surveys, 1950s-1990s.
- Format: scanned/hand-drafted PDF per sheet (vector-ish PDFs of ~120 KB; not georeferenced). Reference table is CSV.
- Quality: depth contours in feet or metres per chart, with a scale bar and shoreline. Datum NAD27 on many.
- Licence: OGL-BC (catalogue page `1427d389-cd21-4fe2-8ed9-282d9bdcb7e2`, last modified 2026-01-28, status "Under Development").
- Cost / access: free, no key. CSV: https://catalogue.data.gov.bc.ca/dataset/1427d389-cd21-4fe2-8ed9-282d9bdcb7e2/resource/d1d89c2e-1994-4f7d-a269-55b40e26067d/download/bathyopenreferencetableandmapsaug22_2023final.csv
  Sample PDF: https://a100.gov.bc.ca/pub/fidq/downloadBathymetricMap.do?filename=00045501.pdf (103 Mile Lake, HTTP 200)
- Join key: LAKE_ID_WSA_50K / WATERBODY_IDENTIFIER_WSA_50K (old WSA id); the spatial layer below links these to FWA polygons.

### 2. BC "Bathymetric Maps" spatial index (verified, catalogue)
- WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW: FWA lake polygons carrying the PDF URLs. WMS/KML/WFS via openmaps.gov.bc.ca. This is the
  clean way to attach each PDF to an FWA waterbody key (our registry's join).

### 3. BC "Bathymetric Maps of Surveyed Lakes" (verified)
- WFS: https://openmaps.gov.bc.ca/geo/pub/WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=pub:WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP&outputFormat=application/json
- Format: MultiPolygon "slices" (CONTOUR_DEPTH_M, 0 = surface), EPSG:3005 coordinates. Properties include survey date and agency, max and
  mean depth, volume, FWA waterbody key, and a CREATION_PROCESS note: the BC government georeferenced PDF scans to the FWA lake
  polygon and digitised/hand-added contours. That proves the georef + vectorise approach works and is the model for our own.
- Only 26 lakes. A related "Bathymetric Lakes for the Cariboo Region" (Oracle SDE, access-only) exists; not downloadable here.

### 4. Freshwater Fisheries Society of BC (GoFishBC) (unverified details)
- "Depth Map" link on Where to Fish for many stocked lakes; these are the same ministry scans or FFSBC-produced PDFs. No bulk feed or
  API found. Licence not stated; treat as © FFSBC. Ask FFSBC for a data-sharing agreement (best route to the ~800 stocked lakes).

### 5. Habitat Wizard / FISS / FIDQ
- FIDQ (https://a100.gov.bc.ca/pub/fidq/viewBathymetricMaps.do) is the search UI over source 1. FISS_LAKE_SURVEYS_SVW lists which lakes
  were surveyed (dates). No extra depth geometry.

### 6. CHS NONNA, NRCan (unverified)
- NONNA (OGL-Canada plus NONNA licence, non-navigational use, 10 m / 100 m grids, WMS/WCS) targets coastal waters and large navigable
  waters. I found no evidence of inland BC lake coverage. NRCan Atlas of Canada "7.5M Bathymetric" is a coarse base map. Neither helps.

### 7. Regional / academic (unverified)
- Okanagan Basin Water Board and the Okanagan large lakes have bathymetry in research reports and models (e.g. Okanagan, Kalamalka,
  Shuswap, Kootenay, Arrow). Worth a targeted request for the ~10 largest lakes (CHS charts, universities, regional districts); not
  an open feed. Nothing bulk found.

### 8. Crowd-sourced sonar (licence-required)
- Navionics SonarChart (Garmin): web API needs a developer key; "Standard" tier must be free to users, "Enhanced" is paid. Contours are
  rendered inside their viewer; extraction is not permitted. Best coverage of popular lakes, so the quality benchmark.
- C-MAP Genesis, Lakemaster, Fishidy, Garmin Quickdraw Community: proprietary or user-contributed under platform terms; no public API, no
  redistribution right found. Quickdraw maps can only be shared inside Garmin devices.
- OpenStreetMap: no standard lake depth-contour tagging; effectively no BC coverage (unverified).

### 9. Backroad Mapbooks (licence-required)
- Sell "Bathymetric (Depth) Charts" in BC fishing mapbooks. They do not state the source. Almost certainly the same ministry scans
  traced by hand plus their own work. The ministry originals are the open version of that data, so we need not license Backroad.
  Their redrawn contours are copyrighted: do not copy.

## Georeference + vectorise effort (source 1)
- Precedent: BC's own 26-lake layer did this. Method: render PDF, georeference to the FWA lake outline (shoreline on chart matched to FWA
  polygon, using NAD27 vs NAD83 shift and scale bar), trace contours, assign depths.
- Automation: PDF -> raster -> auto-fit the shoreline polygon to the FWA outline (ICP / affine) -> colour/line contour extraction ->
  OCR depth labels (or per-chart contour interval) -> polygonise. Plausibly 60-70% of simple lakes fully automatic; the rest need
  manual QA. Estimate 2-3 weeks to a pipeline plus review UI; then ~5-10 min of QA per lake (hundreds of hours for 2,200; so tier it).
- Cheaper phase: ship the ministry scan's max/mean depth per lake and a link to the PDF (no georef), then contours for the top N
  fished lakes (stocked / most-searched), validating the method against the 26 digitised lakes.
- Check: chart quality varies (hand-drafted, some multi-sheet, some no shoreline match); confirm a sample of 20 PDFs before committing.

## Suggested T201 path
1. Feed the Depth toggle from per-lake max/mean depth (source 3 plus metadata read off the PDFs); no tile change.
2. Ship the 26 digitised lakes' contours (confirm OGL with the ministry), size diff against the T64/T85 budget.
3. Prototype automated georef on 20 sample PDFs; decide the tier size.
4. Email FFSBC about their stocked-lake data; do not use any proprietary sonar source.
