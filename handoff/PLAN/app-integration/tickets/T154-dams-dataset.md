# T154 — A dams layer so "a widening behind a dam reads as a lake" can be decided by data

Status: DONE, REVIEWED — 067d6a64 (fetch entry, dams.py, revise.py --dams) + review 35f8f974 (dams.py runs without
the lost census scratch); result below for the owner. No app change, no committed candidates change. Rebuild: none (the merger fetches the layer: `python data/fetch_data.py --layers
dams`).

Source: T148 (R2 could not fire on the owner's three "dammed area" marks — Quinsam, South Nanaimo, Bonaparte — because no
dam or obstruction dataset is in the repo; the waterfalls layer is OSM falls only). 2026-10-23.

## Work
Fetch the provincial dams layer (BC Data Catalogue, e.g. WRIS dams public view) into data/source with its licence noted;
match dams to FWA lines/polygons; re-run the T148 revise with R2 using it and report how many candidates move and whether
the three owner marks are caught. No app change until the owner reads the result.

## As built
- **The layer.** BC Dams, `WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW` (catalogue bd632217-35f9-4d01-8e57-a6dbc454f236),
  **Open Government Licence – British Columbia**. Fetched 2026-10-23 over the openmaps WFS into scratch
  (`<scratch>/t154/raw/dams_0.json`, one page): **2,490 crest lines** (2,488 LineString, 2 MultiLineString); operation
  Active 2,080, Breached (Failed) 122, Not Constructed 110, Decommissioned 99, Deactivated 34, Removed 23, Application 22;
  function Main 1,934, Conversion 315, Saddle 171, Outlet 38, Spillway 32.
- **Its place:** `data/fetch_data.py` DATASETS `"dams": {"type": "WFS", "source": "WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW"}`
  — the existing paginated WFS fetcher writes it as layer `dams` of `data/source/bc_fisheries_data.gpkg` (EPSG:3005),
  beside `obstacles`/`waterfalls`. Not written into data/source by this lane (the merger fetches).
- **Matching** `handoff/PLAN/sloughs/v2/dams.py` (reads `--dams FILE` or the gpkg `dams` layer): every dam → its FWA line
  (nearest streams edge within 50 m) and polygons (lakes / manmade / wetlands within 50 m) → `dams_fwa.json`; every T148
  candidate polygon → the dams within 100 m with the dam's SIDE on the polygon's own blue line (`side_of`: the dam's
  route measure vs the polygon's measure range; `outlet` = the polygon is the water it holds back, `inlet` = the polygon
  is below it) → `dams_by_wbk.json`. Province-wide: 2,120 dams have an FWA line within 50 m (1,780 crossing it), 1,727 a
  polygon within 50 m (nearest: lakes 1,255, wetlands 377, manmade 95), 227 neither.
- **R2 with dams** (`revise.py --dams <dams_by_wbk.json> --out DIR`; without `--dams` the run is T148's, and `--out` keeps
  the T148 scratch outputs untouched): new evidence = a STANDING dam (Active / Deactivated) within 25 m (FWA 1:20k
  tolerance) at the polygon's OUTLET. The book exception (R2-book) and rule order are unchanged.

## Result (2026-10-23)
- Baseline re-run reproduces the committed candidates_v2 exactly (0 class, 0 rule_class differences).
- **51 candidates carry provincial-dam evidence**; 20 were already R2 (reservoir codes), 1 is NOT_FISHING_WATER (R0
  first). **26 rule classes move, all to LAKE** (LIKELY_STREAM → LAKE 18, UNSURE → LAKE 7, STREAM_BY_BOOK → LAKE 1:
  Cayoosh Creek 329505890, Walden Power dam at its outlet); **21 final classes move** (5 are owner-marked, so their class
  stays the owner's). Classes: STREAM_BY_BOOK 96→95, LIKELY_STREAM 827→814, UNSURE 520→513, LAKE 706→727.
  Examples: Fiftynine Creek 329056124 (Fifty-Nine Creek Res. #1 dam, 0 m), Marion Creek 329094460 (lower dam, 2 m), Anyox
  Creek 329248608 (Anyox dam, Deactivated, 42 m high), Goldstream 328996539 (Japan Gulch dam), Robertson Creek 329016231,
  Peachland / Powers Creek intakes, two Okanagan oxbow pieces (Osoyoos Oxbows dam).
- **Owner marks:** **Quinsam 329070300 — caught** (Quinsam River Diversion Dam, Main, Active, 15 m, 0 m, at the outlet;
  rules now LAKE via R2). **South Nanaimo 328966554 — caught** (South Fork Dam, Main, Active, 25.6 m, 8 m, outlet).
  **Bonaparte 329055141 — not caught**: no WRIS dam within 7 km (nearest Wilson Creek Res. dam 7.3 km, Bonaparte Lake dam
  10.3 km), and no dam-like FISS obstacle within 7.6 km either; its note is "probably a lake", not "dammed".
  All three were marked UNSURE, so their final class stays UNSURE.
- **The cost:** R2-with-dams turns **3 owner STREAM marks** to LAKE by the rules (the class stays the owner's):
  Nicola River 329316355 (Nicola Lake dam at its outlet, 4 m), Big Bar Creek 328963527 (Ringneck Slough dam, 20 m),
  Fiftynine Creek 329056223 (Fifty-Nine Creek Res. #2 dam, 1 m). Rule agreement with the 97 marks: 90 → 87 (the 3 above;
  Quinsam and South Nanaimo move from LIKELY_STREAM to LAKE, both against "unsure", so neither counts). Without the
  outlet test a 4th stream mark moved (Seton Canal 329506580, the dam at its inlet) and 34 rule classes moved.
- Sensitivity (candidate polygons with a standing dam within r m, any side): 0 m 22 · 10 m 48 · 25 m 61 · 50 m 68 ·
  100 m 74 (any operation code: 24 / 53 / 68 / 75 / 82).
- Scratch: /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t154/ (`compare.txt`, `dams_by_wbk.json`,
  `dams_fwa.json`, `rev_base/`, `rev_dams/` = outlet rule, `rev_dams_any/` = no side test, `raw_fiss/` FISS obstacles).
  **That scratch, the T148 census scratch (`t55v2/`) and the marks (`t55marks/`) were lost in the 2026-10-23 restart**;
  the review re-derived the result below without them.

## Owner questions (default built: nothing promoted)
- Q-T154a: a dam at a widening's outlet makes it LAKE (R2) — but the Nicola below-lake piece, Big Bar and Fiftynine #2 are
  your stream marks with a dam at their outlet. Promote R2-with-dams into candidates_v2 (26 rule moves, the 3 marks stay
  yours), or keep dams as review evidence only on the T149 page? **Default built: evidence only; candidates_v2 unchanged.**
- Q-T154b: Bonaparte 329055141 has no dam in either provincial dataset; mark it from the map (LAKE or stream)?
  **Default: stays UNSURE.**

## Left / follow-ups
- Fetch the layer into the live gpkg (merger: `python data/fetch_data.py --layers dams`); `obstacles` (FISS) is declared
  in fetch_data.py but is not in the live gpkg either.
- If Q-T154a is yes: run `revise.py --dams` into handoff/PLAN/sloughs/v2/ and re-publish the T149 page.

## Adversarial review (2026-10-23)
Re-derived independently (the builder's scratch was gone), nothing written into `<repo>/data/source`.
- **Licence:** BC Data Catalogue `package_show` for bd632217-… answers title "BC Dams", licence "Open Government Licence -
  British Columbia", audience Public. Holds. OGL-BC asks for an attribution line if the layer is ever shown or shipped;
  nothing ships today.
- **The fetch script, run for real:** `fetch_data.fetch_wfs_paginated("dams", …)` (the exact code path `--layers dams`
  takes: sortBy OBJECTID, EPSG:4326 → 3005) into a scratch gpkg: 2,490 features, one page, operation counts exactly as
  above, srs 3005, every column `dams.py` reads present. The gpkg stores them as MultiLineString (pyogrio promotes);
  `dams.load_dams` reads that layer unchanged (2,490, same counts) — the merger's path, not only the `--dams` JSON path.
- **dams.py, re-run** on that fetch against the live gpkg: province-wide 2,120 / 1,780 / 1,727 / 227 and 82 candidate
  polygons within 100 m — every figure above reproduced exactly.
- **Match quality.** Of the 353 dams named for a river/creek with a named FWA line within 50 m, 324 (92 %) share the
  name; the rest are naming quirks or tributary dams (Borland Creek 12–21 dams on Bruno/Bruning Creek, "EVEGREEN" for
  Evergreen Creek, McMurdo Creek dam on Brisco Creek). Spot check of 10 Active Main dams ≥ 15 m (seeded sample):
  Cleveland → Capilano River 0 m (Capilano Lake 3.7 m), John Hart → Campbell River 0 m (John Hart Lake 1.3 m), South
  Slocan → Kootenay River 0 m, Corbin Creek → Corbin Creek 0 m, Long Lake → Cascade River 0 m, Graystoke → Loch Katrine
  Creek 0 m, Kenney → the Nechako's unnamed lake line 0 m (Knewstubb Lake 20.7 m), Lake #7 → unnamed line 0 m, a
  d-48-H reservoir 13 m, Arrow Lakes Generating Dam no line within 50 m (Lower Arrow Lake 22.9 m — the generating
  station beside Keenleyside, right). All plausible.
- **`side_of` rests on FWA line direction.** Measured: 182 of 182 sampled stream edges end where the next-upstream
  edge (same blue line, DRM = this URM) starts — FWA lines run downstream → upstream, so "measure near the polygon's
  lowest = outlet" is right. Its tie-break can pick a short 1450 construction line (Quinsam: the 24 m side line, not the
  417 m main flow); over all 72 standing-dam/candidate pairs within 25 m, the side on the longest inside line differs
  in ONE (a Highland Valley tailings saddle dam, R0 not-fishing-water first) — no class depends on it.
- **R2 re-run result (simulated from the committed candidates_v2 + the re-made dams_by_wbk.json, R2's own order: R0/R1
  first, existing dam evidence and the book exception unchanged):** 51 candidates carry standing-dam outlet evidence =
  20 already had dam/reservoir evidence + 1 R0 + **4 already LAKE by another rule** (the builder's count left these 4
  out) + **26 rule classes move, all to LAKE (LIKELY_STREAM 18, UNSURE 7, STREAM_BY_BOOK 1)**, 21 final classes (5
  owner-marked). Without the outlet test: 34 rule moves. **Owner marks: Quinsam 329070300 caught** (Quinsam River
  Diversion Dam, Active, 0 m, outlet — on the main flow line too: at 47,368.7 vs lo 47,368), **South Nanaimo 328966554
  caught** (South Fork Dam, Active, 7.6 m, outlet), **Bonaparte 329055141 not caught** (no dam within 100 m). The 3 owner
  stream marks turned by the rule (Nicola below the lake, Big Bar Creek, Fiftynine #2) reproduced.
- Not re-run: `revise.py --dams` itself (needs the census + marks scratch); the simulation reads the same inputs it
  writes into candidates_v2, so moves are exact for R2 but rules after R2 are not re-evaluated (none can override an
  R2 LAKE — R2 returns).
- **Fix (35f8f974):** `dams.py --polys` was required and pointed at the lost census scratch; omitted, it now reads the
  pieces from the gpkg by WATERBODY_KEY. Output byte-identical to the explicit-parquet run (`cmp` both files).
- **Left for the owner / merger:** `revise.py` still hard-codes `t55v2/` + `t55marks/` scratch inputs — re-running it
  (Q-T154a yes) needs `census.py` (heavy) and the owner's marks re-exported from the T149 page first. Q-T154a/b stand.
