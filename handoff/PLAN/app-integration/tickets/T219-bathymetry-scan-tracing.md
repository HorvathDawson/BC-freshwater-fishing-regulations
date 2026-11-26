# T219 — bathymetry: trace contours from the open scanned charts for the top-fished lakes

Status: POSTPONED (deferred, owner 2026-11-05; T437 aligned with the ledger) — was: OPEN. Not to be implemented yet (owner 2026-10-27).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T219 | bathymetry: trace contours from the open scanned charts for the top-fished lakes | POSTPONED (deferred, owner 2026-11-05) | — | not yet (owner 2026-10-27); raster scans, georef works, depth labelling needs a review UI; ★ tier size |

Source: owner 2026-10-27 on T201's research (handoff/PLAN/bathymetry/SOURCES.md, source 1). The input is the OGL-BC
"Bathymetric Maps - Open" set: 2,741 CSV rows, 2,246 WSA waterbodies (2,216 by name). T218 ships the 28 lakes that
are already digitised. This ticket covers the rest, tiered by how much each lake is fished.

## Feasibility (measured 2026-10-27, prototype in the session scratchpad; no repo code)
Samples: Fox L. 1988, 103 Mile L. 1975, Cottonwood L. 1950, Roche L. group 1959, Tunkwa L. 1981, Pinchi L. 1 of 2,
Lac la Hache 1 of 2.
- **The PDFs are raster, not vector:** one 1-bit JBIG2 scan per page (CCITT/Flate on the oldest), about 8,100–9,500 ×
  5,900–6,800 px at 270–380 dpi; no contour can be pulled out as a path. Each page has an OCR text layer (1.4–2.6 k
  chars) holding the statistics block (Fox: max 24.7 m, mean 10.5 m, volume 12,000,000 m³, matching the digitised layer
  exactly); garbled cases still need a check (feet, odd glyphs such as "10·5", "2 5 .6"). Scan quality is good, pre-1975
  hand-drafted sheets too.
- Labels: inline depth labels in gaps (3, 6, 9 …), spot soundings (".12"); feet on pre-~1975 sheets, metres after;
  intervals vary (3 m, 10 ft, 20 ft…); some sheets are mostly transects and soundings (Tunkwa).
- No graticule or lat/long ticks; a north arrow (often rotated: Fox −60°, Cottonwood +34°), a scale bar, a printed
  scale; newer title blocks have one UTM point and an NTS sheet. So the shoreline is the georeference.
- **Georef prototype:** render at 100 dpi, threshold, flood the outside, open to cut creeks, take the blob; fit a
  similarity transform to the FWA polygon (15° rotation sweep + trimmed ICP). On the single-lake sheets 4 of 5 fit
  automatically (median error 5–13 m, scale within 0–5% of print); Tunkwa 1981 failed (reservoir shore changed; creeks
  in the blob); the 2 partial sheets (Pinchi, Lac la Hache) failed: they need a mosaic first, or the scale from the
  printed ratio and only the matching shore fitted. In the full set, 363 rows (158 lakes) are multi-sheet.
- **Contour prototype (Fox):** long ink strokes inside the lake, skeletonise, bridge gaps, label bands by nesting. Stroke
  geometry is good: the digitised 10 m contour lies a median 4.5 m (p90 13 m) from our nearest stroke. Automatic depth
  assignment failed (2 nesting levels of the chart's 8; bridging at 40–120 px did not fix it), so the depth label needs a
  person, or a vision model pass, plus snapping.

## Failure modes
1. Partial or multi-sheet lakes: 158 lakes (mosaic or partial-shore fit).
2. Shoreline changed since the survey (reservoirs, dams, a 1950s air-photo "interim" outline): Tunkwa, Roche NE.
3. The wrong blob: creeks, islands or a title box merged in; several lakes on one sheet (118 PDFs serve more than one
   lake row).
4. Charts with few contours (transects and soundings only).
5. Units and interval: feet vs metres, irregular intervals, shoals.
6. Several chart versions per lake (216 lakes; Stump L. has 4): pick the newest, or the best shore fit.
7. Metadata gaps: 90 rows no datum, 326 no draft date; NAD27 does not matter (the fit is to FWA).

## Work (stages, estimate)
1. Index: CSV ↔ `WHSE_FISH.BATH_SURVEY_MAP_SHEETS_SVW` ↔ FWA key; one chart per lake; rank lakes by fishing
   pressure (stocked list from T200, regs mentions). **0.5–1 d.**
2. Georef: harden the prototype (pick the blob by shape, trim creeks, QA flags on p90 and scale drift vs the printed
   ratio). **1–2 d.** About 80% of single-sheet charts automatic. Multi-sheet: a mosaic or partial fit (**+2–3 d**),
   or a manual 3-point fit at about 5 min per sheet.
3. Strokes: vectorise the contour strokes and close gaps by snapping. **1–2 d.** Automatic.
4. Depth labels + review UI: show the georeferenced strokes over the scan; a person clicks a stroke and types its
   depth, and fixes gaps and shoals. The OCR text layer and a nesting guess pre-fill it. **3–4 d.** An optional
   vision-model pre-label pass spends credits: owner's call.
5. Polygonise, clip to FWA, simplify, then tiles + T218's drawing. **1–2 d.**

Total: about 2–2.5 weeks of build. QA: about 10–20 min per simple lake and 30+ min for multi-sheet or changed-shore
lakes, so an average of about 15 min. The top 100 lakes take about 25 h, the top 300 about 75 h, and all 2,200 about
550 h, which is not worth doing; tier it.

## Acceptance
For the 28 T218 lakes, run on their open PDFs as a blind test: median shoreline error ≤ 15 m, and the traced contour
areas within 10% of the digitised slices at matching depths. Then the top-N tier is shipped with per-lake QA sign-off
recorded, plus the tile size delta.

Depends: T218 (contour drawing, ground truth), T200 (stocked list for ranking). Size: L. Owner: yes (the tier size N;
whether to use a vision model for pre-labelling). Default: top 100 lakes, human labelling only.
