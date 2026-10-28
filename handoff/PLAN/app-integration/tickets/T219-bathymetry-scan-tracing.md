# T219 — bathymetry: trace contours from the open scanned charts for the top-fished lakes

Status: OPEN. Not to be implemented yet (owner 2026-10-27).

Source: owner 2026-10-27 on T201's research (handoff/PLAN/bathymetry/SOURCES.md, source 1). The input is the OGL-BC
"Bathymetric Maps - Open" set: 2,741 CSV rows, 2,246 WSA waterbodies (2,216 by name). T218 ships the 28 lakes that
are already digitised. This ticket covers the rest, tiered by how much each lake is fished.

## Feasibility (measured 2026-10-27, prototype in the session scratchpad; no repo code)
Samples: Fox L. 1988 (metres, small, also ground truth), 103 Mile L. 1975, Cottonwood L. 1950 (oldest, feet), Roche
L. group 1959 (feet, three lakes on one sheet), Tunkwa L. 1981 (feet), and two multi-sheet sets: Pinchi L. 1 of 2
(ground truth) and Lac la Hache 1 of 2 (large).

**The PDFs are raster, not vector.** Each page is one 1-bit JBIG2 scan (CCITT/Flate on the oldest), about 8,100–9,500 ×
5,900–6,800 px at 270–380 dpi, from Acrobat 4 "Import". The only vector paths are the frame and the disclaimer box,
3–28 per page. No contour can be pulled out as a path. Each page does have an OCR text layer, 1.4–2.6 k chars.
- Scan quality is good: clean black-on-white, thin continuous pen lines, little noise. The hand-drafted pre-1975
  sheets read as well as the 1988 one.
- Labels: contours carry inline depth labels in gaps (3, 6, 9 …) and pass through spot soundings (".12"). Units are
  feet on pre-~1975 sheets and metres after. Intervals vary (3 m, 10 ft, 20 ft…). Some sheets are mostly
  transects and soundings with almost no contours (Tunkwa).
- Georeferencing aids: none of the samples has a graticule or lat/long ticks. Each has a north arrow (often rotated:
  Fox −60°, Cottonwood +34°), a scale bar and a printed scale. Newer title blocks have one UTM point and an NTS
  sheet. So the shoreline is the georeference.
- The OCR text layer holds the statistics block. Fox reads max 24.7 m, mean 10.5 m and volume 12,000,000 m³, which
  matches the digitised layer exactly. Garbled cases still need a check (feet, odd glyphs such as "10·5", "2 5 .6").

**Georeferencing prototype.** Render at 100 dpi, threshold, flood the outside, open the result to cut off creeks, and
take the candidate blob. Then fit a similarity transform (scale, rotation, shift) to the FWA polygon: a 15° rotation
sweep followed by trimmed ICP. Fit errors are measured both ways between the boundaries:

| sheet | median m | p90 m | implied vs printed scale | result |
|---|---|---|---|---|
| Fox 1988 | 6.1 / 5.4 | 35 / 14 | 1:3,515 vs 1:3,500 | good (p90 is the inlet creek) |
| 103 Mile 1974/75 | 5.2 / 5.2 | 20 / 22 | 1:3,619 | good |
| Cottonwood 1950 | 4.9 / 5.5 | 24 / 48 | 1:3,404 vs 1:3,588 | good |
| Roche group 1959 | 11 / 13 | 92 / 187 | 1:7,233 vs 1:7,200 | good fit; the 1959 shore differs from FWA at the NE end |
| Tunkwa 1981 | 34 / 29 | 227 / 79 | 1:11,844 vs 1:18,264 | **failed** (reservoir shore changed; creeks in the blob) |
| Pinchi 1 of 2 | 264 / 336 | — | 1:188,646 | **failed** (a partial sheet against the whole lake) |
| Lac la Hache 1 of 2 | 168 / 291 | — | 1:55,356 | **failed** (same) |

On the single-lake sheets, 4 of 5 fit automatically, with a median error of 5–13 m and the scale within 0–5% of
print. The 2 partial sheets both failed: they need the sheets mosaicked first, or the scale taken from the printed
ratio and only the matching part of the shore fitted. In the full set, 363 rows (158 lakes) are multi-sheet.

**Contour prototype (Fox).** Keep ink inside the lake that forms long strokes, which drops digits, soundings and
transect dots. Skeletonise, bridge endpoint gaps, label the bands between strokes, and give each band a depth by its
nesting distance from shore.
- Stroke geometry is good. The digitised layer's 10 m contour lies a median 4.5 m (p90 13 m) from our nearest
  georeferenced stroke, so georef + strokes agree with the government's own trace to about 5 m.
- Automatic depth assignment failed: it reached 2 nesting levels, but the chart has 8. Gaps at the inline labels,
  lines run through soundings, and contours that hug the shore leak bands into each other. Endpoint bridging at 40–120
  px did not fix it.
- So the depth label (which stroke is which depth) needs a person, or a vision model pass, plus snapping to close
  the gaps.

## Failure modes
1. Partial or multi-sheet lakes: 158 lakes. They need a mosaic or a partial-shore fit.
2. The shoreline changed since the survey (reservoirs, dams, a 1950s air-photo "interim" outline): Tunkwa, Roche NE.
3. The wrong blob: creeks, islands or a title box merged in. Several lakes on one sheet (Roche group; 118 PDFs serve
   more than one lake row).
4. Charts with few contours (transects and soundings only), so there is nothing to trace.
5. Units and interval: feet vs metres, irregular intervals, shoals (closed contours that get shallower).
6. Several chart versions per lake (216 lakes; Stump L. has 4). Pick the newest, or the one whose shore fits best.
7. Gaps in the metadata: 90 rows have no datum and 326 have no draft date. NAD27 does not matter, because the fit is
   to FWA and not to coordinates.

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
