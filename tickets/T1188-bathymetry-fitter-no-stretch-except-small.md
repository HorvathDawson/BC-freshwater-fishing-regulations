# T1188 — Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial

Status: OPEN — the fitter merged in #386 (faffa736c, 2026-11-28); the vector depth maps are now wanted in 0.10.0 (T201, owner 2026-11-28); owner: see ## Owner (his look at each placing)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1188 | Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial | OPEN — fitter merged (#386); depth maps wanted in 0.10.0 (T201); owner: see ## Owner | faffa736c (#386); v5 shore joins + depth lines (this PR) | his look on the review page; the review page moved to the v5 placings; item 6 (T201's order) |

Source: the owner on the bathymetry sample page, 2026-11-26: "It seems like a lot of the poor ones could be reoriented to match better? Not all but
definitely some. Then others like elk lake one need to be reoriented then it also matches 2 lakes. This could be considered for actually implementing
them?" And on the refit trial: "no stretch except small ones and for the bigger stretch items maybe flag the items it would have helped and give a how it
would change please." The study: `bathymetry-pdf-to-vector`; the trial's result is in the planning chat's day notes. T201 (the order, the licence) is the
parent; this is the fitter work ticket, so T201 is extended by a pointer, not duplicated. Also: depth does NOT ship today; `lakes/1` has no producer.

## What the trial measured (read-only)
Baseline 298 strict / 117 loose / 83 poor reproduces exactly. Without scale freedom 22 sheets move (7 poor to strict, 12 poor to loose, 3 loose to
strict): 308 / 126 / 64. With plus or minus 15% uniform scale: 396 / 66 / 36, but scale is a stretch the fitter refuses on purpose (103 scale wins; 6 at the
limit). The union of neighbouring lakes recovers Beaver & Elk (IoU 0.55 to 0.90), Thetis, Showh, Spectacle-Swan, Fowl Creek 2, Jane. Stake Lake's code
points 40 km away (fits strict by name).

## Work
1. **Rigid search wider:** every rotation from several starts, the mirror image; no scale freedom.
2. **Two-lake fit:** a sheet of two lakes fits the union of the neighbouring lake outlines, then splits contours per lake.
3. **Wrong-lake codes:** search nearby lakes by shape (Stake).
4. **Small stretch only:** at most 5%, its own flagged class. **Bigger stretches are NOT adopted;** each sheet one would help is listed with
   what the stretch would change (before / after), for his eye.
5. **Reorientation trial** on the poor and loose placings (the 83 + 117), results as a list; his eye-check per placing stays.
6. Then the order of T201: the chart card, the 28-lake layer, the ~300 tight-fit vector lakes, a 3-lake pilot. No bulk tracing; images, not PDFs, ship.

## Built 2026-11-27 (items 1-5; merged in #386, 2026-11-28)
`pipeline/bathymetry/refit.py` (the steps) and `refit_all.py` (the run; `GENERATED.bathymetry/refit.json`, `.csv`,
`_shapes.json`); `place_more._one` now takes the zip's path. A step is adopted only when it lifts the class and is not
ambiguous (another turn, the mirror or another lake within 0.03 IoU). Tests: `pipeline/tests/test_bathymetry_t1188.py`
(built shapes, no data). Measured on all 512 sheets (510 zips: two zips hold a second, duplicate coverage) (6 min, 4
workers, lane 3), baseline reproduced exactly:

| after | strict | strict, stretch <= 5 % | loose | loose, stretch <= 5 % | poor |
|---|---|---|---|---|---|
| the repo's fitter | 298 | — | 117 | — | 83 |
| 1. rigid wider (every turn from 3 starts, the mirror; scale 1) | 301 | — | 121 | — | 76 |
| 2. + two-lake union, contours split per lake | 305 | — | 125 | — | 68 |
| 3. + wrong-lake code by shape (50 km; a different name refuses) | 309 | — | 126 | — | 63 |
| 4. + small stretch, its own flagged class | 309 | 73 | 53 | 14 | 49 |

Lists: 108 changed class; 8 two-lake (Beaver & Elk, Thetis, Showh, Spectacle-Swan, Moffat, North Cameron, Wright,
Parrott); 5 wrong-lake codes (Stake by its title, South Gataga 3 by its group's name, Fowl Creek 1 and 2, Jane: unnamed
neighbours within 3 km); 87 small stretches (33 at the 5 % limit: flagged `small_stretch_at_limit`); 20 where a bigger
stretch would help (NOT adopted; each with what it would change); 4 lifts refused (3 ambiguous, 1 a different name).
No mirror was adopted. Pictures and the CSV: planning night notes `T1188/`. Not done here: item 6.

**Review fixes and the trust checks (2026-11-27, same branch; two re-reviews, final rerun 14 min, lane 3):** the
stretch keeps the ambiguity rule (both mirrors; the best fit on every other target an earlier step searched is a rival,
weighed against the placing before its stretch); a found lake (a wrong-lake find or a union neighbour) must be free (on
a lake another sheet ends on: refused and rerun; on a lake only coded for a sheet that moved off: kept, flagged
`takes_a_coded_lake`: Fowl Creek 1); an empty zips folder raises. Two sheets on one lake are
legitimate only when it is coded for both (today: Carp and Mahood, two surveys each). Then EVERY placed sheet: the best
lake within 50 km (screened plain and mirrored) and its gap from the unstretched placing (`neighbour_rival` within
0.03; never moves a placing), a strict baseline's turn / mirror check, the shore's complexity (1 - its best raster IoU
on itself turned > 20 deg or mirrored, over unturned; min 0.04, median 0.29, max 0.68) and `confidence`: `high` 284 /
`high_stretched` 49 / `review` 179 (14 unplaced). `high` = the repo's own strict placing on its coded lake, unstretched,
nothing flagged; a two-lake / wrong-lake find, or a placing the wider rigid search moved (`rigid_wider`), is `review`.
A shore below 0.10 (18 of 498) is `review` unless its lake is the only one its size in the radius (13 strict moved).

| after | strict | strict, stretch <= 5 % | loose | loose, stretch <= 5 % | poor |
|---|---|---|---|---|---|
| 4. with the review fixes | 309 | 73 | 53 | 5 | 58 |

Nine stretches refused as ambiguous (loose stretched to poor): Abruzzi (mirror), Dunlevy, McKay, Myles, Blenkinsop,
Gataiga, Idleback, Azuklotz, Hoover (another lake fits as well unstretched).
Pictures: night notes `T1188/v2/` (the review set by reason, then `high_stretched`).

## 2026-11-27/28: the review page and his words (verbatim)
v2 counts: `high` 284 / `high_stretched` 49 / `review` 179. The review page is a private claude.ai Artifact ("Depth chart review", 196 charts:
4 names differ, 15 partly, 177 review; the marks are his, read with ArtifactData; link in the planning chat's notes); 23 marks so far.
- The page: "Options A also the names that differ for definite should have those names added to the name variants for those lakes" (2026-11-27);
  when: "After I mark each one Right (Recommended)" (2026-11-27); he then marked the four Right (Grizzly, Snow, Walkin, Kitwanga; branch
  `fix/T1188-chart-names-as-variants`, merged in #388). "the artifact needs bigger images so i can tell" (2026-11-27).
- Confidence: "the complexity of the shape matetrs too" (2026-11-27). Abruzzi: "okay i went through a bunch and for exampel (ABRUZZI LAKE) i
  think would match better if you looked at the first ring in the vector too? maybe that is why it looked weird? it has a big shallow area for
  example. look at it and see if it changes anything" and "at least would help align it" (2026-11-27). Reading: the first-ring trial
  (night notes `T1188/v4-first-ring/`) found a stream-coded shore closed by an 830 m straight join; follow-up branch `feat/T1188-shore-and-depth-lines`.
- The page: "draw depth lines in artifact too. and also i could have a way to scale and rotate etc. also one last thing have a way to show
  satellite of the area please so i can reference other things in the pdf" (2026-11-28); "also the map matching should allow me to translate the
  lakes too. as well as scale and rotate (while aving the lines)" (2026-11-28). Built in page v3 except satellite (Esri forbids exported tiles):
  links open Google / Bing aerial at the lake instead.

## Built 2026-11-28 (v5: the shore's joins, depth lines; branch feat/T1188-shore-and-depth-lines)
The owner on Abruzzi: "i think would match better if you looked at the first ring in the vector too? ... it has a big
shallow area". Cause: its north-west shore is coded GA2485 (stream), so the fitter closed the shore with an 830 m chord
across the shallows.
1. **Shore joins** (`place.shore_report`): a join over 300 m (`JOIN_TRUSTED_M` = the first pass's gap: the 769 joins
   run continuously 0-296 m, then only the 29 joins of the 26 sheets that close at the 1,000 m pass) is first
   completed with the sheet's own G-coded arcs between its two ends, kept only when the ring closes and holds the
   depth lines (<= 1 % outside). Of the 26 placed: 6 completed with real arcs (Abruzzi 26.2 ha, the PDF prints 26.0;
   Bruce, Beale, Fox, Grayling, Blackwater), 20 keep a straight join and are flagged `shore_joined_straight` (Boy
   Lake: its west shore is a river's two banks that never meet; French: a road); 3 unplaced sheets alike. Flag
   `depth_lines_outside_shore` over 1 %: Boy only (19.4 %). Either flag is `review`: 7 high and 4 high_stretched
   moved to review (Dahl, Davie, Ed Asp, Hallett, Hesquiat, Kedahda, Kluachesi; Mud, Ghost, Azure, Boy).
2. **Depth lines as a witness** (`depth_on_land_pct`, > 10 m outside our lake at the adopted placing): over 10 % on
   43, flagged `depth_lines_on_land`, all already review; the worst high one 7.6 % (no high sheet moved).
3. **A depth line as the outline** (refit step 5, `refit.depth_line_step`): only when the shore fit is not strict on
   its coded lake; adopted only when it lifts the class with no turn, mirror or lake within 0.03 (the full 50 km
   check on the line's own shape) and our lake inside the chart's shoreline; always `review` (`fitted_on_depth_line`,
   `fit_line`, `shore_over_lake`). 11 adopted: Abruzzi (2 m, poor 0.76 -> loose 0.82; with the true shore the plain
   shoreline is poor 0.68, 1.46x our lake and mirror-tied, so the line is still needed), Blenkinsop, Shesta, Sundance
   East, Wrigglesworth, Kitsumkalum, Suzanne, Azuklotz, Black (87), Jacko, Mackill. 10 refused at the last checks
   (Dunlevy, Green, South Caribou, Fifteen Mile, Og by another lake; Myles and Abruzzi's 1 m line by the mirror).

Confidence `high` 277 / `high_stretched` 46 / `review` 189 (v2 284 / 49 / 179); Blackwater moved up (its completed
shore fits strict stretched). No lake changed; 12 placings moved more than 5 m. None of the owner's 23 marked charts
moved. Tests: `test_bathymetry_t1188.py` (9 new). Outputs and pictures: night notes `T1188/v5/`.

## Acceptance
The strict/loose/poor counts before and after; the flagged-stretch list with before / after pictures; nothing ships without his look at each placing.
Severity: low (feature). Model: Opus. Size: M-L.

## Built
- 2026-11-27, branch `fix/T1188-chart-names-as-variants`: the four definite charts whose printed name differs from their lake's (owner: "the names that differ for definite should have those names added to the name variants for those lakes", each marked Right on his depth-chart review) are name variants in `waters/name_variants.json`, source `bathymetry`, chart number and his review cited: Grizzly Lake (00153501) on Simpson Lake, Snow Lake (00158801) on Iver Lake, Walkin Lake (00168601) on Zippermouth Lake, Kitwanga Lake (00177201) on Kitwancool Lake; the label stays the gazetted name, and no regulation binding moves (`backfill_matched --dry-run` identical with the names added).

## Owner

- [ ] 2026-11-26 — Look at the flagged-stretch list and the reorientation results before any lake ships; the matching checks are why you asked to wait. Default: nothing ships.
