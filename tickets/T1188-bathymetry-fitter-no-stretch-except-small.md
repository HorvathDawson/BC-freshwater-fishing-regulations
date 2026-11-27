# T1188 — Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial

Status: OPEN — before any lake ships (extends T201's fitter step); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1188 | Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial | OPEN — before any lake ships (extends T201's fitter step); owner: see ## Owner | branch feat/T1188-bathymetry-fitter | items 1-5 built and measured (branch, not merged: second review, before_pr, PR); his look at each placing; item 6 (T201's order, 3-lake pilot) |

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

## Built 2026-11-27 (items 1-5; branch feat/T1188-bathymetry-fitter, not merged)
`pipeline/bathymetry/refit.py` (the steps) and `refit_all.py` (the run; `GENERATED.bathymetry/refit.json`, `.csv`,
`_shapes.json`); `place_more._one` now takes the zip's path. A step is adopted only when it lifts the class and is not
ambiguous (another turn, the mirror or another lake within 0.03 IoU). Tests: `pipeline/tests/test_bathymetry_t1188.py`
(built shapes, no data). Measured on all 512 sheets (6 min, 4 workers, lane 3), baseline reproduced exactly:

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

## Acceptance
The strict/loose/poor counts before and after; the flagged-stretch list with before / after pictures; nothing ships without his look at each placing.
Severity: low (feature). Model: Opus. Size: M-L.

## Owner

- [ ] 2026-11-26 — Look at the flagged-stretch list and the reorientation results before any lake ships; the matching checks are why you asked to wait. Default: nothing ships.
