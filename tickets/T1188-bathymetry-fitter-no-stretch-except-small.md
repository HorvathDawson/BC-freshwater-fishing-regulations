# T1188 — Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial

Status: OPEN — before any lake ships (extends T201's fitter step); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1188 | Bathymetry fitter: no stretch except small ones; flag the lakes a stretch would help; a reorientation trial | OPEN — before any lake ships (extends T201's fitter step); owner: see ## Owner | — | wider rigid search, a two-lake fit, a flagged class for stretches of 5% or less; bigger-stretch sheets not adopted but listed with a before / after for his eye; then the 3-lake pilot |

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

## Acceptance
The strict/loose/poor counts before and after; the flagged-stretch list with before / after pictures; nothing ships without his look at each placing.
Severity: low (feature). Model: Opus. Size: M-L.

## Built
- 2026-11-27, branch `fix/T1188-chart-names-as-variants`: the four definite charts whose printed name differs from their lake's (owner: "the names that differ for definite should have those names added to the name variants for those lakes", each marked Right on his depth-chart review) are name variants in `waters/name_variants.json`, source `bathymetry`, chart number and his review cited: Grizzly Lake (00153501) on Simpson Lake, Snow Lake (00158801) on Iver Lake, Walkin Lake (00168601) on Zippermouth Lake, Kitwanga Lake (00177201) on Kitwancool Lake; the label stays the gazetted name, and no regulation binding moves (`backfill_matched --dry-run` identical with the names added).

## Owner

- [ ] 2026-11-26 — Look at the flagged-stretch list and the reorientation results before any lake ships; the matching checks are why you asked to wait. Default: nothing ships.
