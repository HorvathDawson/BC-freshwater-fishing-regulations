# T349 — DFO Region 2 tidal-area defaults + "limit 0" unlisted wording (PROPOSAL)

Owner 2026-11-02. Found by the Cascade tab's region view (`curation-review/backend/region_cascade.py`).

## What the pages say that the code does not encode
- R2: "There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the
  opportunities listed in the table below." / "Unless otherwise stated in the table below, the daily limit for
  waters in Region 2 flowing into Areas 13, 15, and 16 is zero (0)." — no default location exists; an unlisted
  R2 water gets the bundle's NOT_LISTED "closed" everywhere.
- R1, R2 (13/15/16), R5B: "daily limit … is zero (0)" — the bundle says "closed"; limit 0 leaves release open.
  (R6 says the same but section A lists every species all year, so it never answers.)

## The link exists (no new pipeline)
`pipeline/atlas/splits/pfma_drainage.py` already maps every basin to its mouth's nearest PFMA Area province-wide;
`pfma_areas` carries polygons for 13/15/16/28/29. Only the minting is limited: `registry/build.py` calls
`basin_areas()` with the default `SECTION_E_AREAS = (3, 4, 5, 6)`.

Measured (cached table `data/generated/review/pfma_basin_areas.json`): of R2's 71,642 sections, 38,075 drain to
28/29, 26,082 to 13/15/16, 3,717 drain to no BC Area (Skagit/Sumallo/Klesilkwa → Puget Sound; Similkameen →
Columbia; Fishtrap/Bertrand → Nooksack), 3,768 are lakes with no FWA code.

## Design (small)
1. `build.py`: mint `area:drains_to:pfma_N` for 13, 15, 16, 28, 29 too (pass the areas; one line).
2. `entries.TIDAL_EXTENTS`: `(28, 29)` and `(13, 15, 16)` with `within_area: area:region:2`; two R2 default
   locations (precedence 2) from the page sentences (they need a source row — `header._RE_STOP` currently stops
   at them).
3. Owner decision: the US-draining waters (Skagit, Similkameen, Nooksack tribs) — which default? The page names
   only Areas 13/15/16/28/29.
4. Owner decision: unlisted answer on "limit 0" pages = release (not closed)?
Needs an atlas rebuild (human-scheduled).
