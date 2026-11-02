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

## Owner decisions (2026-11-02)
1. **"Daily limit 0" = closed.** Keep "closed" for unlisted waters on R1 / R5B / R2 13-15-16 pages (drop the wording
   proposal).
2. **Salmon non-retention = closed to salmon.** Unless a row EXPLICITLY says catch-and-release, treat "non-retention" /
   "no retention" / limit 0 as closed to salmon. (Catch-and-release only exists because other fish, e.g. trout, may be
   fished while salmon are present; for salmon it means the same as closed.) Apply across all regions.
3. **Unlisted = closed.** If nothing gives a water a salmon quota, it is closed. This covers R2's US-draining waters
   (Skagit, Similkameen headwaters, Nooksack tribs) and the 29 R6 pieces in no section. Handle it cleanly, not as a
   special case.
4. **R6 Section E residual is correct**, as long as the transboundary and Arctic rivers are named by DFO (they are).
5. **R5A / R7 "closed until further notice" vs listed in-season openings:** assume CLOSED. Check the old notices
   (cache/dfo_salmon/history, FN numbers) to see which statement held last, and report.
6. **Shared quotas across waters need a model:** the R5B aggregate ("in the aggregate (combined total) in MUs 5-6 to
   5-11") and the R2 Squamish hatchery-coho bucket. Use T332's quota tree: an aggregate tier whose scope is a set of
   waters/MUs. Also settle R5B's two definitions ("MUs 5-6 to 5-11" vs "Region 5 not including the Fraser") into one
   scope.
7. **Data model:** every region default must be a first-class ENTRY (a default location per region/scope, e.g. "R2
   waters flowing to Areas 28/29 — closed except listed", "R2 Areas 13/15/16 — limit 0 → closed", "R2 other / US-draining —
   closed"), resolved through the normal cascade, not hard-coded fallbacks. One clean rule: narrowest place wins, and
   an unlisted water falls to its region/scope default entry, which is closed unless a quota says otherwise.
