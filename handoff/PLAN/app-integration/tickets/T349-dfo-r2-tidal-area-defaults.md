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

## Done (2026-11-02) — status DONE (live build local; no staging upload: salmon/3 guard)
- **Merge:** `dfo/t332-shared-quota` merged (0c276aa10; 4 conflicts resolved: wording.py keeps the T331 registry path
  with dfo-wording/2, SalmonCard, ledger, owner to-do). Guard `SALMON_NEEDS = {3: "0.5.0"}` in
  `deploy/staging/upload.sh` (a2415b239): a salmon/3 release is refused on staging below app 0.5.0.
- **One model for a scope default** (`entries.PAGE_DEFAULTS` / `PAGE_DEFAULT_ON`): the page's sentence is the rule
  (`cascade.statement_locators`, feed key `page:<slug>:<scope>`) of one scope entry. Where the page prints its own row
  for the same scope it joins that row (4:region:section, 5b:region:section, 6:a:region); else a `page_default` entry,
  precedence -1 (after every table row; narrowest first among them): 1, 2 (Areas 28/29, Areas 13/15/16), 3, 5a, 7, 8.
  R2's remainder (to the US) has no sentence → no entry → `NOT_LISTED` closed. Removed: 4/5b/6 parallel defaults,
  2:default:other, R6 dormant duplicates (areas-5/6/3-4-5-6, d:section~2 folded into the active rows' fingerprints;
  b:section~2 into the dormant b:section, which keeps the archived wordings).
- **Non-retention = closed** (`deliver.salmon.says_catch_and_release`): `release` only where the row says catch and
  release. Tests: `pipeline/tests/test_t349_page_defaults.py`.
- **Shared quotas:** R5B's "in the aggregate" row and R2 Squamish hatchery coho are the tree's aggregate tier (`total`,
  `whose` = MUs 5-6 to 5-11 / the Squamish River watershed) — `quota.mark_shared` / `shared_buckets`; core draws
  `bucket.shared`. R5B settled to the MU list.
- **R5A / R7:** stay CLOSED. History: the 2025-08-21 pages opened Horsefly Bay sockeye (FN0846) and Nechako sockeye
  (FN0851) under "except for the opportunities listed"; the 2026-04-01 pages restored "until further notice" and kept
  the 2025 rows. The closure is the later statement. (R7 did the same in 2022→2023, FN0955.)
- **Atlas:** registry mints `area:drains_to:pfma_{13,15,16,28,29}`. Side build `data/generated/side/t349`: parity +5
  items, 0 changed, 0 boundaries; handles unchanged (822db448); binding diff: no rule binding changed. Salmon vs
  r-4e709418ebd8: release→closed 13.4M section-species-days (all non-retention), closed→open 47k and closed→check 773k
  all on the 19 owner-reviewed DFO places applied since the 10-31 reach run. Other release files: water* differ only in
  answers.schemas/about + meta (T332's TreeTier `period`). Promoted; live build run.
- **Extent queue:** scope entries are not water places (curation-review `dfo._compute_rows`); they show in the Cascade
  tab under DFO's words. Queue rows before → after (all): 1 84→83, 2 69→66, 3 10→9, 4 2→0, 5a 5→4, 5b 14→12, 6 164→147,
  7 3→2, 8 8→7.
- **Worker:** per owner principle (cascade resolved in Python) the worker was left as is; its byte-parity suites are
  skipped until T350 (inventory in T350).

## RESUME (paused by owner 2026-11-02 ~17:45)
- **Done (commits):** 0c276aa10 merge T332 · a2415b239 SALMON_NEEDS guard · f5fc9c356 page defaults / non-retention /
  shared quotas · next commit: one scope-default model + queue cleanup + worker revert (T350) · b06ca12de ticket ·
  latest: fix(tiles) reuse a released tile folder with the same content hash.
- **Gate: PASSED** (side build `data/generated/side/t349`): atlas parity +5 drains_to areas, 0 changed; handles
  822db448 unchanged; rule binding diff empty; every salmon status change explained (non-retention → closed; 19
  owner-reviewed DFO places bound since 10-31). **Side atlas PROMOTED** (`data/generated/atlas/full`; old → full.prev).
- **Live build:** run 1 failed at tiles (released tile folder already existed, same content — fixed). Run 2
  (`scripts/heavy.sh t349-live2`, PID 55756, log `data/generated/side/t349-live2.log`) was left RUNNING at the pause:
  reach up to date, tiles then deliver. Not stopped: a stop mid-tiles would leave a half-written live atlas.pmtiles.
- **Remaining, in order:**
  1. `tail -20 data/generated/side/t349-live2.log` — expect `deliver ok` and the vintage lines. If it failed, rerun
     `scripts/heavy.sh t349-live 60 -- .venv/bin/python -m pipeline build --jobs 2`.
  2. Check that the new release in `data/release` names salmon/3. LATEST may move locally; no staging upload until
     app 0.5.0 (the SALMON_NEEDS guard refuses it).
  3. Fast suite ONCE, unfiltered: `scripts/heavy.sh t349-suite 60 -- .venv/bin/python -m pytest -q -p no:cacheprovider`
     (expected new reds: none from T349; the targeted DFO / salmon / T349 tests passed).
  4. `graphify update .`
- **t349-live2 final (2026-11-02):** tiles ok (8m26s, reused release folder 822db448a2851809.f98ea27b8e5f); deliver
  was TERMINATED (SIGTERM, "Terminated: 15" — not by this lane) before writing a release. No new release id; LATEST
  still r-4e709418ebd8. Resume at step 1: rerun `scripts/heavy.sh t349-live 60 -- .venv/bin/python -m pipeline build
  --jobs 2` (reach and tiles are up to date, so only deliver runs, ~10 min).
- **State at stop (coordinator, owner request):** t349-live2 was stopped during deliver (PIDs 55756, 55766, 59839).
  The T349 atlas IS PROMOTED (`data/generated/atlas/full`). The live tiles were rebuilt from it; the bundle / release
  were NOT — so locally the tiles and the bundle are DIFFERENT VINTAGES until the next live build. LATEST unchanged
  (r-4e709418ebd8); no new release folder. **Next step (owner):** ONE atlas build + live build that also includes the
  T326 tidal changes (it supersedes step 1 above); then the fast suite once and `graphify update .`.
