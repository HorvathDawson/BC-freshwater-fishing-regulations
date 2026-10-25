# T162 — Salmon and steelhead run timing in the app: refresh the data, ship it, colour the map by when fish are there

Status: PARTIAL — data + plan; app integration awaits owner

Source: owner 2026-10-23: "add a ticket for adding run timing info that we scraped into the app too. will have to create
the feed … we started implementing a run timing with the stream network on when fish would have to be passing through an
area for each species etc. … it will need the data fetching and also the working out how to display it (probably as a
coloring layer)." Related: T95 / T132 (salmon regulations; the owner said 2026-10-22 salmon is not dropped).

## What exists (pipeline/runtiming/README.md — "PREPARED, NOT FED")
- Source: Pacific Salmon Foundation dataset #90, "Average Run Timing of Salmon and Steelhead Conservation Units" (LGL,
  DFO, Pacific Salmon Commission; revised ~yearly), read from the Pacific Salmon Explorer's three server-rendered pages.
  Raw pages + 748 CU vector tiles in data/source/runtiming/ (fetched 2026-09-08; upstream updated 2024-09-10).
- fetch/ (explorer.py, boundaries.py) → generate/ (extract 463 runs; index: section → CU by midpoint, 581 profiles;
  passage.py: walks the flow graph DOWNSTREAM from each CU to tidewater — 94,098 passage reaches, 1,192 profiles, with
  `km_to_sea`; profiles; indicators) → consume/bundle.py (its own run_*/section_run/passage tables, NOT in schema.sql).
- Outputs in data/generated/runtiming/ (2026-09-08): run_cube.bin (581 profiles × 7 species slots × 73 pentads, max over a
  species' runs — for a COLOUR), passage_cube.bin, profile_runs.json / passage_profile_runs.json (which runs, labels,
  peaks — for a SHEET), run_clim.csv, run_timing_daily.csv. data/curated/runtiming/review.json is AUTHORED (keep).
- Design rules already decided in the README: a reach has RUNS, not a run (steelhead winter AND summer); Pink even/odd
  are separate slots picked by the year shown; spawning (inside the CU) and passage (between CU and sea) are TWO
  relations that render differently (solid vs dashed); the curve is RIVER-ENTRY timing — no invented travel time,
  `km_to_sea` shown instead; the passage walk must not cross into another river's mouth (watershed-code ancestor guard).

## Work
1. Data ("the feed"): re-fetch (3 GETs + CU tiles; check the dataset's terms/licence and record the attribution the
   app must show), then regenerate against the CURRENT atlas (section ids moved since 2026-09-08 — tiles and bundle must
   share a vintage). A yearly refresh path: a fetch notice like the HYDAT one, owner-run.
2. Ship it: a release file of its own (e.g. `runs/1`: the two cubes + the run lists, keyed by the section → profile
   map), not the core bundle; inside the release size gate (cubes are ~11–14 KB gz each; measure the section → profile
   map). consume/ writes it from the frozen generated index only.
3. App, a colouring layer: a "Runs" layer in the map's layers menu (like Level/Change) — pick a species (and run where a
   species has several), the date pill drives the pentad; colour = strength from the cube; spawning solid, passage
   dashed (or a second ramp); one colouring pathway (the colouring engine), light/dark/cvd themes through the gates.
   Tap a section → a sheet listing every run here (species, run label/CU name, peak, window, spawning or passing,
   "dates are river entry; you are N km from the sea"). Words and limits in core; no regulation logic involved.
4. Tests: whole-value tests on the cube read and the sheet words; a no-second-copy pin (one reader of the cube);
   shots of the Skeena (multi-run steelhead) and the Fraser (Early Stuart / Summer sockeye) in light and dark.

## Owner questions (defaults to build with)
- Default species when the layer opens? Default: steelhead.
- Show passage at all, or spawning only first? Default: both, passage dashed.
- Where it lives: its own map layer (default) or inside Conditions?

Depends: nothing blocking; heavy (generate needs the atlas) → one at a time via scripts/heavy.sh. Size: L.
Rebuild: runtiming fetch + generate, release (new file), app.

## Outcome (2026-10-24, reopened) — PARTIAL: data + plan; app integration awaits owner

Reopened after PSF granted permission (their reply is not stored or quoted anywhere). Owner scope (same day): the data
and an integration plan only, nothing in the app.

- **Data:** `python -m pipeline.runtiming.parse` -> `data/curated/runtiming/timing.json` (tracked): 329 PSF CU curves +
  4,944 DFO NuSEDS stream records (new `fetch/nuseds.py`, OGL – Canada). The three PSF pages are untracked
  (gitignored, Q163); `fetched.json` records the permission and attribution, not the reply.
  Tests: `pipeline/tests/test_runtiming_parse.py`.
- **Plan:** `handoff/PLAN/runtiming/PLAN.md` — binding at delivery (spawning / passage / observed, with the guards a
  prototype run on the live atlas found), the `runs/1` release file (866 kB gz; the total would be ~14.92 of 15 MB),
  the Fish runs colouring, the runs sheet, credits, and the open questions (Q144, the size gate).
- **Not done (owner's call):** any deliver, release or app change. A prototype of all of it was built, measured and
  reverted (not committed).

## Outcome (2026-10-24) — STOPPED at step 1: the terms forbid redistribution (superseded above)

Nothing was fetched, regenerated, shipped or built. The brief was to record the dataset's licence first and stop if it
forbids redistribution; it does.

- **Dataset #90's record** (https://data.salmonwatersheds.ca/result?datasetid=90, read 2026-10-24): "Average Run Timing
  of Salmon and Steelhead Conservation Units", Pacific Salmon Foundation / Salmon Watersheds Program, updated
  2024-Sep-10, source file `dataset90_run_timing_2024-09-10.csv`. **No licence field, no DOI, no reuse grant.** So the
  upstream data has not changed since the 2026-09-08 fetch.
- **The Salmon Watersheds Program's Terms of Use** (https://salmonwatersheds.ca/terms/, which govern the site's
  document/data library) say, verbatim: "we authorize you to view and download the information and materials … only
  for your personal, non-commercial use … You may not copy, publish, post or mirror any Materials on any other server,
  website or other platform", and "the Content may not be copied, modified, reproduced, republished, transmitted, sold,
  offered for sale, or otherwise distributed without our prior written permission."
- **The Pacific Salmon Explorer** (where `fetch/explorer.py` reads the curves and `fetch/boundaries.py` the CU tiles)
  states no licence at all, only "© Pacific Salmon Foundation 2024" — all rights reserved by default.
- A `runs/1` release file (cubes + run lists derived from the curves and CU polygons) served to every app user is
  distribution of modified Content. Attribution alone does not make it allowed.

**Also flagged:** `data/source/runtiming/{run-timing,catch-and-run-size,biological-status}.html` are TRACKED in a PUBLIC
GitHub repo (`HorvathDawson/BC-freshwater-fishing-regulations`). That is a mirror of PSF Content on another platform —
the owner's call whether to untrack them (`git rm --cached`, add to `data/.gitignore`) and purge them from history.

**To unblock:** the owner asks PSF for written permission (info@salmonwatersheds.ca, 1-604-664-7664) to ship derived
run-timing curves and CU-based reach assignments in a free app, with the attribution they want. With permission in hand,
the Work section stands as written (Q144 defaults: steelhead first; spawning solid + passage dashed; its own layer), and
the permission + attribution text get recorded in `data/source/runtiming/fetched.json` and `pipeline/runtiming/README.md`.
Alternatives that need no permission: DFO's own open run-timing data (Open Government Licence – Canada) for the
salmon species — a different source with different coverage (no steelhead: steelhead is provincial), so a new design
pass, not a swap.

**After T67 (braids) changes section ids:** whenever this ticket resumes, generate runs AFTER the braid atlas is
promoted, never before — section ids are the join key.
