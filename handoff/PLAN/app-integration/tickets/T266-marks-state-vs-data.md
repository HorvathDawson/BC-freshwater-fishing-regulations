# T266 — Marks are review state; decided data lives in its own files (one exporter)

Status: DONE (2026-11-05, cbb457042) — T408 re-scope landed: no `decided/`; the writers compute the marks' decisions in memory (`review.export.load`). Was IN REVIEW (2026-10-31, ec2d60bdd).

Source: owner 2026-10-29: "should we get tidal marks into a separate file to simplify? so marks is just state of review
then data goes into the individual files? could use a single exporter."

## Problem
`data/curated/regulations/review/marks.jsonl` mixes two things: review STATE (who checked what, verdicts, notes) and
DECIDED DATA (tidal cut points, DFO extents, slough/pond calls, name accepts, new splits). Builds read decided data out of
review marks through several per-scope paths (proposals/apply, tidal, sloughs, q154, names, in-season), which caused the
"applying a mark made it look stale" bug and today's release blocker (an uncommitted marks file).

## Work
1. One exporter (`python -m pipeline.regs.review.export`) turns the marks in force into per-domain decided-data files,
   each with provenance (mark id, reviewer, date): e.g. `data/curated/waters/tidal_boundaries.json`,
   `data/curated/dfo/extents.json`, `data/curated/waters/slough_calls.json`, `…/pond_calls.json`, `…/names_accepted.json`.
   Deterministic, idempotent, diff-friendly; refuses on conflicts.
2. Builds read only those data files (never marks.jsonl); marks.jsonl stays the review log/state (append-only, owner).
3. The review app shows done-state from marks + "landed in data" from the exported files (marks.health already does
   held/landed/stale/orphan).
4. Migrate the existing apply paths onto the exporter; one test proves export(marks) == committed data files.

## Acceptance
A build with marks.jsonl absent but data files present produces the same release; review app progress unchanged;
mark-health 0 orphans.

Depends: none. Size: M. Owner: confirm the file split (default as above).

- Owner 2026-10-31: one `data/curated/decided/` folder is fine (approved).

## T408 re-scope (owner walkthrough 2026-11-05, `handoff/PLAN/data-flow-decisions.md`)
`decided/` is dropped (owner 2026-11-05): the exporter's per-domain copy goes; the apply paths read the marks
directly (with T325). The rest (build never reads marks) stands.

Done 2026-11-05 (cbb457042): `data/curated/decided/` removed (git rm), `CURATED.decided` + config key gone, and
`NOT_BUILD_INPUTS` drops it. `review.export.load(marks, closed)` = `decide(marks)` less closed domains — no file IO,
no `--check`. apply / apply_proposals / proposals take `--marks`. Proof: `review.apply --dry-run` output identical
before and after (tidal, sloughs, names current; the same proposals waiting); in-memory load == the old files'
load (2095 decisions, 1179 cuts, provenance equal). Targeted tests pass.
