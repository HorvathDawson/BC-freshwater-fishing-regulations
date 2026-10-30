# T266 — Marks are review state; decided data lives in its own files (one exporter)

Status: IN REVIEW (2026-10-31, ec2d60bdd) — exporter + apply paths on the export; a sandboxed full build with marks.jsonl unreadable passed and matched r-51d80e68f056 (`handoff/PLAN/app-integration/build-pass-2026-10-31.md`). Owner: confirm the one-directory split `data/curated/decided/`.

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
