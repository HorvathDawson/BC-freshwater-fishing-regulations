# T184 — Keep the local curation review app; clean it up (the artifact version is retired)

Status: OPEN. Owner 2026-10-24: the review is better as "our old separate app — then we just clean it up". Phase 1's
artifact (the 30-target pilot page) was published, then deleted at the owner's request; its code goes too.

## What the review is for (owner)
Verify every ENTRY: (1) the regulation was parsed correctly (T text, R rules) and (2) its rules land on the right
water and extent (P), with A (the angler's answer) as the final sanity check — then the entry is saved as
HUMAN-REVIEWED. Splits are reviewed BY PROXY through each entry's extent: on the map the split shows its proper name
and place; a wrong split is a "Place: wrong" mark on an entry that uses it. No split is a review target of its own.

## Work
1. The queue and the pilot: entries only — drop the S0 split-point tier from `gate` (queue tiers, pilot quotas,
   reports); regenerate the 30-target pilot as entries across the tiers (zone entries rule by rule).
2. The map in review shows every split the entry's rules name, labelled with its name, so the proxy check is visible.
3. "Human-reviewed": a target marked verified on all its checks is shown as reviewed (by whom, when) on the screen and
   in the queue; it returns for a re-check when its fingerprint (text, rules or extent) changes (T97 marks).
4. Remove the artifact path: curation-review/artifact/, pipeline/regs/review/export_artifact.py,
   import_artifact_marks.py and their tests (keep the tributary-geojson fix in curation-review/backend/reuse.py).
5. Review mode only, as T185 says (fix-mode writers retired there); a tidy pass over the screen for the current build.
Acceptance: the local app reviews only entries; splits visible by name on each entry's map; the reviewed state shown;
artifact code gone; tests green.

## Outcome (2026-10-24)

DONE on `redesign/stream-sections` — the local app reviews ENTRIES only, splits by proxy, and writes one thing, a mark.

1. **Entries only** (`328d7718`): `tiers.TIERS` is P1 Z P2 P3 P4 sweep; `split_rank` / `GRADE_ORDER` and the split
   cards (`GET /api/review/splits`, `review.split_queue`, `SplitCardView`, the "split points (S0)" work view) are gone;
   progress counts entries alone; `POST /api/marks` refuses scope `split` in the reviewer's words; the split marks
   already in the store stay readable (`/api/marks?scope=split`, `/api/marks/splits`). `s0_split_ids` became
   `tiers.splits_to_verify`, read by the `split_to_verify` pre-check ("check its place on the map"). Pilot quotas are
   entries only: P1 7 · Z 5 · P2 4 · P3 5 · P4 7 · sweep 2. **The pilot is regenerated** for release
   `r-98c43091c440` (one build — atlas, reach, bundle, release agree; owner checkout's catalogue incl. T183's region-6
   edit): 30 entries, no tier short — `handoff/PLAN/curation-review/PILOT.md` (table + wording: no S0, how splits are
   checked by proxy) and `pilot.json`.
2. **Splits labelled on each entry's map** (`0050a267`, `11068cf7`): `GET /api/entries/{id}/splits`
   (`reuse.entry_split_points`) — every split id the entry's extents name (rules, scope, licensing) as a point with
   label, water, "WATER: label", the rules naming it, `to_verify`/grade; unplaceable ones in `unplaced` with why. The
   map draws them over everything, labelled (never culled), amber while still to verify, faded when the shown reach's
   rule does not name them, widening the view when one is off-screen; each rule has a "where" line naming its splits;
   the entry lists "Splits the rules name". Fixed on the way: lake boundaries had no map point at all (`_rep_point`
   read lists, shapely writes tuples), and a layer whose data arrived while tiles loaded was never drawn.
3. **Human-reviewed** (`0050a267`): `reviewed {by, on, holds}` on the entry and each queue row — verified on all
   checks (a zone entry: every rule); VerifyBar "Human-reviewed by X on date", queue chip; when the fingerprint moves
   (T97 stale reasons) `holds` is false — "re-check" — and the review filter `recheck` lists them.
4. **Artifact path removed** (`06fc178d`): `curation-review/artifact/`, `export_artifact.py`,
   `import_artifact_marks.py`, `test_review_artifact.py`; the tributary-geojson fix in `reuse.py` kept.
5. **Review mode only** (`d959855b`, `23dfc3ca`; T185's writers part, marked there): editors, routes and writer
   functions removed; pins in `test_curation_review_safety` (one write route whatever the env; no backend module
   but `writes.py` writes a file, its one caller the mark — both with mutations) and the screen test's route sweep
   (nothing under `data/curated` changes). Tidy: dead reads (vocab, species, item search / boundaries / tributaries,
   split-groups, GET split / refs, the live-match suggestion, the unused bundle wrappers in `api.ts`), the editors'
   CSS, `model_api` vocab / warnings; docs (`38b1afb5`: README, API.md, frontend README, AGENTS 66/67).

Tests: the curation-review + review suites (16 files) 319 passed unfiltered; browser test (`-m slow`) passed;
frontend `tsc --noEmit` clean. Screen checked on own ports (Ash River, Sproat River: labelled splits, reviewed banner).
