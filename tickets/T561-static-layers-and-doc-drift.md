# T561 — Static layers: real source dates, no-access overlap, OSM fetch checks; pipeline docs match the code

Status: PARTIAL — pipeline side of all 8 items committed (9b5edac88, 36d196b7f, 56a9d60ef, 6326e3264, 3488e571d, df0b0d0d7, 41ecaabb5, 247112b44). Owner 2026-11-11: the union no-access share goes to the app with the next release format change, not on its own. left (Claude): item 1 an `access` source key in SOURCE_KEYS; item 2 the union share in core/access.ts (with the next format change); item 4 gate.py:13 docstring; item 6 staleness.ts wording; item 8 review-lane docstrings (apply/export/write_through/apply_proposals, data/curated/review/README.md marks rule + its test).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T561 | Static layers: real source dates, no-access overlap, OSM fetch checks; pipeline docs match the code | PARTIAL | 9b5edac88, 36d196b7f, 56a9d60ef, 6326e3264, 3488e571d, df0b0d0d7, 41ecaabb5, 247112b44 | left (Claude): `access` source key; union share in access.ts (with the next format change, owner 11-11); staleness.ts wording; review-lane docstrings |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 17 of 17; highest severity 2 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Done / left (2026-11-10)
1. 9b5edac88 — fetch_data records each layer's SOURCE day in `bc_fisheries_data.source.json` (`pipeline.common.source_dates`; FWA from the object store's s3b-last-modified, probed: 2024-03-26); `sources_block` dates the FWA from it, never `last_change`; `source_pins pin` copies it (`layer_dates`). A release built now states fwa 2024-03-26, past the app's 730-day rule. LEFT: an `access` source key — `SOURCE_KEYS` feeds `pack.generated.ts` and the app's freshness list (app lane); the access-layer days are already in the sidecar.
2. 36d196b7f — `section_no_access` (union share, ≤1000) is what `mostly_inside` reads; `write` refuses a bundle where the summed per-area rule (app, paint_sync) and the union disagree. Live tiles: 7 summed >1000, union max 1000, 0 disagree. LEFT: the app (`core/access.ts`) and the release format still sum per-area rows — ship the union share to the app.
3. 56a9d60ef — Overpass remark / no elements = OverpassPartial (retried, then fails); empty results fail; the named area is pinned by OSM way 1166294466 (was labelled a relation); missing BC boundary and a failed roster fail; row counts recorded and `source_pins pin` refuses a >10% drop without `--accept-drop`.
4. 6326e3264 — promote.py, build.py, vintage.py reworded. LEFT: `pipeline/regs/review/gate.py:13` (review lane; pending in test_doc_drift).
5. 3488e571d — ReachCache/entry_key deleted; cache.py is the POLICY_VERSION changelog; REACH-BUILDER.md rewritten from the code; AGENTS 20 updated.
6. df0b0d0d7 — 15-live-data-flow.md gauges section, cuts.py/review.py/build.py/bundle_tables.py/check_curated.py paths, schema.sql + climatology.py wording; empty pipeline/atlas/gauges removed. LEFT: `app/packages/core/src/staleness.ts:66` ("every run"; app lane).
7. 41ecaabb5 — both READMEs and MAP.md say audit.py was deleted in T436 and how to restore it.
8. 247112b44 — HANDOFF-curated-layout.md marked historical. LEFT (review lane): apply.py/export.py/write_through.py/apply_proposals.py docstrings, data/curated/review/README.md marks.jsonl rule + its test.

## Left, with the review's evidence
- Item 1, `access` source key: the app shows the manifest date (app/packages/core/src/more.ts:124) and ages it with a
  730-day stale rule (staleness.ts:62, menu/freshness.ts:81); the gpkg's other layers have their own days
  (land_parcels_private 2026-07-15, land_access 2026-07-22, pfma_areas 2026-09-15) and SOURCE_KEYS (manifest.py:33) has
  no key for them. Add an `access` (parcels / land_access / reserves) source date to the manifest and the freshness list.
- Item 2, union share in the app: pipeline/deliver/bundle/access.py:212-236 writes one permille per area; the app's
  MOSTLY_INSIDE rule SUMs permille over all no_access areas and compares with 500 (nested OSM polygons: 19 overlapping
  pairs in data/generated/tiles/layers/no_access.geojsonl). Example: sid 143248 = "Slesse Creek Demolition Training
  Areas:1000 | Slesse RTA:1000" (2000 permille); no section mis-coloured today. access.ts caps with Math.min(1, ...) at
  lines 140, 181 and 225. With the next release format change (owner 2026-11-11).
- Item 4: `pipeline/regs/review/gate.py:13` still says 'the atlas build is not deterministic' (T65 proved it is; the
  immutability rule stays right: derivatives are keyed by the handle digest).
- Item 6: `core/staleness.ts:66` says the HYDAT listing is checked "every run" (once a day, publish.ts:503-512).
- Item 8: apply.py:20, export.py:13 and write_through.py:11 still list a "splits" domain (apply.py:43 PROPOSAL_DOMAINS =
  ("dfo","entries"); T434); apply_proposals.py:7 says tidal lands through it (T326 moved tidal to a file writer);
  data/curated/review/README.md:5 says marks.jsonl is never edited by a tool (commit 1d58354b3 edited a line): state the
  real rule (append-only except diff-proven id renames) next to a test that enforces it.

## Acceptance
The app shows the FWA vintage, not the local file date; access layers carry a date. Overlapping no-access polygons are unioned before the share is summed. fetch_data fails on an Overpass remark or an empty answer. The listed stale docs and comments are corrected or marked historical.

Depends: —. Size: S. Owner: no.

## Owner answer 2026-11-11
The union no-access figure goes to the app with the next release format change, not on its own. The build check stays meanwhile.
