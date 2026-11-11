# T561 — Static layers: real source dates, no-access overlap, OSM fetch checks; pipeline docs match the code

Status: PARTIAL — pipeline side of all 8 items committed (9b5edac88, 36d196b7f, 56a9d60ef, 6326e3264, 3488e571d, df0b0d0d7, 41ecaabb5, 247112b44); left: the app/review-lane parts listed under "Done / left".

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 17 of 17; highest severity 2 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. The app's "Freshwater Atlas" date is the day the gpkg was written locally, not the FWA vintage; access layers carry no date at all
Severity 2, wrong-or-missing-data, confirmed, reaches an angler; area static-layers.
- Evidence: pipeline/deliver/release/manifest.py:120-129 (_gpkg_day) reads gpkg_contents.last_change of table `streams`; :182 puts it in manifest sources.fwa.date. data/source/pins.json records the same file's upstream as "FWA (2024-03-26, nrs.objectstore.gov.bc.ca/itqlyp/FWA_Public) + live WFS/Overpass layers as fetched on this Mac". The app shows the manifest date (app/packages/core/src/more.ts:124) and ages it with a 730-day stale rule (staleness.ts:62, menu/freshness.ts:81). The gpkg's other layers have their own days (land_parcels_private 2026-07-15, land_access 2026-07-22, pfma_areas 2026-09-15) and SOURCE_KEYS (manifest.py:33) has no key for them.
- Example: Live release r-a178299f6005 manifest: sources.fwa.date = "2026-07-15"; pins.json says the FWA bytes are the 2024-03-26 publication. Measured from 2024-03-26 the 730-day rule would already be past; measured from 2026-07-15 it is not.
- Second opinion: Accurate. The code is honest that it is a fetch day; the fault is that the app presents and ages it as the source's date. The 2024-03-26 figure comes only from the pins.json note, not from a machine-read field, so the fix needs a recorded upstream day first. Effect is a missed stale notice, not a wrong regulation.
- Proposed fix: Record the upstream publication day per layer at fetch time (a small sidecar beside the gpkg, also copied into pins.json) and have sources_block read that, never last_change. Add an `access` (parcels / land_access / reserves) source date to the manifest and the freshness list. Test: manifest fwa date equals the pin's recorded upstream day.

### 2. No-access share is summed across overlapping polygons, so a section can read over 100% and the 'mostly inside' test double counts
Severity 2, fragility, partly, reaches an angler; area static-layers.
- Evidence: pipeline/deliver/bundle/access.py:212-236 accumulates the inside length per (sid, area) and writes one permille per area; mostly_inside (:279-285) and the app's MOSTLY_INSIDE rule SUM permille over all no_access areas and compare with 500. The no_access tile layer contains nested OSM polygons: 19 overlapping pairs of `closed` features with different area_id in data/generated/tiles/layers/no_access.geojsonl. No check that the per-section sum is <= 1000.
- Example: bundle.sqlite: sid 143248 = "Slesse Creek Demolition Training Areas:1000 | Slesse RTA:1000" (2000 permille); 7 sections sum over 1000. No section is currently mis-coloured (0 sections with sum > 500 and max <= 500).
- Second opinion: The double count is real but changes no colour today (all 7 over-1000 sections are fully inside anyway). "Pooled part access can state more than the whole length" is refuted: access.ts caps with Math.min(1, ...) at lines 140, 181 and 225. I did not re-count the 19 overlapping polygon pairs. Python and app agree with each other, so this is a shared rule flaw, not a divergence.
- Proposed fix: Measure the no_access tier against the union of its polygons for the 'mostly inside' decision (keep per-area rows for naming), or cap by union in mostly_inside. Add a bundle check: per sid, the no-access union share <= 1000.

### 3. Source fetch (data/fetch_data.py) accepts partial or empty OSM answers: no Overpass `remark` check, 'none found' exits clean, a failed named-area geocode is only a warning
Severity 2, fragility, confirmed, reaches an angler; area gaps.
- Evidence: data/fetch_data.py:85-121 `_overpass_query` returns the JSON on any HTTP 200; no code reads `remark` (0 hits in the file), yet Overpass answers 200 with a `remark` runtime error and partial `elements` on timeout or memory limit. All readers take `raw.get('elements', [])` (lines 258, 544, 752, 866, 1156). Empty results print a warning and `return` (451, 696, 828, 905), leaving the old layer in the gpkg and exit 0. The hand-listed restricted area is geocoded by name, first hit, failure = logger.warning + continue (490, 677-692). A missing boundary prints 'gazetteer NOT clipped to BC' (1489). A failed live roster writes every station `realtime: false` and the file is then never refetched (1305-1322).
- Second opinion: Code claims hold. No real partial fetch was shown (the finder gave no example), and this is a hand-run, occasional refresh, so it is latent. The effect on anglers is limited to the access, indigenous-land, water-access and waterfall layers, not regulations. I did not trace what consumes the station file's `realtime` flag (gauge matching is frozen in data/curated/gauges/matches.json), so that sub-claim's impact is unverified.
- Proposed fix: Raise on `remark` or missing `elements`; make an empty result an error; pin the named area by OSM relation id and fail when it is absent; before `source_pins pin`, print and gate per-layer row counts against the previous pin (e.g. refuse a drop over 10% without --accept).

### 4. Four code comments still say the atlas build is not deterministic; T65 proved it is
Severity 1, doc-drift, confirmed; area atlas.
- Evidence: pipeline/atlas/promote.py:6-9, pipeline/common/vintage.py:139-142, pipeline/build.py:33-34 and pipeline/regs/review/gate.py:13 all state 'the atlas build is not deterministic' and cite memory `atlas-build-not-deterministic`. Ledger row T65 'determinism proof (N12)' is DONE (0d5d3577f); the owner's note of 11-04 records two builds as identical apart from timings, build_ts and pickled set order.
- Second opinion: Stands as doc drift. Minor nuance: the immutability rule these comments justify is still right (handle digest keys every derivative), so only the stated reason is wrong. No behaviour change.
- Proposed fix: Reword the four sites: the build is deterministic for the same inputs, and the promoted directory is immutable because derivatives are keyed by its handle digest. Consider a cheap determinism check on the registry digest.

### 5. ReachCache is unused and unsafe by its own notes; REACH-BUILDER.md still says 'Not built'
Severity 1, doc-drift, confirmed; area reach-binding.
- Evidence: pipeline/atlas/reach/cache.py:186 ReachCache and :141 entry_key have no caller outside pipeline/tests/test_reach_builder.py (grep). cache.py:64-66 and :137 admit that the answer depends on other entries (rowed_waters), which the key does not see. _resolution_inputs (:153-183) also omits `undrawn_part` (decides the walk, classify.py:105), `standing`, licensing records and tidal splits. pipeline/docs/REACH-BUILDER.md opens with '**Not built.**', cites 3,038 rules (live: 3,401), lists 'Incremental: sha256 cache' as load-bearing, and its open-policy table (ambiguous cut 'leaning: refuse', tributaries_pending) contradicts classify.py:40 and report.json (tributaries_pending 0).
- Example: test_reach_builder.py:467 pins POLICY_VERSION == "35", so every policy change must bump a version for a cache nothing uses.
- Second opinion: Nothing overstated, but it is housekeeping: no angler-facing effect today, since nothing calls the cache. POLICY_VERSION does double duty as the policy changelog that other tests read, so deleting the cache should keep that list. The 'serves stale reaches if adopted' harm is hypothetical.
- Proposed fix: Delete ReachCache and entry_key (keep the policy list as a changelog), or key it on the corpus-wide inputs before use. Rewrite REACH-BUILDER.md from classify.py's constants.

### 6. Gauge docs describe a pipeline that no longer exists
Severity 1, doc-drift, confirmed; area gauges-hydat.
- Evidence: pipeline/docs/15-live-data-flow.md:20 says generate.match runs "per build" (pipeline/gauges/__init__.py says a few times a year, by hand). The doc says clim.json is "325 KB, ~450 stations" (actual 8.9 MB, 1,959 stations; the worker slims it at publish.ts:488), that the reading tier is a Python "hydro job", and it does not mention obs.json, hydat/<station>.json, carry-stale or `stale`. pipeline/gauges/consume/cuts.py:8-22 names `pipeline/gauge_match.json` and `data/bc_hydrometric_stations.json`. core/staleness.ts:66 says the HYDAT listing is checked "every run" (once a day, publish.ts:503-512). pipeline/atlas/gauges/ holds only __pycache__.
- Example: The doc's stated reason the cron can hold the envelope ("325 KB plus 450 readings fits anywhere") is 27 times off against the real file.
- Second opinion: Nothing overstated. Also stale: pipeline/tools/audit/check_curated.py:20,121 still names gauge_match.json in its header. The empty pipeline/atlas/gauges directory is untracked leftovers, not a repo defect.
- Proposed fix: Rewrite the Gauges section of 15-live-data-flow.md from the worker's actual outputs and cadences. Fix the cuts.py paths. Delete the empty pipeline/atlas/gauges directory.

### 7. Stocking and bathymetry READMEs describe files that do not exist
Severity 1, doc-drift, confirmed; area static-layers.
- Evidence: pipeline/stocking/README.md table lists `generate/audit.py` ("written; run by hand", shared with bathymetry) and pipeline/bathymetry/README.md lists `generate/audit.py` ("written; wrote data/generated/bathymetry/audit.json"). pipeline/stocking/generate/ holds only identifiers.py and match.py; pipeline/bathymetry/generate/ holds only __init__.py. The rest of the READMEs (prepared, not fed; lakes/1 the one landing point; empty stock_water/chart tables) matches the code.
- Example: `ls pipeline/bathymetry/generate` -> __init__.py only.
- Second opinion: Accurate, doc drift only. Added evidence: the audit's output file is still on disk, orphaned from its generator.
- Proposed fix: Fix the two README tables, or restore the audit from git history when the feed work starts; note in pipeline/docs/MAP.md.

### 8. Docs and docstrings describe a review flow that no longer exists (splits domain, tidal via apply_proposals, pre-move layout, append-only claim)
Severity 1, doc-drift, confirmed; area review-data-flow.
- Evidence: pipeline/regs/review/export.py:37-41 has no "splits" domain (T434) and apply.py:43 PROPOSAL_DOMAINS = ("dfo","entries"), but apply.py:20 and export.py:13 and write_through.py:11 still list splits; apply_proposals.py:7 says tidal lands through it (T326 moved tidal to a file writer). pipeline/docs/HANDOFF-curated-layout.md is dated 2026-09-02 ("Nothing has been moved") and names waters/splits.json and pipeline/splits.json; the file is waters/cuts.json now. data/curated/review/README.md:5 says marks.jsonl is never edited by a tool; commit 1d58354b3 edited a line.
- Example: `python -m pipeline.regs.review.apply splits` is documented in apply.py's table but argparse rejects it (unknown domain).
- Second opinion: Minor line offsets (apply.py table is line 19, PROPOSAL_DOMAINS line 41). HANDOFF-curated-layout.md was deliberately kept and moved up by T335 (a40989161) as a cited historical doc, so it needs a "historical" banner rather than counting as live drift. The README append-only line conflicts with an owner-approved exception rather than an unnoticed breach. Docs only; no data effect.
- Proposed fix: Fix the four docstrings, mark HANDOFF-curated-layout.md as historical (or delete it), and state the real marks.jsonl rule (append-only except diff-proven id renames) next to a test that enforces it.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
The app shows the FWA vintage, not the local file date; access layers carry a date. Overlapping no-access polygons are unioned before the share is summed. fetch_data fails on an Overpass remark or an empty answer. The listed stale docs and comments are corrected or marked historical.

Depends: —. Size: S. Owner: no.

## Done / left (2026-11-10)
1. 9b5edac88 — fetch_data records each layer's SOURCE day in `bc_fisheries_data.source.json` (`pipeline.common.source_dates`; FWA from the object store's s3b-last-modified, probed: 2024-03-26); `sources_block` dates the FWA from it, never `last_change`; `source_pins pin` copies it (`layer_dates`). A release built now states fwa 2024-03-26, past the app's 730-day rule. LEFT: an `access` source key — `SOURCE_KEYS` feeds `pack.generated.ts` and the app's freshness list (app lane); the access-layer days are already in the sidecar.
2. 36d196b7f — `section_no_access` (union share, ≤1000) is what `mostly_inside` reads; `write` refuses a bundle where the summed per-area rule (app, paint_sync) and the union disagree. Live tiles: 7 summed >1000, union max 1000, 0 disagree. LEFT: the app (`core/access.ts`) and the release format still sum per-area rows — ship the union share to the app.
3. 56a9d60ef — Overpass remark / no elements = OverpassPartial (retried, then fails); empty results fail; the named area is pinned by OSM way 1166294466 (was labelled a relation); missing BC boundary and a failed roster fail; row counts recorded and `source_pins pin` refuses a >10% drop without `--accept-drop`.
4. 6326e3264 — promote.py, build.py, vintage.py reworded. LEFT: `pipeline/regs/review/gate.py:13` (review lane; pending in test_doc_drift).
5. 3488e571d — ReachCache/entry_key deleted; cache.py is the POLICY_VERSION changelog; REACH-BUILDER.md rewritten from the code; AGENTS 20 updated.
6. df0b0d0d7 — 15-live-data-flow.md gauges section, cuts.py/review.py/build.py/bundle_tables.py/check_curated.py paths, schema.sql + climatology.py wording; empty pipeline/atlas/gauges removed. LEFT: `app/packages/core/src/staleness.ts:66` ("every run"; app lane).
7. 41ecaabb5 — both READMEs and MAP.md say audit.py was deleted in T436 and how to restore it.
8. 247112b44 — HANDOFF-curated-layout.md marked historical. LEFT (review lane): apply.py/export.py/write_through.py/apply_proposals.py docstrings, data/curated/review/README.md marks.jsonl rule + its test.

## Owner answer 2026-11-11
The union no-access figure goes to the app with the next release format change, not on its own. The build check stays meanwhile.
