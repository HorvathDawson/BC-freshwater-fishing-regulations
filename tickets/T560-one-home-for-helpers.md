# T560 — One home for shared helpers: id slug, name key, live release reader, atomic write, cut tolerances

Status: PARTIAL — items 2 and 6 (Python) done (cc50a2379, 63a3e6173); items 3 and 5 done (0b23c1403, built in `full_next` 2026-11-12); left (Claude): items 1, 4, 7, 8 (item 6's JS half is done: ff787ac34, confirmed 2026-11-12)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T560 | One home for shared helpers: id slug, name key, live release reader, atomic write, cut tolerances | PARTIAL — left (Claude): items 1, 4, 7, 8 (item 6 both halves done) | cc50a2379, 63a3e6173, ff787ac34, 0b23c1403 | item 2 (paint sync reads the encoder's twin + the build's threshold), item 6 Python half (`manifest.live`, 8 readers); items 3 (`wsc.code_owner`) and 5 (`tolerances.meets_at`, one … (detail: ticket file) |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 16 of 17; highest severity 2 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Done
- Item 2 (cc50a2379): `paint_sync` takes `access.MOSTLY_INSIDE_PERMILLE` and reads the status half with `status_index.Index`; a test pins the threshold to the build's.
- Item 6: Python half (63a3e6173) `manifest.live(root)` (LATEST required, must name a folder with a manifest, no mtime fallback), eight readers moved, `test_release_latest.py` refuses a new hand read; `pipeline release`'s own `read_pointer` unchanged. JS half (ff787ac34, T601 item 2): `conformance/src/latestRelease.mjs`.
- Item 3 (0b23c1403): `common.utils.wsc.code_owner` is the one answer for names.py and `registry.build._wsc_gnis_map`; first-met == owner on all 11,580 codes (9 multi-gnis).
- Item 5 (0b23c1403): `common.tolerances.meets_at` (`SAME_PLACE_M`) at all three sites; the area slug is one function, `common.split_ids.readable_slug` (area_catalog, registry, review split_use). Built in `full_next`, no section/boundary/sidecar change; tests `pipeline/tests/test_next_atlas_items.py`.

## Left (items 1, 4, 7, 8)
### 1. Pipeline has six private copies of the id slug rule (and none folds accents)
Severity 2, duplication, confirmed; area app-logic-parity.
- Evidence: Same regex re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_") in: pipeline/common/split_ids.py:20 (slug, cut to 60), pipeline/atlas/registry/build.py:31 (_slug), pipeline/atlas/splits/area_catalog.py:25 (_slug, builds area:{kind}:{slug} ids at :29-31), pipeline/regs/parsing/batch_exporter.py:37 (adds `or "row"`), pipeline/regs/parsing/catalogue_parts/licensing.py:220, pipeline/deliver/answers/glossary.py:510; hyphen variant in pipeline/regs/review/tickets.py:42. Only review/proposals.py:72 delegates to split_ids. None uses pipeline/common/names.py:54 fold (NFKD), unlike the app's slugOf (core/src/gaugePage.ts:12-15).
- Example: area_id("watershed", "Écho Creek") gives area:watershed:cho_creek in Python, while the app/site slug of the same name is echo-creek.
- Second opinion: Undercounted: at least ten copies, and the cited lines are each one too low. The accent loss is already in curated data, not hypothetical: data/curated/regulations/synopsis/region-6.json:2525 has entry_id "r6:fran_ois_lake@6-4" and :8160 split "stellako_river__fran_ois_lake", while data/curated/waters/cuts.json:8238 has "..._francois_lake_road_bridge" and :9685 "trepanier_river__hwy_97c_bridge". So the same lake is spelled two ways across stored ids. These ids are internal and stored; I found no wrong binding from it, and any fold change needs an id migration (entry_id encodes the name).
- Proposed fix: One pipeline/common slug (fold then regex), the others import it; a test that greps for the regex outside that module. Check existing curated ids before changing fold behaviour (ids are stored).

### 4. Name keys and id slugs still exist in several hand copies after T61 2.8 (one key)
Severity 1, duplication, partly; area static-layers. Related ticket: T61 (DONE; copies remain).
- Evidence: THE key is pipeline/common/names.py:59 (accent fold, apostrophes deleted). ASCII-only copies that do NOT fold accents: pipeline/regs/review/names.py:76-78 (builds candidate ids stored in found_name_calls.json and marks), docs/curation-review/name-variants/mine.py:63-67 (same name `key`, but also folds mt->mount, st->saint), pipeline/tools/audit/tidal_preverify.py:70, tidal_candidates.py:189, curation-review/backend/app.py:1269 and 1276. The area slug is three byte-identical copies: pipeline/atlas/splits/area_catalog.py:25, pipeline/atlas/registry/build.py:31, pipeline/regs/review/split_use.py:31 (its docstring says it mirrors area_catalog._slug). The split slug has one home (common/split_ids.py:20) but pipeline/regs/dfo_salmon/splitwork.py:394 re-spells it.
- Example: "Barrière Lake": names.key -> "barriere lake"; the ASCII copies -> "barri re lake". mine.py key("St. Mary") = "saint mary" while review/names.py key = "st mary", though the second builds the ids for the first's rows.
- Second opinion: Copies exist as cited, but impact is overstated. review/names.key only forms the id suffix from the row's own `proposed` text and is never joined against mine.py's key, so the St./saint difference breaks no join. The app.py copies are a local prefix match of row-box water names for a page image; the two audit tools are by-hand reports. splitwork.py:394 `_short` strips a water prefix for display, it does not mint a split id. The one copy set with real drift risk is the three area slugs, since split_use.area_slug must reproduce registry area ids.
- Proposed fix (the area-slug half is done: `readable_slug`, item 5): Import common.names.key in the audit tools and app.py. Move the area slug to one function in common (like split_ids) imported by all three. Keep review/names.key as the frozen id function but rename it (e.g. candidate_slug) and pin it with a test over the existing ids; make mine.py import it.

### 7. Atomic write is re-implemented in six places and some curated/published writers skip it
Severity 1, fragility, partly; area cross-duplicates.
- Evidence: Temp+replace copies: pipeline/regs/parsing/io.py:367 atomic_write; curation-review/backend/writes.py:150-164; pipeline/deliver/release/manifest.py:69-74,80-86; pipeline/regs/dfo_salmon/live.py:288 and fetch.py:381; pipeline/regs/inseason/resolve.py:417-418. Plain write_text straight onto files people or the app depend on: pipeline/gauges/review.py:181 (the hand gauge review file, REVIEW_FILE), pipeline/regs/inseason/publish.py:149 (matches.json to be uploaded), pipeline/gauges/matches.py:110 and :214, pipeline/gauges/feed/publish.py:530,898,950 (Python feed files), pipeline/regs/dfo_salmon/locations.py:283,290.
- Example: The gauge review page saves through gauges/review.py:181: an interrupt mid-write leaves a truncated curated JSON, and the next match/build fails to load it (git restore needed, unsaved decisions lost).
- Second opinion: The example is not supported: I found no non-test caller of gauges/review.py save() (match.py, promote.py and atlas/build.py only load), and pipeline/common/curated.py:191 says match_calls 'is never written by a program', so no review page saves through it today. The plain writers are small single-call JSON writes of git-tracked or regenerable files, so the risk is low. The only one that feeds an upload is inseason/publish.py:149 (matches.json).
- Proposed fix: One helper in pipeline/common (move io.atomic_write there), used by every writer of a curated or published file; a grep test that `write_text(json.dumps` does not appear under gauges/, regs/inseason/, regs/review/ outside the helper.

### 8. Cut-distance tolerances restated as literals beside tolerances.py; 'same cut' means 5, 10 and 25 m in different stages
Severity 1, duplication, confirmed; area cross-duplicates.
- Evidence: pipeline/common/tolerances.py:38 SLIVER_M = 5.0 is imported by sliver_gate.py:25, clean_cut.py:176, close_pairs.py:20, registry/regions.py:33. Literal copies: pipeline/atlas/splits/anchors.py:408 SAME_ENTRY_M = 5.0 (its comment says it is SLIVER_M), braid_sweep.py:68 _SELF_M = 5.0, added_streams/build_dataset.py:398 _MOUTH_TIE_M = 5.0. Review side: pipeline/regs/review/marks.py:1092 SAME_CUT_M = 10.0 (two proposals are one cut), proposals.py:743 TIDAL_REUSE_M = 25.0 (a tidal point IS an existing cut), sectionizer.py:91 PROPOSAL_M = 25.0. Name clashes with other meanings: prune.py:298 TOUCH_M = 1.0 vs tolerances TOUCH_M = 60; answers/display/part.py:369 SAME_PLACE_M = 200 vs tolerances 1e-6.
- Example: Two proposed cuts 7 m apart on one line: marks.py treats them as one cut (under 10 m); the atlas would accept both (over the 5 m sliver gate). A tidal point 20 m from a cut is reused, a non-tidal one is a second cut.
- Second opinion: Three paths are wrong: build_dataset.py is pipeline/atlas/waters/added_streams/build_dataset.py:398, sectionizer.py is pipeline/atlas/splits/sectionizer.py:91 (PROPOSAL_M = 25.0), and prune.py is pipeline/atlas/graph/prune.py:298. The 25 m tidal reuse is a documented, deliberate choice (proposals.py:740-742), not drift. No wrong output today; this is a maintenance hazard only.
- Proposed fix: Import SLIVER_M in anchors.py/braid_sweep.py; put SAME_CUT_M and the 25 m reuse distance in tolerances.py with one sentence each on why they differ; rename the two clashing local constants.

## Work
Fix items 1, 4, 7, 8 in severity order, one commit per item where separable. Item 1 is stored ids: needs a migration map. Anything found and not done becomes a new ticket.

## Acceptance
One slug function in pipeline/common used by all ten sites (accent folding decided once; existing ids such as fran_ois_lake kept by an explicit map, no id changes). One release reader with the manifest check. One atomic-write helper. Tolerances read from tolerances.py; a guard test fails on a new private copy.

Depends: —. Size: M. Owner: no.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
item 2 (paint sync reads the encoder's twin + the build's threshold), item 6 Python half (`manifest.live`, 8 readers); items 3 (`wsc.code_owner`) and 5 (`tolerances.meets_at`, one area slug) built in `full_next` 2026-11-12, no binding change
