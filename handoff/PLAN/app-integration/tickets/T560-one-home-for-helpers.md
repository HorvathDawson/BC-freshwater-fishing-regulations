# T560 — One home for shared helpers: id slug, name key, live release reader, atomic write, cut tolerances

Status: PARTIAL — items 2 and 6 (Python) done (cc50a2379, 63a3e6173); left (Claude): items 1, 3, 4, 5, 7, 8 (item 6's JS half is done: ff787ac34, confirmed 2026-11-12)

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 16 of 17; highest severity 2 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. Pipeline has six private copies of the id slug rule (and none folds accents)
Severity 2, duplication, confirmed; area app-logic-parity.
- Evidence: Same regex re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_") in: pipeline/common/split_ids.py:20 (slug, cut to 60), pipeline/atlas/registry/build.py:31 (_slug), pipeline/atlas/splits/area_catalog.py:25 (_slug, builds area:{kind}:{slug} ids at :29-31), pipeline/regs/parsing/batch_exporter.py:37 (adds `or "row"`), pipeline/regs/parsing/catalogue_parts/licensing.py:220, pipeline/deliver/answers/glossary.py:510; hyphen variant in pipeline/regs/review/tickets.py:42. Only review/proposals.py:72 delegates to split_ids. None uses pipeline/common/names.py:54 fold (NFKD), unlike the app's slugOf (core/src/gaugePage.ts:12-15).
- Example: area_id("watershed", "Écho Creek") gives area:watershed:cho_creek in Python, while the app/site slug of the same name is echo-creek.
- Second opinion: Undercounted: at least ten copies, and the cited lines are each one too low. The accent loss is already in curated data, not hypothetical: data/curated/regulations/synopsis/region-6.json:2525 has entry_id "r6:fran_ois_lake@6-4" and :8160 split "stellako_river__fran_ois_lake", while data/curated/waters/cuts.json:8238 has "..._francois_lake_road_bridge" and :9685 "trepanier_river__hwy_97c_bridge". So the same lake is spelled two ways across stored ids. These ids are internal and stored; I found no wrong binding from it, and any fold change needs an id migration (entry_id encodes the name).
- Proposed fix: One pipeline/common slug (fold then regex), the others import it; a test that greps for the regex outside that module. Check existing curated ids before changing fold behaviour (ids are stored).

### 2. The parts-vs-paint checker carries its own copy of the no-access threshold and of the status-index layout
Severity 2, duplication, partly; area deliver-contract.
- Evidence: pipeline/deliver/bundle/access.py:280 `MOSTLY_INSIDE_PERMILLE = 500` is the build's rule, pinned to the app's `MOSTLY_INSIDE` by pipeline/tests/test_section_access.py:159-163. pipeline/deliver/release/paint_sync.py:39 re-types `NO_ACCESS_PERMILLE = 500` and uses it at 145; nothing pins it to access.py. paint_sync.py:46-73 `status_profiles` also hand-parses the BCSI v2 header (skip 5+8+8, profiles, three columns) instead of a decoder owned by pipeline/deliver/status_index.py, whose docstring is the only definition of the layout.
- Second opinion: The duplicated threshold is confirmed and unpinned (grep finds no test naming NO_ACCESS_PERMILLE). The layout half is overstated: status_profiles refuses any status half that is not BCSI version 2 (line 48-49), so a version bump stops the gate loudly; only a layout change made without bumping VERSION would be misread. The added point is that its only test builds the bytes by hand, so it never meets the real encoder. Release-gate tooling only: it cannot change what an angler sees by itself.
- Proposed fix: paint_sync imports `MOSTLY_INSIDE_PERMILLE` from access.py; add `status_index.decode(bytes)` beside `encode` (round-trip tested) and have paint_sync and paint.decode call it.

### 3. Which river owns a shared watershed code is decided twice: highest magnitude in the graph names, first-seen in the registry
Severity 2, divergence, partly; area atlas.
- Evidence: pipeline/atlas/graph/names.py:186-192: an unnamed channel inherits from `max(siblings, key=_mag)` on its code, and only when that sibling is strictly bigger. pipeline/atlas/registry/build.py:35-42 `_wsc_gnis_map` answers the same question with `m.setdefault(trim_wsc(n.wsc), n.gnis_id)`, the first gnis met in `graph.nodes` order. `_effective_gnis` (45-53) and `item_id` (56-65) use it for every node that has no gnis and no inherited tuple. No test names `_wsc_gnis_map`.
- Example: Live registry: 9 watershed codes are shared by more than one gnis stream item. wsc:100 is shared by the Fraser River (gnis:39325) and 13 other gnis items (Nicomen Slough, Annacis Channel, Maria Slough, ...); wsc:300-625474 by the Kootenay River and two named channels.
- Second opinion: Two rules for one question is real. Impact is overstated: the registry's first-seen rule is only a last fallback, reached by a no-gnis node with no inherited tuple, i.e. mainly an unnamed chain at least as big as every named sibling on its code. Ordinary unnamed Fraser side channels inherit by magnitude and never reach it. The finder showed no node that actually takes the fallback on a multi-gnis code, and node order is stable for the same inputs (T65), so 'Fraser pieces move into a slough' is unproven. I did not re-count the 9 shared codes (registry is over the size limit to load).
- Proposed fix: Give one function the job (highest magnitude, tie broken by lowest blk) and have both names.py and the registry call it. Add a test on a two-gnis code, and have the build report any code where the two rules would disagree.

### 4. Name keys and id slugs still exist in several hand copies after T61 2.8 (one key)
Severity 1, duplication, partly; area static-layers. Related ticket: T61 (DONE; copies remain).
- Evidence: THE key is pipeline/common/names.py:59 (accent fold, apostrophes deleted). ASCII-only copies that do NOT fold accents: pipeline/regs/review/names.py:76-78 (builds candidate ids stored in found_name_calls.json and marks), handoff/PLAN/curation-review/name-variants/mine.py:63-67 (same name `key`, but also folds mt->mount, st->saint), pipeline/tools/audit/tidal_preverify.py:70, tidal_candidates.py:189, curation-review/backend/app.py:1269 and 1276. The area slug is three byte-identical copies: pipeline/atlas/splits/area_catalog.py:25, pipeline/atlas/registry/build.py:31, pipeline/regs/review/split_use.py:31 (its docstring says it mirrors area_catalog._slug). The split slug has one home (common/split_ids.py:20) but pipeline/regs/dfo_salmon/splitwork.py:394 re-spells it.
- Example: "Barrière Lake": names.key -> "barriere lake"; the ASCII copies -> "barri re lake". mine.py key("St. Mary") = "saint mary" while review/names.py key = "st mary", though the second builds the ids for the first's rows.
- Second opinion: Copies exist as cited, but impact is overstated. review/names.key only forms the id suffix from the row's own `proposed` text and is never joined against mine.py's key, so the St./saint difference breaks no join. The app.py copies are a local prefix match of row-box water names for a page image; the two audit tools are by-hand reports. splitwork.py:394 `_short` strips a water prefix for display, it does not mint a split id. The one copy set with real drift risk is the three area slugs, since split_use.area_slug must reproduce registry area ids.
- Proposed fix: Import common.names.key in the audit tools and app.py. Move the area slug to one function in common (like split_ids) imported by all three. Keep review/names.key as the frozen id function but rename it (e.g. candidate_slug) and pin it with a test over the existing ids; make mine.py import it.

### 5. The 'two pieces meet at this cut' test is written three times with two tolerances; the area slug is written twice
Severity 1, duplication, partly; area atlas.
- Evidence: Severed-at-a-cut predicate: pipeline/atlas/reach/extent.py:356-362 (`_across_the_cut`) and 680-691 (`_severed`) use 0.001 m; pipeline/atlas/splits/braid_sweep.py:969-974 (`severed`) uses 1e-6 and omits the principal-line exclusion. pipeline/common/tolerances.py declares SAME_PLACE_M = 1e-6 for exactly this and none of the three imports it. Area id slug: pipeline/atlas/registry/build.py:31-32 `_slug` (mints `area:{slug}` in the area-items pass) and pipeline/atlas/splits/area_catalog.py:25-26 `_slug` (mints `area:{kind}:{slug}`) are byte-identical copies.
- Second opinion: The duplication is there; the impact is overstated. Both sides of a cut get their measure from the same split point, so a join 1e-6 to 0.001 m apart is not something the finder showed can occur. The two slugs mint different id families (`area:{slug}` at build.py:438, which passes any ready-made `area:` id through unchanged, versus `area:{kind}:{slug}` in the catalog), so one drifting would not by itself split catalog ids from registry ids. This is tidiness with low risk, no example from data.
- Proposed fix: Put one `meets_at(graph, a, b)` beside tolerances.py and use it at all three sites. Import one slug (common/split_ids or a shared area_id) in both area sites.

### 6. 'Which release is live' is read seven ways; some fall back to newest mtime, some skip the manifest check
Severity 1, duplication, partly; area cross-duplicates.
- Evidence: Canonical: pipeline/deliver/release/manifest.py:88-101 latest() (checks the named folder has a manifest; falls back to newest mtime when LATEST is absent). Copies: pipeline/release.py:103 read_pointer; pipeline/regs/inseason/resolve.py:58-62 release_dir; pipeline/regs/inseason/publish.py:130; pipeline/deliver/answers/display/name_check.py:230; pipeline/runtiming/feed/base.py:309 and feed/release.py:90 (raw read_text, no manifest check); app/tools/serve-tiles.mjs:84-88 (own mtime fallback, RELEASE env override); app/tools/regs-recut/sync.mjs:20.
- Example: LATEST naming a pruned or half-copied folder: latest() stops with 'names X, which has no manifest'; runtiming base.py:309 and inseason resolve.py:62 go on with the bare path and fail later or read a partial release.
- Second opinion: The copies exist, but the divergence is overstated. Only latest() and serve-tiles.mjs fall back to mtime, and only when LATEST is absent; in that case every raw Python reader raises on read_text, so they cannot quietly act on a different release. The real gap is the missing manifest check in the raw readers, which then fail later on a missing file rather than read wrong data. inseason publish.py:131 also refuses when the matches name another release. Developer-side only.
- Proposed fix: All Python readers call manifest.latest(); remove the mtime fallback outside tests (make it an explicit argument); serve-tiles.mjs errors when LATEST is missing instead of picking by mtime.

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
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
One slug function in pipeline/common used by all ten sites (accent folding decided once; existing ids such as fran_ois_lake kept by an explicit map, no id changes). One release reader with the manifest check. One atomic-write helper. Tolerances read from tolerances.py; a guard test fails on a new private copy.

Depends: —. Size: M. Owner: no.

## Done 2026-11-12 (pipeline lane)
- Item 2 (cc50a2379): `paint_sync` takes `access.MOSTLY_INSIDE_PERMILLE` and reads the status half with
  `status_index.Index` (the encoder's twin); its test builds the bytes with `status_index.encode` and refuses another
  version and trailing bytes. A test pins the threshold to the build's.
- Item 6, Python half (63a3e6173): `pipeline.deliver.release.manifest.live(root)` — LATEST required, it must name a folder
  with a manifest, no modified-time fallback. Eight hand readers moved onto it (inseason resolve + publish, name_check,
  runtiming base + release, dfo_salmon live, bathymetry propose + contours); `test_release_latest.py` refuses a new hand
  read of the pointer anywhere under `pipeline/`. `pipeline release`'s own `read_pointer` is unchanged (it handles a
  missing pointer as a stop). JS half (serve-tiles.mjs mtime fallback, regs-recut/sync.mjs) left for the app wave.
- Not done, by size: item 1 (slug + accent fold: stored ids, needs a migration map), item 3 (one wsc owner: changes the
  atlas, measure on the next rebuild), items 4, 5, 7, 8 (severity 1 tidiness; item 5's tolerance unification can move a
  cut decision, so it goes with an atlas rebuild).

## 2026-11-12 (app wave)
Item 6's JS half is done and was already in the tree (ff787ac34, see T601 item 2): `serve-tiles.mjs`, `regs-recut/sync.mjs` and `label-shots.mjs` read `LATEST` through `conformance/src/latestRelease.mjs` (no modified-time fallback; a pointer to a folder without a manifest throws). Left: items 1, 3, 4, 5, 7, 8 (Python / pipeline).
