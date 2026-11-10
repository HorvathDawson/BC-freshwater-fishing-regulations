# T547 — Atlas records the curated inputs it was built from; a release refuses an atlas older than its cuts, names or areas

Status: OPEN.

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 3 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. The promoted atlas records none of its curated inputs, so a release can ship cuts, names or areas edited after the atlas was built
Severity 3, missing-check, confirmed, reaches an angler; area atlas.
- Evidence: data/generated/atlas/full/graph.pkl.meta.json holds `"input_hashes": {}`: pipeline/common/io/serialize.py:16-22 supports hashes, but pipeline/atlas/build.py:1370-1371 passes none. The only recorded inputs are the gauge cut and match shas (build.py:1631-1635, checked by gauges/matches.py:221). The cut anchors are recorded in split_anchors.json (atlas/splits/splits.py:113-117) but read only by the review tool (regs/review/vintage.py:414-422). pipeline/release.py and pipeline/build.py have no reference to it; stage keys cover the atlas's handles and registry, not whether it is current. check_curated --waters only warns about name-variant targets.
- Example: Today cuts.json (453 splits) matches split_anchors.json exactly: 0 new, 0 gone, 0 moved (checked). The check would pass now; nothing would say so on the day it does not.
- Second opinion: Stands. Partial mitigations the finder left out or underweighted: a rule naming a cut id the old atlas lacks surfaces as an unresolved extent in the reach run, and the review app shows 'awaiting build' for cuts. Neither covers a moved cut with the same id, a name variant, an added water or an area edit. I did not re-run the finder's cuts.json vs split_anchors.json comparison.
- Proposed fix: Record a digest per atlas input (cuts, name_variants, added_*, area_closures, tidal_limits, found_names, slough_calls, FWA stamp) in the atlas. Have `pipeline release` preflight compare them with the tree and stop, naming the files, unless told to ship without them.

### 2. The promoted atlas records nothing about the curated files it was built from, so a release can ship an atlas older than slough calls, found names, name variants and added waters
Severity 3, missing-check, confirmed, reaches an angler; area static-layers.
- Evidence: pipeline/atlas/build.py reads CURATED.waters.found_names (:618), tidal (:818), slough calls, name_variants and added lakes/streams, but writes no digest of them (only handles and gauge-match digests: :455-456, :1488). pipeline/common/vintage.py pins tiles+bundle to one atlas (handles, registry) only. pipeline/build.py:162-200 hashes these files into the DELIVER key (comment :154: found_names.json "IS hashed"), yet no deliver code reads them (only atlas/*, grep of `waters.found_names|slough_calls|name_variants`), so a change reruns deliver on the old atlas and the result looks fresh. Guards exist only for cuts (atlas/sidecars.py:29) and per-entry splits (regs/review/vintage.py).
- Example: Today they agree (slough_fold_report.json confirmed 811 = slough_calls.json 811 rows; tidal.resolved.json 1,169 rows = tidal_limits.json 1,169). After the next confirmed slough or accepted name, `pipeline release --staging` would pass every check without it.
- Second opinion: Accurate. One softening: tidal has a cheap catch-up path (python -m pipeline.atlas.sidecars --tidal-only rewrites tidal.resolved.json for a built atlas), but nothing detects that it is needed. The build docstring's "not deterministic" reason is itself stale (memory: the atlas build is deterministic).
- Proposed fix: Have the atlas build write `atlas.inputs.json` (sha256 of each curated/source file it read). Add a cheap check to `pipeline build` and release preflight that compares it with the files now and names each one that moved ("atlas is behind slough_calls.json: rebuild with --atlas"). Drop atlas-only files from the deliver key.

### 3. A lake boundary's clash suffix (_2, _3) follows node iteration order, and a curated rule binds through one
Severity 3, fragility, partly, reaches an angler; area atlas.
- Evidence: pipeline/atlas/registry/build.py:392-417: auto (lake) boundary ids are minted as `{item_slug}__{slug(label)}` while looping `for n in nodes: for end in (lower_bound, upper_bound)`; on a clash lines 412-413 append `_2`, `_3` in first-seen order. `nodes` is the group's order in `graph.nodes` (lines 311-313), which a new cut or re-sectioning can change. Nothing ties the suffix to the lake's wbk. The live registry has 217 auto boundary ids ending in a clash suffix. Both ids keep existing after a swap, so no resolver, parity report or gate would notice.
- Example: Nanaimo River: `nanaimo_river__nanaimo_lakes` = lake:329101304, `nanaimo_river__nanaimo_lakes_2` = lake:329101307. data/curated/regulations/synopsis/region-1.json:8843-8851 binds a fly-only rule `upstream_of nanaimo_river__nanaimo_lakes_2`; its review_reason says it means wbk:329101307 (Second Nanaimo Lake).
- Second opinion: The mechanism and the example are real. Overstated: 'no parity report would notice'. build_parity's fingerprint includes (id, ref), so a swap marks the item as 'changed' in the promote dry run; but that is only a count among all changed items, printed and never gating, so it is easy to miss. Not shown: that node order within an item actually changes on a rebuild. The build is deterministic for the same inputs (T65); a reorder after a new cut on that river is plausible but unproven. Only one curated rule was found binding through a suffixed lake id.
- Proposed fix: Number clashes deterministically by the boundary itself (sort by route measure or wbk), or put the wbk in the id on a clash. Add a build check that every auto boundary id a curated rule names keeps the same `ref` (lake:wbk) as the previous atlas; fail promote if one moved.

### 4. Atlas promote accepts a half-written build, swaps with two bare renames, and never blocks on its parity report
Severity 2, fragility, partly; area atlas.
- Evidence: pipeline/atlas/promote.py:42-44 calls a candidate finished when section_handles.txt and registry.json exist. Both are written in `build_registry` (pipeline/atlas/build.py:1480-1488), before `write_sidecars` (1493-1637) writes region_home.json, splits.resolved.json, tidal.resolved.json, split_anchors.json, item_points.json and the gauge-cut record. promote.py:70-75 does rmtree(prev), target.rename(prev), candidate.rename(target) with no rollback. The build_parity result (48-53) is only printed, including its '!! CUT-POINT(S) LOST' line. pipeline/build.py:678 builds into an existing `<build>_next` without clearing it.
- Example: A `full_next` whose build died in write_sidecars, or one holding an older run's registry and handles beside a newer graph.pkl, passes `python -m pipeline.atlas.promote full_next`.
- Second opinion: The code is as described, but reach is narrower. Through `pipeline build --atlas --promote` a failed atlas stage stops before promote, so a half-written candidate is promoted only by a direct `python -m pipeline.atlas.promote` or a reused `_next`. gauge_matches.json is the last file written, and the bundle refuses an atlas without it, so a fresh build that died in write_sidecars is caught downstream (late, after the swap). The real gaps: stale sidecars in a reused `_next`, no rollback between renames, and a cut-loss report that never blocks. Two same-volume renames failing midway is unlikely.
- Proposed fix: Have the atlas build write a final COMPLETE marker (inputs digest plus the list of files written) as its last step and clear `_next` at start. Promote requires the marker, restores `prev` if the second rename fails, and refuses on net boundary loss unless given an explicit accept flag.

### 5. "A lake must not answer to its river" only checks rivers' primary names; five lakes still answer to West Road River and Blackwater River
Severity 2, wrong-or-missing-data, confirmed, reaches an angler; area atlas.
- Evidence: pipeline/atlas/registry/build.py:322-326 builds `stream_primary_names` from `_primary_name` only. Lines 355-371 drop a still water's flowing name only when it is in that set. A river whose plain name is a variant, not its primary, is missed: gnis:26104's primary is 'West Road (Blackwater) River', so 'West Road River' and 'BLACKWATER RIVER' (which FWA hangs on the lakes it threads) stay on the lakes.
- Example: Live registry: wbk:329126691 Tsacha Lake has variants ['BLACKWATER RIVER', 'Tsacha Lake', 'West Road River']. The same holds for Kluskoil Lake (329047266), Tundra Lake (329126937) and Euchiniko Lakes (329126692, 329126696). The name key 'blackwater river' resolves to 6 items. These were the only 2 such keys found province-wide.
- Second opinion: Confirmed in code and data. Small trims: 'West Road River' was seen on Kluskoil, Tsacha and Tundra; I did not confirm it on both Euchiniko items. I did not re-check that these are the only two such keys province-wide, nor the book_name_links line numbers (the file is not at data/curated/waters/). Effect is search ambiguity, not a wrong regulation.
- Proposed fix: Compare against every stream item's own names (primary plus own-gnis variants), not just primaries. Add a registry test: no lake or wetland variant whose head noun flows may equal any name of a stream item.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
Editing a cut, a name variant or an area after the atlas build makes release preflight stop and name the file. Lake-boundary clash suffixes are ordered by the boundary itself; a promote check fails if a boundary id named by a curated rule changes its lake. The five lakes no longer answer to West Road / Blackwater River (needs an atlas rebuild).

Depends: —. Size: M. Owner: no.
