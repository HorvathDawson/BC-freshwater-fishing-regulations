# T679 — Each decision in its own stage (everything but tidal)

Status: OPEN (audit and plan; no code changed)

Source: owner 2026-11-11, about tidal: "we want to avoid it being a weird post processing step", "the cuts used for
tidal should happen where cuts happen etc.", then: "Look for other things like that too". Tidal itself is T678; this
ticket is every other stage. It mentions tidal only where another topic touches it.

Read-only audit of the code on `redesign/stream-sections` at 6c3bc9588. Nothing was built, run or measured. A count
written as "about" comes from a code comment, not a measurement.

## Owner answers (2026-11-11) - current
- **D1 (Q154) - unnamed widening pond on a river:** answered, river for both. Whole-river rules and "in streams" rules
  both apply on it (`reach/extent.py:826`). Finding 5 is therefore a fix (registry folds the pond into the river),
  no longer a question.
- D2-D6: still open (listed below, with defaults).

## Short answer
The atlas is in good shape. Every cut goes through `sectionizer.split_graph_at`. Both prunes run once, after naming
and before any cut. Water kind is decided once, in the registry. The problems are later:
- the reach and the tiles decide "outside B.C." with different rules;
- the bundle places gauges again instead of reading the atlas's gauge cuts;
- some curated facts (DFO places, added-stream fixes, export lists) live in Python;
- the app re-derives a few things the pipeline already resolved (salmon status, own-or-donor gauge).

That is 18 real findings: 9 can change what the angler sees, 9 are maintenance only. Separately, 6 items would change
how a regulation is READ and are listed for the owner, not planned.

## Findings, ranked by risk to what the angler sees

Risk words:
- **wrong** — a wrong answer can be shown.
- **disagree** — two surfaces can silently differ.
- **maint** — maintenance only.

| # | What | Where | Runs in | Belongs in | Risk |
|---|---|---|---|---|---|
| 1 | "Outside B.C." has two rules. The reach counts a section as outside when it has `out_of_bc` OR lies in no region polygon. The tiles drop only `out_of_bc`, and the tributary walk reads only `out_of_bc`. Border slivers are drawn and tappable, carry no rules, and a tap says "outside B.C.". They are also missing from `undrawn.json`, so `bundle/undrawn.py` cannot check them. | `atlas/reach/outside.py:48-69`; `deliver/tiles/export.py:244`; `atlas/graph/tributaries.py:988` | reach + tiles | atlas: one flag, stamped at the border cut (`splits/border.py`) | wrong / disagree |
| 2 | DFO salmon places are hardcoded in Python: `SCOPE_EXTENTS`, `PAGE_DEFAULTS`, `CNR_BRIDGE`, `gnis:2936`, `HAIDA_GWAII`. The promote guards read only curated JSON (`promote.curated_boundary_ids`, `orphaned_ids`). A rebuild that moves the CNR bridge cut or the Skeena item therefore passes promote, and B(i)/B(ii) bind wrong or go unresolved. | `regs/dfo_salmon/entries.py:353-403, 469-489`; `atlas/promote.py:59-78, 143` | DFO entry generation | curated data (`data/curated/dfo_salmon/`), read by the promote guards | wrong |
| 3 | Gauges are placed twice, with different rules. The atlas cuts at a station within 400 m, and only for stations `speaks_now` on the roster snapshot. The bundle projects the coordinate again with its own limit (`max(500, d+250)`), filters with a different liveness test (`dormant`), and never reads the `gauge__` boundary the atlas made. A seasonal station, or one matched 400–700 m off, can land on a section that does not start at the gauge. | `gauges/consume/cuts.py:37, 56`; `gauges/consume/place.py:27-36`; `gauges/consume/bundle_tables.py:146-150` | atlas + bundle | atlas (the cut is the placement); the bundle reads the `gauge__` boundary | wrong / disagree |
| 4 | Area names are cleaned twice in the bundle and not at all for the map. The bundle strips the `[OSM id]` suffix and restores a cut-off word ("RESER" → "RESERVE") in `area_display`. `access.display_name` strips the suffix again with its own regex. The admin tile layers label with `display(name)` only, so the map can say "CFB Comox [12332677]" or "… ECOLOGICAL RESER" while the card has the clean name. | `deliver/bundle/place_names.py:66-82`; `deliver/bundle/access.py:85-91`; `deliver/tiles/export.py:801`, `tiles/names.py:30` | bundle ×2, tiles none | where areas are loaded (`atlas/splits/area_splits`): one display name beside the id, which keeps its suffix | wrong (label) |
| 5 | Unnamed ponds the reach takes as a river's widening (T153) are the river for `whole`/cut extents. But `water_kind.kind_of` still calls them `lake`, so every `feature_types:[stream]` rule takes them out again. Steelhead patches around it (`A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS`), and gauges keep a switched-off copy. | `atlas/reach/extent.py:826`, `:1180`; `classify.py:338`; `steelhead.py:115, 412`; `gauges/consume/panels.py:143` | reach, per rule | registry (`registry/flowing.py` already folds flowing polygons) | wrong — **owner decision (Q154)** |
| 6 | A section of no registry water still gets a name. The tiles label it from the graph node, the bundle's span words do the same, and gazetted wetlands in no item are kept. The map names water that opens as unnamed when tapped. | `deliver/tiles/export.py:138-140, 576`; `deliver/bundle/spans.py:340, 431` | tiles + bundle | registry: every named node is in an item, or carries no name | disagree |
| 7 | Salmon status and notes exist in TypeScript too. `salmonStatusFromSpecies` re-derives the chain status from the five species verdicts. On a partial-coverage chain it differs on purpose from the map runs ("check"), so the map and the card's bar can differ. `salmonNotes` re-implements `deliver.salmon.notes_for`; a golden test holds the two equal. This goes against "cascade resolved in Python". | `app/packages/core/src/salmon.ts:351, 382, 923` | app | `deliver/salmon.py`: emit both per chain | disagree — **owner: which status is the answer on a partial chain** |
| 8 | "Own gauge or donor" is decided twice. `gaugeSide.ts` re-decides which gauges are on this water from `item_id` + `water_continue`, while `panel.py` `own_distances` decides it for the lead ranks. A gauge can be listed "on this water" without being the lead's own. | `app/packages/core/src/gaugeSide.ts:37`; `gauges/consume/panel.py` | app + bundle | bundle: an `own` flag on each panel member | disagree |
| 9 | `PROVINCE_SET_RULES = ("steelhead.r1","steelhead.r2","steelhead.r4")` and the `b`-twin naming are hardcoded rule ids; the export reuses them. If a re-parse renames one, "steelhead rules apply" goes empty silently, and every `anadromous` with it. | `atlas/reach/steelhead.py:98, 422-426`; `deliver/export/steelhead.py:84` | reach | the corpus (a flag on the rule), or at least a refusal when an id is missing | wrong (on a re-parse) |
| 10 | Two licensing passes decide who owns a walked section. `own_beats_inherited` keeps an inherited designation; `first_classified_downstream` then rehomes the same section "whatever its class". Only call order in `build.py:244-276` decides, and the first pass's outcome never reaches the output. | `atlas/reach/licensing.py:278, 358, 519-522` | reach | one pass | maint (dead diagnostic) |
| 11 | In-season matches: `regs.inseason.resolve` writes derived `waters`/ranges back into the curated `notice_waters.json`, and the release commits it on every release (`follows`). The curated file holds a generated answer. | `release.py:979`; `release_follows.py:59-60`; `regs/inseason/resolve.py` | release follows | ranges to `data/generated/`; the curated file holds only the owner's matches | maint |
| 12 | The export keeps entry-id lists because the bundle lacks the fields: `UNDRAWN_PART_TRIBUTARIES`, `EXCEPTED_RIVERS_CREEKS_CLOSED`. Owner rulings sit in code: `RELEASE_UNDER_CLOSURE_KNOWN`, `QUOTA_UNDER_CLOSURE_KNOWN`. Tests re-derive them from the corpus. | `deliver/export/closures.py:116…, 658, 666` | export | the bundle carries `tributary_excludes` and a rule's tributary scope; the rulings move to curated data | maint |
| 13 | The added-streams generator holds hand fixes in code: `RECEIVER_OVERRIDE_BY_SOURCE` (Magnolia Creek), `_EXCLUDE_SRC_IDS_BY_SOURCE` (about 40 Burnaby/Squamish pieces) and a name-typo map. They shape the reviewed `added_streams.json`. | `atlas/waters/added_streams/build_dataset.py:405`; `clean.py:170-185` | generator (off the build) | a curated `*_calls.json` beside `added_streams.json` | maint |
| 14 | "Flows into" has two answers. `nearer.py` builds `trib_nearer` from raw `item_down`, while `topology.exit_receivers` removes receivers that flow back (Fraser → Annacis/Morey Channel). On braids, the raw version can nest two waters both ways and leave a ✱ near/far pair unordered. | `deliver/bundle/nearer.py`; `deliver/bundle/topology.py:319` | bundle ×2 | one function in the bundle (or the registry) | disagree — **owner: changes which tributary row wins** |
| 15 | `splits.resolved.json` and `registry.json` have a second writer. `atlas.sidecars` rewrites them on a built atlas: labels, confidence, offsets, lake parts and `outside_area_items`. It was written as a migration and is now also the "label edit, no rebuild" path, and a review proposal points at it. The build itself does the same. | `atlas/sidecars.py:48-148`; `regs/review/proposals.py:877`; `atlas/inputs.py:20` | after the atlas | the atlas build only (or keep it, named as a supported label-only path with a test that the two writers agree) | maint |
| 16 | "Mostly inside no-access land" is decided twice: the bundle uses the union share, the app sums per-area rows. A guard refuses a disagreement. | `deliver/bundle/access.py:306, 322`; `app/…/access.ts` | bundle + app | bundle: ship one flag | maint |
| 17 | Display re-joins runs. `_by_grp` joins part runs end to end by matching `to`/`from` and km. The bundle decided `(grp, run)` already; this re-join is of runs the salmon chain cut (T517). | `deliver/answers/display/builders.py:158-188`; `bundle/derived.py:296-343` | answers | bundle: ship the joined run per group | maint |
| 18 | Small duplicates. `continuation.py` holds a topology fact, with a watershed-code check that works around added-stream orientation; it belongs in the atlas. Places are de-duplicated in tiles (3 dp) but not in the bundle (4 dp), and `build.py:413` prints a dead "dropped" count. A part has two "where is it" points (`part_geo.mid_sid` vs `release/part_line.py`). `position.nearest_pieces` returns `{}` when `attach` was not called (no caller does that today), and should raise. | `deliver/bundle/continuation.py:31, 140`; `tiles/export.py:968-972`; `bundle/build.py:378, 413`; `bundle/topology.py`; `atlas/reach/position.py:30-60` | various | as said | maint |

Touches tidal (for T678, not planned here):
- The reach applies outside and not-fishing first, then tidal (`reach/build.py:455-462`), so the typed reason a rule
  went empty depends on that order.
- `bundle/access.py` measures against tile geometry, which drops tidal pieces, so named tidal sections get no length
  or access rows.

## Owner decisions (how a regulation is READ — not plan items)
- **D1 (#5, Q154) - ANSWERED: yes, river for both.** Is a river's unnamed widening pond the river for every rule, "in streams" rules included?
- **D2 (#7).** On a salmon chain with partial coverage, is the answer "check" (map) or the species verdict (bar)?
- **D3 (#14).** Should "nearer row wins" use the receivers with flow-back removed? This can change which ✱ row speaks
  on braided tributaries.
- **D4.** The reach reads printed WORDS to decide extents. Regexes over `verbatim` decide whether a joining water at a
  confluence cut is in or out (`reach/build.py:939-998, 1162`). "(also in M.U. x-y)" in a row name widens its regions
  (`outside.py:159, 191`). `prints_steelhead` matches the word (`steelhead.py:145`). Should these be parse-time fields
  (a re-parse plus re-curation), or stay as they are?
- **D5.** DFO places resolve a confluence cut without `owned` (`reach/dfo_salmon.py:112`), so every joining water
  goes with the cut, while a provincial rule on the same cut may keep it out. Is that intended?
- **D6.** The feeds worker reads DFO rows in "robust" mode, and only the tests run `pythonParity`
  (`app/workers/feeds/src/dfo/live.ts:16-21, 335`). Should the Python read robustly too, or should the tests cover the
  robust mode?

## Plan, in order (cheapest rebuild first; each step one commit, Tier 0 + smoke)
1. **#9 refuse a missing steelhead set id** (reach rerun, no output change expected); **#18 `nearest_pieces`
   raises** (code only).
2. **#2 DFO places to curated data.** Move `SCOPE_EXTENTS`/`PAGE_DEFAULTS` to a curated file and add it to the
   promote guards' id scan. Reach rerun + bundle; the binding diff must be empty.
3. **#4 one area display name.** Give it to areas where they load. For now the tiles call the same `area_display`,
   and `access.display_name` is deleted. Tiles + bundle rebuild.
4. **#3 gauge placement from the atlas cut.** The bundle reads `split:gauge__X` and uses one liveness predicate in
   both places. Bundle rebuild. If the cut set changes, wait for the next atlas build. Diff `section_gauge` before
   and after.
5. **#8 `own` flag on panel members**, **#16 one access flag** (bundle + app). **#17** joined runs from the bundle
   (bundle + answers).
6. **#12 bundle carries `tributary_excludes` + tributary scope**; the export lists go, and the `*_KNOWN` rulings
   move to curated data (bundle rebuild).
7. **#10 one licensing pass** (reach rerun; diff designations per section, expect none).
8. **#11 in-season ranges to `data/generated/`** (pipeline + release follows; no rebuild).
9. **#13 added-stream fixes to a curated calls file** (generator only; the regenerated `added_streams.json` must be
   byte-identical).
10. **NEXT ATLAS BUILD (batch with T678):** **#1** stamp one outside-B.C. flag at the border cut (region test
    included), which the reach, tiles and walk read. Before that, only the tiles reading the reach's
    `outside_bc.jsonl` would close the gap, which adds a reach → tiles edge to `STAGE_GRAPH`. **#6** registry owns
    every named node. **#18** continuation into the atlas. **#15** decide whether the sidecar writer stays. Then a
    reach rerun and a full deliver. Diff the per-rule bindings against live (AGENTS 63) and the tiles' drawn set
    against `undrawn.json`.
11. D1 is answered, so #5 can start; after the owner answers D2–D6: #7, #14 and the D4 fields, each as its own ticket.

## Looked at and fine
- Every atlas cut (border, curated, braid, area, length, shore) goes through `split_graph_at`. Gauge and tidal cuts
  ride in as split defs.
- The braid prune and leaf prune run once, after naming and before any cut. No `protected_blks` remain.
- Reach-qualified name variants are applied after the cuts on purpose (7 of 3,663 need a boundary).
- Water kind is decided once, in the registry (`test_one_water_kind.py`). `not_fishing` is stamped once by the atlas.
- Region membership has one function (`regions.region_members`), which reach, steelhead twins and the bundle use.
- `bundle/undrawn.py` checks the tiles' choices and decides none of them. Tiles-before-bundle order is enforced
  (`undrawn.load` counts, the `access.write` digest).
- `store/`, `release/` (shards, paint, part_line, gone_ids) only package data and refuse mismatches.
- `release_follows.py` re-cuts fixtures on the candidate, enforced by the ledger. `atlas/promote.py` renames and
  guards, and patches nothing. `STAGE_GRAPH` order is explicit.
- `OWNER_APPROVED_FWA_EXCLUDE` (empty) is a refusal, not a patch. `steelhead_waters.json` is curated data, with its
  generator's `HAND`/`REVIEW` documented as reviewed (AGENTS 54).
- App `core/regs/*` only reads and words stored answers. `waterNav.mergeNav` joins on live salmon as the owner ruled
  (T517). `map/colouring.ts` only paints.
- The feeds worker's gauge port has Python as its oracle; `trust.ts` mirrors `panel.py` under tests. Live readings
  justify running it on the device (maint at most).

Depends: T678 (share its atlas build). Size: L (split per plan step when started). Owner: D2–D6.

## Owner 2026-11-12
D1 ponds: built then reverted (the bundle refused it: 161 rule keys mixed lake and stream); goes into the next atlas build instead (16,152 ponds change; 97 rules gain, 32 lose). D2 partial salmon coverage: map and bar both give the species verdict. D3 nearer row wins: ignore the loops (receivers with flow-back removed), next build, diff shown. D4 words read at build time: leave, add a guard that stops the build and names the row when a row that matched before stops matching. D5 DFO confluence cuts: same as provincial, the joining water stays out unless DFO's words add tributaries; diff before it ships. D6: Python reads DFO rows tolerantly too (an unreadable row reads "check", is reported, and no longer stops a release). Gauge placement (#4, held): "One rule, accept the moves" - atlas placement wins; 29 of 458 shown gauges move at the next build; list reported.
