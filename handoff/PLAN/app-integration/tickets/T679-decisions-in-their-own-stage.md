# T679 — Each decision in its own stage (everything but tidal)

Status: IN PROGRESS — D2 and D6 built in c9488453c (owner 2026-11-12); D1 ponds, D3 nearer row, D4 word-read guard, D5 DFO confluence cuts and gauge placement #4 built in 9b0d6f4ae (+ f7f4fcfee), promoted with `full_t678b` (a5fe40636) and released in r-0a9930fb8826; the 11 other-line gauge cuts decided (RULINGS P49, T682); plan steps 1–3 done (#9 e1f1a268c, #2 8bc6eadbe, #4 b3a3938de); #14 done (9b0d6f4ae); lane L7 2026-11-20: #10 ece8f7a29, #11 7774bcbab, #12 rulings a493cf12d (+ 59e9589cc), #13 61f2bd774, step-10 code 5339c18ef (#1 and #6 behind switches, #15) and 94842bada (#18 dead code), all proved no output change; owner: OWNER-TODO "T679 #12: should the bundle carry"; left (Claude): wave 4 flips #1/#6 with the atlas rebuild and diffs, #18's continuation / places / part point, and L8's steps 5 (#8, #16, #17)

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

## Built 2026-11-12 (c9488453c)
- **D2 (#7):** one source. The card's salmon bar, its "in part" headline and the closed-all-year line read the chain's
  map runs `m` (core `salmonDayStatus` -> `salmonChainStatus`), which the pipeline builds as `map_status` of the day's
  five species verdicts (T372 already took the undrawn place off the map). The species-side re-derivation is gone.
  Measured on r-483b295de57c: 0 chain-days differed (127 chains carry an undrawn place), so nothing on screen moves;
  pinned: `salmon.test.ts` "(T679 D2) ONE source" (mutation: a map run the species do not give shows on the bar too),
  `test_salmon_file.py::test_t679_d2_*` (every chain's `m` = `map_status` of its species, partial water included).
  RULINGS P48.
- **D6:** Python reads DFO rows tolerantly, as the worker's robust mode (`feed.type_rows`): a locator whose rows do not
  type carries `error` and one unread rule (`check` = "DFO's row for this place could not be read — check DFO"); the
  place reads "check DFO" for every salmon every day, the feed logs it, the salmon build names it, the rest of the
  region stands, and the release is not stopped. `live.type_page` (the Mac's live run) stays strict: it is the parity
  spec of the worker's `pythonParity` mode. Builds on T676 (4bc03ac14, b7749b726): a check verdict now leads with each
  rule's own `check` in both languages (same words for the date cell). Shared row "a row that does not type"
  (`dfo-rows.json`, both test suites + the vectors). Measured: 0 of 261 locators fail on today's rows; the typed feed
  is unchanged. RULINGS D6.

Built 2026-11-12 (9b0d6f4ae + the gauge hold after it; candidate atlas `full_t678b`, not promoted, diffs in
`T678-progress.md`): D1 ponds folded at the registry (`flowing.fold_widening_ponds`, one test `graph.widenings`;
16,119 ponds, 7 left and listed; lake:329461947 in the Oxbows); D3 `item_exit_down` + `nearer.NEARER_IGNORES_LOOPS`
(bundle; 3 pairs newly ordered, none lost); D4 `reach.word_reads` + `regulations/word_reads.json` (69 reads; a lost read
stops the run); D5 `build.DFO_JOINING_WATER_STAYS_OUT` (7 DFO places keep the joining water out); gauges
`place.nodes_at_cuts` (bundle): 10 of the owner's 29 move, 17 lake-outlet cuts and 11 cuts on another blue line keep the
projection, listed. Left (Claude): D1/D3/gauges show at the next deliver after promote; the 11 other-line gauge cuts and
the Dewdney side effect go to the owner.

Owner 2026-11-20 09:59 (dialog), #8: "Ranking's rule, list first (Recommended)": along the water, a lake on the way ends it; the list of gauges that move goes to the owner before it ships.
Owner 2026-11-20 10:45 (dialog), after the list (`T679-shots/own-gauge-moves.csv`) was sent: **"Ship it"**. `packed.SHIP_OWN_GAUGE_FLAG` is ON (merge of lane/l8-shrink); `flow-own/1` ships with the next data release, which rebuilds the bundle for `section_own`.
Owner 2026-11-20 11:02 (dialog), after the blocker measure (`T679-shots/own-gauge-blockers.csv`: 448 of 1,060 moves behind unnamed ponds of no water): **"Named lakes only (Recommended)"**: `own_distances` walks through an unnamed pond that belongs to no registry water, at any size; a named lake still ends it. The lead-gauge ranking reads the same walk, so it changes with it. Ships with #8.

## Plan step 5, lane L8 (2026-11-20, `lane/l8-shrink`)
- **#17 BUILT.** The export joins a rule-set group's runs once (`export.read.group_runs`, the water field `grp_runs`,
  only where a group has 2+ parts: 200 waters on the live bundle); the answers read it (`display.builders._by_grp`
  refuses a 2+-part group with none, never re-joins). Proof: export + answers on the live bundle with the code before and
  after (via `scripts/heavy.sh`): `ui-rules-answers.json` BYTE-IDENTICAL; the export differs only by `grp_runs` and its
  field-dictionary line. Measured alternative, not taken: composing each group's runs from its sections
  (`compose_runs` over the union) equals the join in 244 of 246 groups; the Shuswap (gnis:25478, a branch run's order)
  and gnis:3273 (three zero-length side-channel branch runs dropped) would change — a visible change for a maint item.
- **(superseded below: #8 was built, then shipped on the owner's 10:45 "Ship it")** **#8 NOT BUILT — owner question (OWNER-TODO 2026-11-20 09:52; answered 09:59 above).** The app's "on this water" (`gaugeSide`: the gauge's
  water is this water or a `water_continue` of it) and the ranking's own (`panels.own_distances`: the same, walked along
  the water, a lake ends it) can differ; which one to keep is what the angler sees. Measured on the live bundle: the
  app's rule over 176,367 (section, panel member) pairs calls 12,306 own, and in 12 of 1,587 panels it differs between
  the panel's sections, so the flag ships PER PAINT SECTION (a bitmask over the panel's members, as `flow-small`), not
  per member; it needs a release format (new file or flow-small/2), its app reader beside `gaugeSide` for older releases,
  and the atlas graph to measure which gauges move.
- **#16 NOT BUILT.** One flag means a paint format (paint/6: the union share `section_no_access` or a mostly-inside bit,
  instead of the app summing per-area rows) with its app reader; `access.summed_rule_disagrees` already refuses any
  bundle where the two disagree, so nothing on screen differs today (maint). Left for the next paint format change.

## #8 and #16 built behind switches (2026-11-20 10:22, lane L8, after the owner's 09:59 / 10:00 answers)
- **#8 BUILT; SHIPS (owner 10:45 "Ship it"; switch ON).** The rule is the ranking's: along the water, across a
  `water_continue`, and a lake on the way ends it. `panels.build` keeps each section's own stations (`Panels.own`, from
  `own_distances`), and the bundle writes `section_own (sid, bits)`: bit i is panel_member `ord` i. The release file
  `flow-own/1` (`paint.encode_own`, one varint per paint section, the same layout as flow-small) is written only when
  `packed.SHIP_OWN_GAUGE_FLAG` is on, and it is OFF. App 0.8.4 reads it: `loader.flowOwn`, then `gaugeWaters` gives
  `own`, then core `gaugeSides(..., own)`. Where a release carries the flag, it is the answer. Without it, the old rule
  decides, as before. `flow-own` is optional in the contract, so an older app ignores it.
- **The list** (`app-integration/T679-shots/own-gauge-moves.csv`: water, gauge, was → now, sections). Made by running
  this branch's gauge pass on the live atlas, read-only, via heavy.sh. Its panels equal the live bundle's exactly.
  - Over 176,367 (section, panel member) pairs, the heading's rule says own 12,306 times and the ranking's 11,246.
  - 1,060 pairs move, all of them own → donor. None moves the other way.
  - That is 155 rows: 155 gauges on 104 waters.
  - Biggest: Halfway River (07FA006, 59 sections; 07FA003, 53), Stikine at Telegraph Creek (08CE001, 44), Lillooet at
    Tenas Narrows (08MG027, 35), Columbia at Donald (08NB005, 34) and at Nicholson (08NA002, 31), Graham (07FA005, 26),
    Pitt near Port Coquitlam (08MH035, 25), Bowron (08KD007, 24), Moberly (07FB008, 22).
  - The owner OK'd the list (10:45): `SHIP_OWN_GAUGE_FLAG` is ON; the next bundle rebuild writes `section_own`.
- **#16 BUILT, on the T686 switch** (owner 10:00: every left-over on this release). paint/6 is paint/5 plus one column:
  the sections MOSTLY inside no-access land, by the bundle's union share (`section_no_access.permille` > 500).
  - Core `noPublicAccess` reads it where carried (`SectionAccess.mostly`); paint/4–5 still sum the rows.
  - It is written when `packed.SHIP_PACKED_FORMATS` is on, so it switches on with the packed formats. Off, paint/5 is
    byte-identical to live.
  - Measured ON, on the live bundle: 3,597 sections, the same set the summed rows give; +282 B gz.

Owner 2026-11-20 11:11 (dialog): #1 and #6 **"Turn both on (Recommended)"** in the wave-4 rebuild, diff first (stop if anything unexpected moves). #12 not asked: lane L7's default stands (lists stay in code).

## #8 follow-up: what ends the ranking's walk (2026-11-20 ~11:05, lane L8, read-only)
The owner asked whether tiny lakes block "own". For each of the 1,060 moved (section, gauge) pairs, I found the first node
that ends `own_distances`' walk on the flow path between the gauge and the section. The run used dev's gauge pass on the
live atlas via heavy.sh, and its panels equal the live bundle's. Results are in `app-integration/T679-shots/own-gauge-blockers.csv`
(water, gauge, sections, blocker id / name / kind, area_ha; pond areas from FWA `lakes` / `wetlands` AREA_HA).
- **A lake or pond ends 881 of the 1,060.** By area: under 1 ha 354, under 5 ha 414, under 10 ha 433, under 50 ha
  443, under 100 ha 450.
- **448 of the 881 are UNOWNED ponds:** 223 `lake:<waterbody key>` polygons that are in no registry water. They are
  NOT folded ponds (T679 D1): a folded pond belongs to its river's item, so the walk passes through it. These are ponds
  the fold did not take. The biggest is 163 ha; most are under 1 ha.
- **The other 433 are named lakes,** e.g. Pitt, Bowron, Lillooet / Little Lillooet, Laslui, Windermere.
- **Not a lake (179):**
  - 127: no flow path. The gauge is neither upstream nor downstream of the section (another branch or a tributary); the
    walk never turns.
  - 36: another water's stream piece in between (Hotsprings Channel on the Columbia, Widgeon Slough on the Pitt).
  - 15: an unowned line.
  - 1: the path passes every test.
- **Per river (sections):**
  - Halfway: ponds under 1 ha 80, ponds of 1 ha or more 10, no path 22. The pond is lake:329626214, 0.44 ha.
  - Stikine: ponds under 1 ha 23, Laslui Lake 13, no path 5.
  - Lillooet: Little Lillooet 18, Lillooet Lake 12, no path 5.
  - Columbia: Hotsprings Channel 21, ponds 20, no path 10, Windermere 6.
  - Pitt: Pitt Lake 24.
  - Bowron: Bowron Lake 15, ponds 8.

## #8 "Named lakes only" built (2026-11-20 11:16, lane L8, `lane/l8-blockers`; owner 11:02)
- **The change.** `panels.own_distances` now walks THROUGH a pond that belongs to no registry water, at any size and at
  its graph length (0). The pond is never itself reached. A named lake (a registry item) still ends the walk, and so does
  an ownerless line. The panel-membership walk (`candidates`) is unchanged: the lead ranking and `section_own` both read
  `own_distances`.
- **The test.** `test_polygon_walk.py::test_t679_8_a_river_cut_by_an_ownerless_pond_keeps_its_own_gauge_a_named_lake_still_cuts`.
  It fails on the old walk (checked).
- **D1 folded ponds:** unchanged by construction. A folded pond belongs to its river's item, so it is never "ownerless".
  None of the 223 pond blockers was a folded pond.
- **Re-measured** on the live atlas (read-only, heavy.sh), with the two CSVs regenerated
  (`T679-shots/own-gauge-moves.csv`, `own-gauge-blockers.csv`):
  - Moved pairs: 611, down from 1,060. That is 118 gauges on 78 waters, all own → donor.
  - What still blocks them: named lakes 433 (none under 10 ha; 3 under 50 ha, 9 under 100 ha); no flow path 127;
    another water's stream 34; an ownerless line 16; other gates 1.
  - Lead gauges changed by this rule, against dev's code without it: 0 sections. The 267 pairs that became own were
    already leads (an adopted side channel takes its river's panel and leads), so the heading now agrees with the lead.
  - The same run differs from the live bundle in 7 lake sections: Sproat Lake 2, Moat Lake 2, Tatlatui Lake 2 and
    Nanika Lake 1 have no panel. That difference comes from dev's other merged work, not from this rule: the baseline
    without it gives the same 7.

## Lane L7, 2026-11-20 (wave 3: parity only, no live atlas rebuilt)
Owner kept #10, #13, #15, #17 (#17 is lane L8's, with #8/#16); #14 was already done.
- **#10 one licensing pass** (ece8f7a29): `own_beats_inherited` folded into `first_classified_downstream` (one call in
  `reach/build.py`; its one own case — a walked section on the walker's own water another designation names by reach —
  is decided before the walk: `trib_yields_to_own` / `trib_kept_beside_own`). Proof: reach on the live atlas,
  `licensing_section.jsonl` (1,049,654 rows) and `licensing_placement.jsonl` byte-identical, `binding_diff` empty; the
  11 `trib_yields_to_own` records (317 sections) now count under `first_classified_downstream`. POLICY_VERSION unchanged.
- **#11 in-season ranges generated** (7774bcbab): `inseason/notice_waters.json` (format 3) holds only the owner's
  matches; what a resolve derives is `data/generated/inseason/resolved.json` (`python -m pipeline.regs.inseason.resolve`;
  a missing one stops, naming it). The release commits nothing for in-season; AGENTS 66 updated. Proof on
  r-6258ffded676: split + merge byte-identical (38 matches, 79 history rows), published `feeds/inseason/matches.json`
  byte-identical. The deliver stage key now carries the notices' end tokens (deliver reruns once).
- **#12 rulings to curated data** (a493cf12d, 59e9589cc): `RELEASE_UNDER_CLOSURE_KNOWN` (1) and
  `QUOTA_UNDER_CLOSURE_KNOWN` (19) are `regulations/under_closure_calls.json` (`closures.under_closure_calls`, refuses
  a missing / malformed / duplicate row). Proof: the export alone on the live bundle, `ui-rules-export.json`
  byte-identical. LEFT: `UNDRAWN_PART_TRIBUTARIES` / `EXCEPTED_RIVERS_CREEKS_CLOSED` need the bundle to carry a rule's
  own tributary scope (it drops `includes_tributaries: false`) and `tributary_excludes` (kept out on purpose,
  `bundle/rules/row.py:84`) — an owner question (OWNER-TODO 2026-11-20) and L8's files.
- **#13 added-stream hand fixes curated** (61f2bd774): `waters/added_stream_calls.json` (drop by name, the name-typo
  map, 77 src_ids, the Magnolia receiver), read by `added_streams.clean.calls()`; not an atlas input. Proof: the loaded
  file equals the old constants and the cleaned layers are byte-identical for all six sources (abbotsford 18,583,
  burnaby 432, squamish 1,486, port_moody 506, osm 2, manual 1); the full generator was not rerun (it fetches DEM tiles).
- **#15 the sidecar writer stays, named** (5339c18ef): `atlas.sidecars` is the supported label-only path; its files
  must equal the build's (`disagreements`, `--check OUT`, `test_sidecars_agree.py -m slow` on the atlas under test).
  Fixed: `widening_fold_report.json` was missing from `SIDECARS`, so an `--out` run wrote it through its link into the
  source atlas. Its full-data run waits for the wave-4 atlas (an atlas built by today's code and curated files).
- **#1 one outside flag** (5339c18ef, `border.ONE_OUTSIDE_FLAG`, off): the atlas stamps `out_of_bc` on every node in no
  region polygon (`mark_regionless`, in `build_registry` before graph.pkl); the reach reads the flag alone and refuses an
  atlas that left one unstamped. Measured on every build in `step10_report.json`.
  Live atlas (probe 2026-11-20): 1,019 regionless nodes, 203 of them not yet `out_of_bc` (lake 71, stream 58,
  wetland 74) — what the flip moves outside.
- **#6 the registry owns every named node** (5339c18ef, `registry.build.NAMED_NODES_ARE_IN_ITEMS`, off): a node no
  stream / lake / wetland item holds loses its `display_name` (`clear_unheld_names`; `name_tuples` kept). Measured the
  same way. Live atlas: 42 named nodes in no water (stream 38, lake 4; e.g. Serpentine River,
  Salmon River, Katzie Slough, Kootenay River pieces) — check those in the wave-4 diff before flipping.
- **#18** (94842bada): the bundle's dead `_in_bc` / "dropped" count go. Left for wave 4 (output changes, L8's files):
  continuation's topology into the atlas (`bundle/continuation.links` + `goes_on` as an atlas sidecar, the bundle keeping
  the book pairs and the owner gate), one place-dedupe precision (tiles 3 dp vs bundle 4 dp), one "where is it" point
  per part (`part_geo.mid_sid` vs `release/part_line.py`).

### Wave 4 must run (one rebuild)
1. Flip `border.ONE_OUTSIDE_FLAG` and `registry.build.NAMED_NODES_ARE_IN_ITEMS` to True (their tests in
   `test_t679_step10.py` flip with them) — or leave either off on the owner's word.
2. The side atlas build (heavy.sh), then `python -m pipeline.atlas.sidecars --build <side> --check <new dir>` (#15: must
   print "the two writers agree") and `ATLAS_BUILD=<side> pytest -m slow pipeline/tests/test_sidecars_agree.py`.
3. `build_parity.py` live → side, reach on the side atlas, `binding_diff` against the live reach run; attribute every
   change to #1 (sections newly outside: the regionless count in `step10_report.json`), #6 (labels only), T926 / T780 and
   the curated edits since the live atlas (tidal_limits.json, name_variants.json are ahead of it).
4. Tiles: the drawn set against `undrawn.json` (#1 drops the regionless slivers from the stream layer).

## Review leftover, test hygiene (lane LF, 2026-11-20, 0043ad017)
`test_export_codec.py::test_the_files_on_disk_are_the_bundles_pair` compared the 6 MB one-line export with a bare
`assert`; on a mismatch pytest's assertion rewrite ran difflib over it for hours. It now uses `_same`: equality, and on
a mismatch the lengths, short digests and the first differing offset with the text around it. Test:
`test_a_mismatch_of_two_big_exports_fails_in_seconds_and_says_where` (a plain assert fails it).

## #8 review leftover (lane LF, 2026-11-20, fe5dbbe7d)
`own_distances` walked through ANY ownerless non-stream polygon, ownerless wetlands and not-fishing polygons included,
which `river_widenings` excludes. Now one test serves both (`panels.ownerless_pond`: a LAKE polygon no registry water
owns, not a not-fishing polygon), so per the owner's "Named lakes only" only unnamed ponds and lakes are walked through.
Test: `test_t679_8_only_an_ownerless_lake_polygon_is_walked_through_never_a_wetland_or_a_not_fishing_polygon` (fails
with the old test, and with the not-fishing check dropped). Measured on the live atlas via heavy.sh (the bundle's gauge
pass, dev be924cbf3 against this lane): moved (section, gauge) pairs 611 -> 611 (118 gauges, 78 waters), lead sets
changed 0, `section_own` rows changed 0. No bundle output changes; no POLICY_VERSION bump.
