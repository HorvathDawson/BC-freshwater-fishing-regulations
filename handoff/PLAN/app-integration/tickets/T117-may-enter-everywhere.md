# T117 — `may_enter` everywhere: the other walks, and one owner for outlet, code relation and edge type

Status: DONE — 8446c1ff (step 1), b98c3f3a (step 2), 48c6e9a0 (POLICY_VERSION 24); step 1 reach binding diff EMPTY, step 2 diff: 0 rules, 0 unresolved, 1 licensing record (−1 section, explained below). Gauge shed diff reviewed below (ships at deliver). Rebuild: reach + deliver (no atlas).

Source: owner 2026-10-22 (unify checks that decide the same thing; the motivating case is T90/T102); unification
audit 2026-10-22, findings reach 2, 3, 4, cross 6, 7, 9, 10 and matching 5 (CONFIRMED in code; leaks PLAUSIBLE, not
measurable without `graph.pkl`). This is the concrete list for T102 part B; T102 itself is unchanged. Do this after
T102 part B lands, as a separate change.

## Decision being unified
"May a walk step from this water into that one?" and the three sub-decisions it rests on.

## Walks that bypass the walk's entry rules today
- `pipeline/atlas/graph/tributaries.py:414` `subtree` → `graph.py:300` `ancestors` (barrier only).
  `pipeline/regs/reach/build.py:687` and `_expander:930` use it for with-cut confluence rows: 26 rows, 3,505 sections,
  never checked against the walk.
- `pipeline/regs/reach/build.py:826` `_stem_to_first_lake`.
- `pipeline/common/io/export_gpkg.py:220` (the QGIS check layer).
- `pipeline/gauges/consume/shed.py:125`.
- `tools/unnamed_trib_sweep.py:150,294`.
- `added_streams/harness.py:66`.
- The review-app picker `curation-review/backend/reuse.py:535-580`.

Note: `subtree` deliberately includes the joining river's own mainstem, so `tributaries_of_reach | mouths` is NOT a
replacement. `subtree` must step through `may_enter` instead.

## Sub-decisions, each needing one owner
- **Lake main outlet:** `_main_outlets:298` (order, then main-flow edge type, no code) against `_is_main_outlet:669`
  (same code and strictly greater order). They disagree on equal-order outlets. → one `lake_main_outlet(graph, lake)`.
- **Code relation:** more than 10 prefix spellings. `_breaks_strahler:729` has no dash guard. `graph.py:45` treats an
  empty parent as yes while `shed.py:206` treats empty as no. → `pipeline/common/utils/wsc.py` `relation(a, b)`,
  `is_coastal`, `is_unknown`. (`graph.py:215` already drops stream→stream edges that break descent, so T102's parent
  check can only fire on lake or uncoded edges.)
- **FWA edge types:** `build_panels.py:181` treats side channel = {1100, 1350}, while `tributaries.py:295` treats main =
  {1000, 1050, 1200, 1250} and calls 1150 secondary. → `pipeline/common/fwa_edge_types.py` with `is_side_channel` /
  `is_main_flow`.

## Work
1. The three sub-decision modules; T102's `may_enter` and every place above call them.
2. Each walk above steps through `may_enter` (gauges/sweep/QGIS/picker may pass a looser `reach` context, but not their
   own rules).
3. Pin: a grep test that no module outside `wsc.py` slices or prefix-compares watershed codes, none outside
   `fwa_edge_types.py` names edge-type numbers, and none outside `tributaries.py` walks `ancestors`/predecessors for a
   tributary set.

## Acceptance
- Step 1 as a pure refactor: an empty per-rule binding diff against T102 part B's output.
- Step 2 (the `subtree` → `may_enter` swap) as its own reviewed binding diff, every moved rule named. Gauge shed diff
  reviewed separately (memory `gauge-regenerate-drifts`).

Rebuild: reach + deliver (no atlas). Depends: T102 part B. Size: L. Owner: no.

## As built (2026-10-23, lane-T117)

### Step 1 — the three sub-decisions, one owner each (8446c1ff; pure refactor)
- **Code relation:** `pipeline/common/utils/wsc.py` — `relation(a, b)` → `same | below | above | apart |
  unknown` (dash-guarded), and the names built on it: `is_at_or_below`, `is_strictly_below`, `groups`,
  `root`, `parent`, `ancestors`, `head`, `join`, `group_after`, `is_unknown` (missing or `999`),
  `is_no_code` (FWA's raw `999-999999` sentinel), `is_coastal`, `in_nine_hundreds`, `has_prefix` (for the
  CONFIGURED prefix lists: `fwa_exclude`, the tidal leaf-prune prefixes). Every hand-written slice, `-`
  split, group join and prefix test of a code now asks it: tributaries (`code_runs`, `_strict_ancestors`,
  `_code_says_tributary` — which gains the dash guard), the atlas edge filter (`graph._wsc_descendant`),
  gauge shed (`drains_through`) and panels (`_code_child`), extent (`_first_group`, `_basin_members`,
  `_line_length`, the placement walk), basins (`in_basin`), registry basins, anchors, pfma drainage, leaf
  prune, bundle continuation (`drains_into`), runtiming passage, dfo splitwork, the atlas build's lake
  codes / `fwa_exclude`, and added streams (build_dataset, validate, mapcheck, coastal). What a MISSING code
  means stays each caller's call, kept exactly: the edge filter keeps an edge into an uncoded mainstem,
  the shed claims nothing, the walk's code test is silent.
  Bare prefix → dash guard is the same answer on well-formed codes; measured on the live graph:
  **0 malformed `wsc` / `basin_wsc` codes** (every one `ddd(-dddddd)*`).
- **FWA edge types:** `pipeline/common/fwa_edge_types.py` — `MAIN_FLOW {1000,1050,1200,1250}`,
  `SIDE_CHANNEL {1100,1350}`, `CONNECTOR 2300`, `SINGLE_LINE 1000`; `is_main_flow`, `is_side_channel`,
  `is_barrier`. Read by `StreamNode.is_barrier`, `tributaries.lake_main_outlet`, `build_panels`
  (`SIDE_CHANNEL_EDGE_TYPES` / `_is_side_channel` are now aliases) and the added-streams ingest. The sets
  were moved, not changed (1150 is in neither — owner question).
- **Lake main outlet:** `tributaries.lake_main_outlet(graph, lake)` — the largest order, ties to main flow
  — read by `code_runs` and by the order guard's exemption (`_is_main_outlet` = main outlet AND the lake's
  own code). Measured: of the 453 lake→outlet edges where the order guard fires, the old and new answer
  differ on **one** — Burnaby Lake → Brunette River (356294414:6775, order 3, main flow), tied on order with
  an added stream (−2000000335:0, order 3, main flow): the old "strictly larger than every other outlet"
  refused both, the new rule calls both the lake's river, so a walk up the Brunette may now enter the lake.
  No rule or licensing binding moved.
- Reach rerun (snapshot of 8446c1ff): digest `81e726a3e6fbad3b` = reaches/full; binding diff 0 / 0 / 0.

### Step 2 — every walk steps through `may_enter` (b98c3f3a)
- `tributaries.subtree` climbs with the mouths as its reach (`via=UP`: its own mainstem is in, no "river
  continuing"); `tributary_node_ids` is `subtree` less the node. `reach.build._stem_to_first_lake` asks
  `may_enter` at each `continuation` step. `export_gpkg.export_tributaries` (the QGIS check layer) and the
  added-streams harness climb through it. The gauge shed's climb UP from a gauge asks it
  (`shed.SHED_CLIMBS_THROUGH_MAY_ENTER`; the walk down is untouched). The T57 sweep asks it (T102 part B).
  The review-app picker and its one-level map layer read `tributaries.joining` (the walk's first step);
  `reuse._tributary_items` / `_TRIB_EDGE_KINDS` are gone.
- No production module imports `graph.ancestors` any more; it stays as the graph's raw closure for the
  graph's own tests.
- **Reach binding diff vs reaches/full** (snapshot of b98c3f3a; digest still `81e726a3e6fbad3b`):
  0 rules, 0 unresolved, **1 licensing record**: `r5:chilko_river@5-5::chilko_river` −1 section,
  `355996084:17`. Why: the Chilko row's confluence cut at Brittany Creek has no row of its own, so Brittany's
  `subtree` goes with the cut (122 sections joined into the designation). 355996084:17 is a piece of
  **North Tsuniah Creek** (code `100-342455-328500-647027-633316`, FWA secondary flow 1100) — the secondary
  outlet of a Tsuniah-coded lake (lake:329079861) that flows into a lake on Brittany's branch
  (`…-401051-…`). It is a code run (Dewar Lake ruling: follow the code), and the edge into Brittany's lake is
  the run's divide: the walk already refused it (`code_divide`), `graph.ancestors` did not. Correct by the
  code ruling; POLICY_VERSION 24.
- **Gauge shed diff** (the bundle's `build_gauge_sheds` call, switch on vs off, live atlas):
  108,430 → 108,406 rows; 307 rows lost, 283 gained — **24 sections lose every gauge, 0 gain one**. Most of
  it is a speaker change at a diversion: `08GC005 THEODOSIA RIVER DIVERSION ABOVE OLSEN LAKE` → `08GC006 …
  DIVERSION BYPASS` on 174 sections, `08HD034 QUINSAM RIVER DIVERSION HEADPOND` → `08HD021 QUINSAM RIVER AT
  ARGONAUT BRIDGE` on 107 — the diversion gauges no longer climb through the 2300 canal piece. The rest
  (Muskwa 6, Mission Creek 5, Kootenay at Fort Steele 5, Salmon/Quinsam/Skeena/… 1–2 each) lose sections
  above refused steps: at the 24 lost sections the first refusal is `barrier` 9, `code_divide` 8,
  `bigger_river` 1. Ships at deliver; review per memory `gauge-regenerate-drifts` before promoting.
- Tests: `test_wsc_relation.py` (whole relation table; callers keep their unknown; pin: no module outside
  `wsc.py` / the added-streams minting module slices, splits, joins or prefix-compares a code, with a
  4-line allow list of non-codes), `test_fwa_edge_types.py` (the sets whole; pin: no edge-type number
  outside the module; the model asks it), `test_may_enter.py` (lake_main_outlet whole; both readers ask
  it), `test_t117_walks.py` (each walk on a braid/canal fixture; mutations: `may_enter` → None in
  `subtree`, the stem, the shed switch off, the picker; AST pin: each listed walk names
  `may_enter` / `joining` / `subtree`, none reads `is_barrier` / the order guard / `MAINSTEM_EDGE_KINDS`, no
  production import of `graph.ancestors`). Sweep fixture regenerated (ff3c7a02): same 150 frontier pairs,
  0 unexplained, reasons renamed.
- Rebuild that ships it: reach + deliver (no atlas). The atlas-side conversions (edge filter, lake codes,
  `fwa_exclude`, leaf prune, basins) are the same answer on the live codes and need no atlas rebuild.

## Owner questions
- **1150 (secondary flow in a wetland)** is neither main flow nor a side channel for the gauge panels.
  Default built: unchanged (moved as it stood). Should the panels treat 1150 as a side channel?
- **Burnaby Lake**: with equal-order main-flow outlets the lake now has two "main" outlets (the Brunette
  and an added stream). Default built: ties go to main flow, every tied main-flow outlet counts. No binding
  moves today.
- **Gauge shed at diversions**: the Theodosia and Quinsam diversion gauges stop speaking for the water
  above their canal (the river's own gauge takes over). Default built: the shed climbs as the walk climbs.

## Left / follow-ups
- `build_panels.candidates` (gauge-panel reading propagation) and the shed's walk DOWN keep their own
  rules: they propagate readings, they do not collect a tributary set.
- `pipeline/atlas/waters/added_streams/wsc.py` (code MINTING) is allowed by the code pin by name; it builds
  codes rather than comparing them.
- The review app's picker/map layer change is not exercised against the live graph here (stubbed test);
  check one item in the app after deliver.
