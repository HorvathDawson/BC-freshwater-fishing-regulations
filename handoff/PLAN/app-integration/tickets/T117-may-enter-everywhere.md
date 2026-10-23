# T117 — `may_enter` everywhere: the other walks, and one owner for outlet, code relation and edge type

Status: OPEN.

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
