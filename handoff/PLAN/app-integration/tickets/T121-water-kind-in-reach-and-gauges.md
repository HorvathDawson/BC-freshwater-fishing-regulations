# T121 — Water kind in reach and gauges: read `water_kind.kind_of`, not the graph `NodeKind`

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, findings reach 8 and
the Python half of cross 8 (PLAUSIBLE).

## Decision being unified
"Is this water a stream, lake or wetland?" (already one function, `water_kind.kind_of`, pinned by
`test_one_water_kind` for other packages).

## Where it is decided today
- `pipeline/regs/reach/build.py:839,845` (stops at a river's own polygon).
- `pipeline/regs/reach/extent.py:156,1191`.
- `pipeline/gauges/consume/shed.py:153` `is_lake_station`.
- `build_panels.py:89` (that part is T55's; do not duplicate it).

How they differ: the graph's `NodeKind` calls a river's own polygon and sloughs/canals a lake, while `kind_of` follows
the curated kind. A walk can stop, or a station be treated as a lake station, where the page calls the water a stream.

## Work
- Call `water_kind.kind_of` at each place; extend `test_one_water_kind`'s pin to `pipeline/regs/reach/` and
  `pipeline/gauges/`.

## Acceptance
Pure refactor where the kinds agree: per-rule binding diff and gauge shed diff, both reviewed (expected small; each
change named).

Rebuild: reach + deliver. Depends: T55 for `build_panels.py`. Size: S. Owner: no.
