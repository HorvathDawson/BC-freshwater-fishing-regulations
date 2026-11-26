# pipeline/atlas/reach: the reach builder

Binds every rule of every catalogue entry to the sections it covers (extents, tributary walks, areas, rest-of-water),
deterministically. It never cuts a river: it only selects among sections the sectionizer already made, and it reports
what it could not bind with a typed reason. Design and policy: [`pipeline/docs/REACH-BUILDER.md`](../../docs/REACH-BUILDER.md).

- Entry: `python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out data/generated/reaches/full
  [--against <earlier run>]` (normally the `reach` stage of `python -m pipeline build`).
- Reads: a promoted atlas, the catalogue corpus `data/curated/regulations/synopsis/`, the DFO places
  `data/curated/dfo_salmon/places/`, the steelhead list.
- Writes: `data/generated/reaches/<run>/` (per-rule bindings, `report.json`, diagnostics, `dfo_salmon.jsonl.gz`).
- Modules: `extent.py` (one extent to sections), `tributaries` (via `atlas/graph`), `covered.py` (an entry to registry items),
  `classify.py` (bound | unresolved with a reason), `build.py` (the corpus), `licensing.py`, `steelhead.py`, `dfo_salmon.py`,
  `io.py` / `diff.py` / `binding_diff.py` (write a run, what changed), `cache.py` (per-entry results).

## Rules (AGENTS 5-16, 43, 63)

The builder never decides where a regulation applies (12) and never widens an extent (13); reaches resolve by route measure
on the cut's own blue line (11); every rule ends bound or unresolved with a typed reason (14); a tributary walk never
enters the entry's own water (63). Bump `cache.POLICY_VERSION` with a changelog line when behaviour changes (20). No
parallelism in the builder (43).

## Tests

`pipeline/tests/test_reach_*.py`, `test_doc_drift.py` (the doc's module list and reason block must
equal the code).
