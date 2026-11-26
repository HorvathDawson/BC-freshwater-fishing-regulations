# pipeline/atlas: the water network, its sections and the rules bound to them

Turns the BC Freshwater Atlas (FWA) into one inverted stream graph, cuts it into SECTIONS at the curated splits, names every water in a registry
and (in `reach/`) binds each rule to the sections it covers.

- Entry: `python -m pipeline.atlas.build --full --out data/generated/atlas/full_next` (a side build, heavy: `scripts/heavy.sh`), then
  `python -m pipeline.atlas.promote <dir> [--dry-run]`; normally `python -m pipeline build --atlas`. A small area: `--gnis "Campbell River" --out ...`.
- Reads `data/source/bc_fisheries_data.gpkg` (FWA, pinned), `data/curated/waters/` (cuts, names, areas, added waters). Writes
  `data/generated/atlas/<build>/` (`graph.pkl`, `registry.json`, `section_handles.txt`, sidecars); not in git (~9 GB).
- Folders: `graph/` (network, prune, tributary walk, name tuples), `splits/` (sectionizer, anchors, area and coast cuts), `registry/` (named waters, regions,
  lake loops), `waters/` (waters the FWA lacks), `reach/` (rules to sections). `inputs.py` digests every curated input into the build's meta.
- Vocabulary: a NODE is a stream piece (a blue line `blk`, cut at its lake runs and curated splits) or a lake (`lake:<wbk>`); an EDGE is "A flows into B";
  geometry sits in a sidecar. Merge on `blk`, never on watershed code or name. Every downstream artifact keys sections by `section_handles.txt`.
- Splits: a lake is cut by the graph build, never by a curated split. `data/curated/waters/cuts.json` (loader `splits/splits.py`, resolver
  `splits/anchors.py`) holds the boundaries the data does not give. An anchor is a line or polygon boundary (`common/models/enums.py` `AnchorType`: `point`,
  `line`, `lake`, `mu_boundary`, `confluence`, `border`, `area_boundary`, generated `gauge`), resolved once at build time to a `SplitPoint(blk, route_measure)`.
- Names: `(name, source, note)` tuples compiled from `data/curated/waters/name_variants.json`, applied in `graph/names.py`; wetlands are an overlay, not nodes.

## Rules

A promoted atlas is immutable: build beside it (`<name>_next`), never rebuild the live one to test (AGENTS 17). Boundaries are cut ONCE, at the source, and
the build only asserts (59); a lake or slough is decided once, in the registry (55). Determinism is a precondition (19).

## Tests

`pipeline/tests/test_atlas_*.py`, `test_tributaries.py`, `test_sectionizer*.py`, the coast and tidal tests, `pipeline/atlas/waters/added_streams/tests/`. Parity
of two builds: `python -m pipeline.tools.audit.build_parity`.
