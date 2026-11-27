# pipeline/common: shared ground

Depends on nothing else in the pipeline; everything depends on it: the project's paths, models, IO helpers and the small shared
definitions that must not drift between stages.

- `curated.py`: `CURATED` / `GENERATED` / `SOURCE`, the ONLY way to name a data path (roots in `config.yaml`, names on its fields, validated by pydantic at
  first access so a wrong key fails at import, naming it). Never a path literal (AGENTS 36, 41; `scripts/check_data_paths.py` fails CI on one).
  A loader for a curated file RAISES on a missing file (37). `curated_write.py` writes curated files.
- Identity and names: `section_handles.py` (the handle table every artifact keys sections by), `entry_id.py`, `names.py` / `name_sources.py` /
  `search_key.py`, `split_ids.py`, `registry_kinds.py`, `water_kind.py`, `fishing_water.py` (water that is not fishing water, stamped by the atlas).
- Guards: `vintage.py` (tiles and bundle come from one atlas and one registry; a promoted atlas is immutable), `stepcache.py` (input-keyed reuse of a
  stage or delivery step; recorder `pipeline/_trace.py`), `feed_formats.py` (every live feed's format string, generated into the app),
  `tolerances.py`, `calendar_spec.py`.
- Synopsis: `synopsis_pages.py` (printed page = PDF page - 2 to p.40, - 6 after), `synopsis_print.py`, `synopsis_definitions.py`.
- Subpackages: `io/` (serialize, gpkg export), `models/` (graph, registry, splits, sections, regs dataclasses), `utils/`.
- Reads/writes: nothing of its own beyond what `curated.py` resolves.
- Tests: `pipeline/tests/test_curated_*.py`, `test_vintage*.py`, `test_stepcache*.py`, `test_section_handles*.py`, `test_workflow_pins.py` (paths).
