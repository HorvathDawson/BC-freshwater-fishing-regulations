# T331 — Curated config hygiene (small items from data-flow.md §5)

Status: OPEN (owner 2026-10-30; T324 design approved, plan in handoff/PLAN/data-flow.md §4–5).

1. Declare every curated file in config (`dfo_salmon/wording.json`, sloughs page-marks, `sub_lake_areas.json`);
   `wording.py`/`slough.py` read through `CURATED`.
2. Drop `CURATION_*` env overrides that repoint a curated file (`CURATION_INSEASON_MATCHES`; tests use temp roots).
3. One sync list: `feeds-sync-inputs.yml` and `sync-inputs.sh` read the same key list (workflow lacks wording.json).
4. Dead files: delete or declare `regulations/cross_references.json` (test only), `waters/sub_lake_areas.json`.
5. Source data out of curated: `gauges/waterbody_type.json`, `runtiming/timing.json` → `data/source/`.
6. Close the 7 retired `proposal`-scope marks (closed list, T325).
7. Narrow the deliver key to the curated files deliver reads.
8. Prune `data/release/` (27 folders) to live + previous.
Size: M (each S).
