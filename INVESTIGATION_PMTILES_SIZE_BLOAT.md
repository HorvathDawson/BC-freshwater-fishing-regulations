# Investigation: PMTiles / GPKG Output Size Bloat

**Date:** March 2, 2026  
**Status:** Root cause unresolved — investigation ongoing

---

## Problem Statement

The regulation mapping pipeline is producing output files that are ~3x larger than a known-good reference run:

| File | BAK (good) | Current (bloated) |
|------|-----------|-------------------|
| `regulations_merged.pmtiles` | 1.86 GB | ~5.4 GB |
| `temp/streams.geojsonseq` | ~1.8 GB (fwa ref) | ~4.7 GB |
| `temp/lakes.geojsonseq` | ~33 MB (fwa ref) | ~922 MB |
| `temp/wetlands.geojsonseq` | ~111 KB (fwa ref) | ~1.5 GB |

- **BAK reference file:** `webapp/public/data/bak/regulations_merged.pmtiles`  
  Created: March 2, 2026 at 02:44
- **Current file:** `webapp/public/data/regulations_merged.pmtiles`  
  Created: after 02:44 March 2, 2026

The user has also noted the GPKG output is larger than expected.

---

## What We Know

### Feature Counts Are Identical

Direct inspection of both PMTiles files using the `pmtiles` Python library confirmed that **feature counts are nearly identical** between the BAK and current files:

| Layer | BAK | Current |
|-------|-----|---------|
| Streams | 712,229 | 712,117 |
| Lakes | 385,971 | 385,971 |
| Wetlands | 333,538 | 333,538 |

The ~112 stream difference is negligible. The bloat is **not caused by more features**.

### Tile Distribution Is Different

| Metric | BAK (1.86 GB) | Current (5.4 GB) |
|--------|--------------|-----------------|
| Total tiles | ~974,101 | ~521,772 |
| Avg bytes/tile (z=12) | ~58,447 | ~37,161 |

The BAK file has **roughly 2x more tiles**, each smaller. The current file has **roughly half the tiles**, each much larger.

This pattern is consistent with features **not being properly clipped to tile boundaries**, causing individual tiles to carry larger, unpruned geometries.

---

## Hypotheses Ruled Out

| Hypothesis | Result |
|-----------|--------|
| `includes_tributaries=True` causing stream group fragmentation | ❌ Groups are keyed by BLK (blue_line_key), tributary flags don't split groups |
| Different `parsed_results.json` input | ❌ Re-running with old `parsed_results.json` still produced large output |
| Source GPKG changed between runs | ❌ GPKG last modified Feb 19 22:52; BAK was generated March 2 02:44 — same file used for both |
| `--no-clipping` flag added to tippecanoe | ❌ Flag has been present in all checked commits |
| Code regression in geo_exporter.py | ❌ Old commits also reproduce large output |

---

## Unexplored Variables

1. **Which `parsed_results.json` was used for the BAK run (02:44)?**
   - The first known partial session is `2026-03-02_095300` — created *after* the BAK.
   - There may be an earlier partial session (pre-02:44) that was the actual BAK input.
   - Check: `ls -lht output/synopsis/parse_synopsis/partial_sessions/`

2. **tippecanoe version differences**
   - If tippecanoe was updated between the BAK run and subsequent runs, tile-packing behavior may have changed.
   - Check: `tippecanoe --version` and compare against any version recorded in logs.

3. **Geometry coordinate density / precision**
   - Are the geometries loaded from the geom cache (`.geom_cache/*.pkl`) the same coordinates for both runs?
   - Geom cache timestamps: Feb 20 09:20–09:25 (pre-dates BAK at 02:44 March 2, so same cache used).
   - However, if cache was invalidated and rebuilt between 02:44 and subsequent runs, the source geometries could differ.

4. **GPKG sizes**
   - No GPKG files currently exist in `output/regulation_mapping/` (directory is empty except `row_images/`).
   - A completed pipeline run is needed to compare GPKG size against any historical reference.

5. **Pipeline CLI arguments**
   - What exact CLI flags were used when generating the BAK run? If `--include-zones` or other flags differ, output may vary.

---

## Side Fix: TRIBUTARIES Parsing Bug (Resolved)

Separate from the size investigation, a bug was found and fixed in the parsing pipeline:

**Bug:** `identity_type: TRIBUTARIES` was allowed with `global_scope.type: WHOLE_SYSTEM` instead of requiring `TRIBUTARIES_ONLY`.

**Fixes applied:**
- [`synopsis_pipeline/models.py`](synopsis_pipeline/models.py) — Added two validation rules in `IdentityObject.validate()`:
  1. `identity_type == "TRIBUTARIES"` → `global_scope.type` must be `"TRIBUTARIES_ONLY"`
  2. `"tributaries" in name_verbatim.lower()` → `global_scope.type` must be `"TRIBUTARIES_ONLY"`
- [`synopsis_pipeline/prompts/parsing_prompt.txt`](synopsis_pipeline/prompts/parsing_prompt.txt) — Updated TRIBUTARIES guidance in three locations to explicitly state `global_scope.type` must be `TRIBUTARIES_ONLY`.

---

## Recommended Next Steps

1. **Find the pre-02:44 partial session:**
   ```bash
   ls -lht output/synopsis/parse_synopsis/partial_sessions/
   ```
   Identify any session timestamped before `2026-03-02_024400`. That session's `parsed_results.json` is the likely BAK input.

2. **Check tippecanoe version:**
   ```bash
   tippecanoe --version
   ```

3. **Re-run pipeline to completion** and compare GPKG size against any available reference.

4. **Compare geojsonseq line counts** between a run using the suspected pre-02:44 `parsed_results.json` and a run using a later one, to isolate whether the bloat originates in the GeoJSON export step or the tippecanoe step.

5. **Check geom cache integrity:** Confirm the `.geom_cache/*.pkl` files are the same across both runs (mtime, size).

---

## Key Files

| File | Purpose |
|------|---------|
| [`regulation_mapping/geo_exporter.py`](regulation_mapping/geo_exporter.py) | Produces GeoJSONSeq layers and calls tippecanoe |
| [`regulation_mapping/regulation_mapper.py`](regulation_mapping/regulation_mapper.py) | Merges/groups features by BLK+reg_set |
| `webapp/public/data/bak/regulations_merged.pmtiles` | Reference good file (1.86 GB) |
| `webapp/public/data/regulations_merged.pmtiles` | Current bloated file (~5.4 GB) |
| `.geom_cache/streams_*.pkl` | Cached stream geometries (1.7 GB, Feb 20) |
| `.geom_cache/polygons_*.pkl` | Cached polygon geometries (774 MB, Feb 20) |
| `data/bc_fisheries_data.gpkg` | Source data (8.1 GB, Feb 19) |
