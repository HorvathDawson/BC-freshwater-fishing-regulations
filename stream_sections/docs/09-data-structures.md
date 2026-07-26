# 09 — Data Structures, Serialization, Layout

The **authoritative schema lives in code**: [`stream_sections/models.py`](../models.py).
This doc records the rationale, split-indexing rules, serialization, and layout so the code
is understandable if revisited cold. (Everything lives in top-level `stream_sections/`, outside
`pipeline/`; run with `.venv/bin/python`.)

## Artifacts and their dataclasses (see `models.py`)

| Step (05) | Produces | Dataclass(es) | Rationale |
|-----------|----------|---------------|-----------|
| `blk-chains` | `blk_chains.pkl` (parquet later) | `BlkChain`, `FidSpan`, `WaterbodyRun`, `NameTuple` | Merge on **BLK** (1:1 w/ WSC). `mouth_measure` stored so `substring` offset math works. `WaterbodyRun` = under-lake sub-spans by `WATERBODY_KEY ∈ lakes∪manmade`. |
| `graph` | `graph.pkl` | `StreamGraph`, `StreamNode`, `FlowEdge` | **One inverted graph**: node = a stream (BLK now; section after splitting) or a lake; edge = "flows into" at a confluence measure. `up_adj` = incoming edges = the tributary/ancestor walk. fids live only inside `StreamNode` as provenance. |
| `sections` (next) | refines graph nodes | `StreamNode` (sub-range) + `SplitPoint` | Split a BLK node into a chain of section nodes at lakes + curated splits; lakes become their own nodes; tributary edges reattach to the section whose measure range contains the confluence. |
| `splits` (input+sidecar) | `splits.json` → `splits.resolved.json` | `SplitDef`, `SplitAnchor`, `SplitPoint` | The anchor defines a **cut line or polygon boundary**; resolution yields one `SplitPoint` per crossed channel; written back for reviewable, deterministic builds. |
| `match` | `section_regs.*` | `SectionRegs` | One `reg_set_index` per section (Fraser handled by curated `mu_boundary` splits). Base regs are a separate `mu_id → base_reg_set` overlay, **not** here. |

## The inverted graph (why nodes are streams)

A standard graph (edges = streams, nodes = junctions) forces a mainstem to be subdivided at
**every** tributary junction — that produced 361 segments for Adams and a node per fid
endpoint. Inverting (node = stream, edge = flows-into) keeps the mainstem as **one node** with
many incoming edges, drops fids from the graph, and makes tributaries a plain ancestor walk.
Measured on the Adams extent: **8393 nodes / 8243 edges** (was 21632 segments + 21511 nodes);
Adams River = 1 node / 435 tributaries.

## Split indexing (how a split lands in the graph)

A curated split subdivides a BLK **node** into two section nodes at a route measure (geometry
cut with `substring`), joined by a continuation edge. Each incoming tributary edge reattaches
to the section whose `[down_m, up_m]` range contains its confluence measure. A split is a
labeling/matching boundary — it does **not** stop flow (there is no `barrier` concept). Lakes
are the only structural barrier, and a lake becomes its own node between the up/down sections.

## `section_id` / node id scheme (ABI — fix it once)

Node id is the **readable** `f"{blk}:{int(start_measure)}"` (a whole BLK's start measure; a
section's sub-range start after splitting) — stable and debuggable. Equivalent hash form:
`sha1(blk | lower_boundary_id | upper_boundary_id | lake_wbk)[:16]`. Both are stable when a
split is added **elsewhere**. ABI consequences: authored `split.id`s are stable keys; moving a
split's *position* re-cuts geometry but keeps the id; a split *inside* a node mints two new
ids (expected).

## Serialization (05 partial-rerun model)

- **pickle** now for `blk_chains` and `StreamGraph` (small, dict/adjacency shaped);
  **GeoParquet** is the planned optimization for the bulk-geometry artifacts once at province
  scale (memory-mappable, column/row-group pruning, reproducible bytes).
- Each artifact writes a sibling `*.meta.json` `{input_hashes, build_ts}`; `serialize.is_stale()`
  skips a step whose inputs are unchanged.
- Visual inspection is via the **GPKG** exporter (`export_gpkg.py`), not GeoJSON.

## Module layout (`stream_sections/`)

`models.py` (schema) · `blk_chains.py` (S1: load fids + merge) · `names.py` (S2) ·
`graph.py` (S4: inverted graph + `ancestors`) · `anchors.py` + `splits.py` (04) ·
`sectionizer.py` (S3: split at lakes/splits — next) · `tributaries.py` (S5: guarded walk —
next) · `cutting.py` (substring, node id, endpoint id) · `serialize.py` (IO + cache) ·
`build.py` (end-to-end validation CLI) · `export_gpkg.py` + `complex_regs_report.py`
(temporary tools) · `run.py` (step entrypoints, to wire into `pipeline/__main__.py`).

Tests: `stream_sections/tests/` — `test_<module>.py`; `test_graph.py` holds the two
braiding/lake regression cases (skipped until the guarded walk exists).

## Reuse vs build-new

**Reuse:** `pipeline.utils.wsc.trim_wsc`; `data.data_extractor.FWADataAccessor` (all GPKG reads
+ id normalization); the `"x_y"` endpoint convention; lake-wbk grouping + lake-outlet machinery
from `atlas/freshwater_atlas.py`; per-magnitude minzoom percentiles;
`effective_includes_tributaries`; `MatchTable`/`OverrideEntry`/`BaseEntry` (extend override
matching to target the node id / `location_identifier`).
**Build new:** `graph.py` (inverted graph + ancestors); `cutting.py` (substring + ids);
`anchors.py`/`splits.py` (cut-geometry resolver + sidecar); `sectionizer.py`; `tributaries.py`.
