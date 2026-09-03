# 08 — Data Structures, Serialization, Layout

The **authoritative schema lives in code**: [`pipeline/common/models.py`](../models.py).
This doc records the rationale, split-indexing rules, serialization, and layout so the code
is understandable if revisited cold. (Everything lives in top-level `pipeline/`, outside
`pipeline/`; run with `.venv/bin/python`.)

## Artifacts and their dataclasses (see `models.py`)

| Step (07) | Produces | Dataclass(es) | Rationale |
|-----------|----------|---------------|-----------|
| `blk-chains` | `blk_chains.pkl` (parquet later) | `BlkChain`, `FidSpan`, `WaterbodyRun`, `NameTuple` | Merge on **BLK** (1:1 w/ WSC). `mouth_measure` stored so `substring` offset math works. `WaterbodyRun` = under-lake sub-spans by `WATERBODY_KEY ∈ lakes∪manmade`. `edge_types` = distinct FWA `EDGE_TYPE`s (preserved so `2300` survives the merge). |
| `graph` | `graph.pkl` + `geometries.pkl` | `StreamGraph`, `StreamNode`, `FlowEdge` | **One inverted graph**: node = a stream **piece** (a BLK cut at lake-runs) or a **lake** (one per `wbk`); edge = "flows into". Lakes are split in **this** step from the fid `wbk`-run (no polygon). `up_adj` = incoming = ancestor walk; a lake's `up_adj`=inlets, `down_adj`=outlet(s). **Geometry is NOT on the node** — a separate `node_id → geom` sidecar (`geometries.pkl`) keeps the graph light. fids live only inside `StreamNode` as provenance. |
| `sections` | refines graph nodes | `StreamNode` (sub-range) + `SplitPoint` | **Curated splits** (falls/bridges/MU boundaries/area boundaries). Lake above/below splits already exist from the `graph` step. A curated split subdivides a stream-piece node further; tributary edges reattach to the piece whose measure range contains the confluence. |
| `splits` (input+sidecar) | `splits.json` → `splits.resolved.json` | `SplitDef`, `SplitAnchor`, `SplitPoint` | The anchor defines a **cut line or polygon boundary**; resolution yields one `SplitPoint` per crossed channel; written back for reviewable, deterministic builds. |
| `match` | `section_regs.*` | `SectionRegs` | One `reg_set_index` per section (Fraser handled by curated `mu_boundary` splits). Base regs are a separate `mu_id → base_reg_set` overlay, **not** here. |

## The inverted graph (why nodes are streams)

A standard graph (edges = streams, nodes = junctions) forces a mainstem to be subdivided at
**every** tributary junction — that produced 361 segments for Adams and a node per fid
endpoint. Inverting (node = stream, edge = flows-into) keeps the mainstem as **one node** between lakes
with many incoming edges, drops fids from the graph, and makes tributaries a plain ancestor
walk. Measured on the Adams extent with lakes as nodes: **10821 nodes (9254 stream pieces +
1567 lakes) / 10929 edges** (was 21632 segments + 21511 nodes). A mainstem is one node per
lake-bounded reach; the Adams River mainstem BLK is 4 pieces (Lower Adams | Adams Lake node |
Upper Adams | …), and Lower Adams' guarded ancestors reach the whole watershed through the lake
node — the upstream/downstream-of-Adams-Lake split comes free, with no curated split.

**Guards at build (spike 12):** the WSC-descendant filter drops braiding-reversed and
cross-watershed **stream→stream** edges at creation; a node whose fids include `EDGE_TYPE=2300`
is `is_barrier` and `ancestors(guarded=True)` stops at it. (Once lakes are nodes, the
Columbia/Kootenay canal drains into a lake node, so the 2300 barrier — not the WSC filter — is
the operative guard there; the WSC filter still fixes stream-stream braiding e.g. Chehalis.)

## Split indexing (how a split lands in the graph)

A curated split subdivides a stream-**piece** node into two at a route measure (geometry cut
with `substring`), joined by a continuation edge. Each incoming tributary edge reattaches to the
piece whose `[down_m, up_m]` range contains its confluence measure. A curated split is a
labeling/matching boundary — it does **not** stop flow (there is no per-split `barrier` flag).
The structural flow barriers are **lakes** (already their own nodes from the `graph` step) and
**`EDGE_TYPE=2300`** connectors (`StreamNode.is_barrier`), both applied by the guarded walk.

## node id scheme (ABI — fix it once)

- **stream piece:** `f"{blk}:{int(down_m)}"` — the BLK plus the piece's start route measure.
  A BLK with no lakes/splits is one piece `"{blk}:{int(mouth)}"`; a lake or curated split
  starts a new piece at its measure. Readable + debuggable.
- **lake:** `f"lake:{wbk}"` — one per lake/manmade `WATERBODY_KEY`, shared across every BLK
  touching it.

Both are stable when a split is added **elsewhere**. ABI consequences: authored `split.id`s are
stable keys; moving a split's *position* re-cuts geometry and changes the affected piece's start
measure (hence its id) — expected; a split *inside* a piece mints two new ids. (Hash form
`sha1(blk | lower | upper | lake_wbk)[:16]` remains available via `cutting.section_id` if a
measure-independent id is ever needed.)

## Serialization (07 partial-rerun model)

- **pickle** now for `blk_chains`, `StreamGraph` (geometry-free — small, adjacency-shaped), and
  the `geometries.pkl` sidecar (`node_id → shapely`); **GeoParquet** is the planned optimization
  for the bulk-geometry sidecar at province scale (memory-mappable, column/row-group pruning).
- Each artifact writes a sibling `*.meta.json` `{input_hashes, build_ts}`; `serialize.is_stale()`
  skips a step whose inputs are unchanged.
- Visual inspection is via the **GPKG** exporter (`export_gpkg.py`), not GeoJSON.

## Module layout (`pipeline/`)

`models.py` (schema) · `blk_chains.py` (load fids + merge, `edge_types`) · `names.py` ·
`graph.py` (inverted graph incl. **lake-node split** + `ancestors` + `build_section_geometries`
sidecar) · `anchors.py` + `splits.py` (04) · `border.py` (border + area-boundary cuts) ·
`sectionizer.py` (**curated** splits) · `tributaries.py` (section-level roll-up) ·
`cutting.py` (substring, node id, endpoint id)
· `serialize.py` (IO + cache) · `build.py` (end-to-end validation CLI) · `export_gpkg.py` +
`oneoff/` (one-off bootstrap scripts: `name_variants_compile.py`, `complex_regs_report.py`) ·
`run.py` (step entrypoints).

Tests: `pipeline/tests/` — `test_<module>.py`. `test_graph.py` holds synthetic pins
(mainstem-one-node, lake split, no-wetland-split, WSC filter, 2300 barrier) plus the two
real-data regressions (Chehalis/Harrison, Columbia/Kootenay), un-skipped and green.

## Reuse vs build-new

**Reuse:** `pipeline.common.utils.wsc.trim_wsc`; `pipeline.atlas.fwa.FWADataAccessor` (all GPKG reads
+ id normalization); the `"x_y"` endpoint convention; lake-wbk grouping + lake-outlet machinery
from `atlas/freshwater_atlas.py`; per-magnitude minzoom percentiles;
`effective_includes_tributaries`; `MatchTable`/`OverrideEntry`/`BaseEntry` (extend override
matching to target the node id / `location_identifier`).
**Build new:** `graph.py` (inverted graph + ancestors); `cutting.py` (substring + ids);
`anchors.py`/`splits.py` (cut-geometry resolver + sidecar); `sectionizer.py`; `tributaries.py`.
