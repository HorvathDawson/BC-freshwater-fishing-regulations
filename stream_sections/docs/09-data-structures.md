# 09 — Data Structures, Serialization, Layout

The **authoritative schema lives in code**: [`pipeline/sections/models.py`](../sections/models.py).
This doc records the rationale, the split-indexing rules, serialization choices, and the
folder layout so the code is understandable if revisited cold.

## Artifacts and their dataclasses (see `models.py`)

| Step (05) | Produces | Dataclass(es) | Rationale |
|-----------|----------|---------------|-----------|
| `blk-chains` | `blk_chains.parquet` | `BlkChain`, `FidSpan`, `WaterbodyRun`, `NameTuple` | Merge on **BLK** (1:1 w/ WSC). `mouth_measure` stored so `substring` offset math works when a chain doesn't start at 0. `WaterbodyRun` = under-lake sub-spans by `WATERBODY_KEY ∈ lakes∪manmade`. |
| `topology` | `topology.pkl` + `topology_segments.parquet` | `Topology`, `TopologyNode`, `Segment` | Contracted, **SCC-condensed** DAG. Geometry stripped from the pickle → fast graph load; segment geometry in companion parquet. `up_adj` = the upstream (tributary) walk. |
| `sections` | `sections.parquet` + `sections_graph.pkl` | `Section`, `SectionBoundary`, `SectionGraph` | The delivery spine, keyed by `section_id`. Fields are exactly the matchable set `08` needs. |
| `splits` (input+sidecar) | `splits.json` → `splits.resolved.json` | `SplitDef`, `SplitAnchor`, `SplitPoint` | Every anchor normalizes to `(blk, route_measure)`; resolved values written back for reviewable, deterministic builds. |
| `match` | `section_regs.parquet` | `SectionRegs` | One `reg_set_index` per section (Fraser handled by curated `mu_boundary` splits). Base regs are a separate `mu_id → base_reg_set` overlay, **not** here. |

## Split indexing (how a split lands in the graph)

Two distinct effects (matches `03` Step 5):

- **non-barrier split** (normal): a **section boundary only**. Not a topology node; does not
  split any `Segment`. It appears as a `SectionBoundary(kind=split)` on the two adjacent
  sections (the downstream section's `upper_bound` and the upstream section's `lower_bound`
  both reference `boundary_id = "split:{id}"`), and geometry is cut with `substring`. Flow /
  tributary reachability is untouched.
- **barrier split** (`barrier:true`, a dam): inserts a real `TopologyNode(kind=barrier,
  is_barrier=True)` and splits the underlying segment so the upstream walk stops there.

When a non-barrier split cuts a BLK **mid-segment**, that one topology segment is shared by
two sections; record each section's `(down_m, up_m)` sub-range (carried by its bounds'
`route_measure`) so the tributary walk still attributes the segment's upstream reach to the
correct section.

## `section_id` scheme (ABI — fix it once)

`section_id` hashes only the section's **two immediate bounds**:
`sha1(blk | lower.boundary_id | upper.boundary_id | lake_wbk)[:16]`, boundary ids from the
fixed domain `{outlet, headwaters, lake:{wbk}, split:{authored_split_id}}`. Or the readable
`f"{blk}:{int(lower_route_measure)}"` (lean choice — debuggable, equally stable). Both are
stable when a split is added **elsewhere** (an unrelated section's two bounds don't change).
Consequences to treat as ABI: authored `split.id`s are stable keys; moving only a split's
*position* re-cuts geometry but keeps the id; adding a split *inside* a section mints two new
ids (expected).

## Serialization (05 partial-rerun model)

- **GeoParquet** for bulk-geometry artifacts (`blk_chains`, `topology_segments`, `sections`):
  memory-mappable, column/row-group pruning (read `blk, wsc, fids` without paging geometry),
  reproducible bytes — essential at multi-GB with frequent partial reruns.
- **pickle** only for the two small graph containers (`Topology` without geometry,
  `SectionGraph`) — dict-of-dicts adjacency has no clean columnar shape and is small.
- Each artifact writes a sibling `*.meta.json` `{input_hashes, row_count, build_ts, git_sha}`;
  `serialize.is_stale()` skips a step whose inputs are unchanged.

## Module layout (created)

`pipeline/sections/`: `models.py` (schema) · `blk_chains.py` (S1) · `names.py` (S2) ·
`topology.py` (S3-4) · `anchors.py` + `splits.py` (04) · `cutting.py` (shared: substring,
section_id, endpoint id) · `sectionizer.py` (S5) · `tributaries.py` (S6) · `serialize.py` (IO
+ cache) · `run.py` (step entrypoints for `pipeline/__main__.py`).

Tests: `pipeline/tests/sections/` — one `test_<module>.py` per module (scaffolded, skipped),
incl. the two braiding/lake regression cases in `test_topology.py`.

## Reuse vs build-new

**Reuse:** `utils/wsc.trim_wsc`; `data/data_extractor.FWADataAccessor` (all GPKG reads +
id normalization); the `"x_y"` endpoint convention from `graph_builder.get_endpoints`;
lake-wbk grouping + lake-outlet machinery from `atlas/freshwater_atlas.py` /
`enrichment/feature_resolver.py`; per-magnitude minzoom percentiles;
`effective_includes_tributaries`; `MatchTable`/`OverrideEntry`/`BaseEntry` (extend override
matching to target `section_id`/`location_identifier`).
**Build new:** `cutting.py` (route-measure substring + id hashing); `topology.py` (SCC
condensation + lake collapse + contraction); `anchors.py`/`splits.py` (general anchor
resolver + sidecar); `serialize.py` (GeoParquet IO + meta cache).
