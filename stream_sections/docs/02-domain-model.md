# 02 — Domain Model & Terms (verified)

All claims here verified against `data/bc_fisheries_data.gpkg` (`streams` = 4,907,441
features) and source on 2026-07-25. This replaces any earlier looser terminology.

## The FWA identifiers — what each really keys

| Term | Keys / identifies | Cardinality facts |
|------|-------------------|-------------------|
| **LINEAR_FEATURE_ID** (fid) | The **atomic polyline segment** (one gpkg row). | Unique (0 dupes in 600k). |
| **BLUE_LINE_KEY** (blk) | **A single continuous flow path** ("blue line"), mouth→source. Main channel and a side channel are **different BLKs**. | Many fids per BLK: mean 2.83, median 1, **max 1,384**. |
| **FWA_WATERSHED_CODE** (wsc) | Hierarchical position in the drainage tree (21×6-digit groups; trailing `000000` padding). | **BLK→WSC is strictly 1:1** (0 exceptions in 211,781 BLKs). **WSC→BLK is 1:1 except ~0.85%** — those are side channels sharing a main channel's WSC. |
| **GNIS_ID / GNIS_NAME** | The gazetted name of a **named main channel**. | NULL on most unnamed segments and on side channels. |
| **WATERBODY_KEY** (wbk) | The **polygon waterbody** (lake/river/wetland/manmade) a flow line is routed *through*. | The join key to lakes. Set on ~all through-water edges; ~7% of single-line edges. |
| **STREAM_ORDER** | Strahler order. | — |
| **STREAM_MAGNITUDE** | Shreve magnitude (# upstream sources). Drives minzoom. | — |
| **EDGE_TYPE** | FWA flow-line *class* (1000 single-line, 1100/1050 side/secondary, 1200/1250 double-line river banks, 1350–1475 lake/connector through-flow, 2300 misc connector). A category, **not** an id. | — |
| **DOWNSTREAM/UPSTREAM_ROUTE_MEASURE, LENGTH_METRE** | Metres along the blue line from the mouth. `UP − DOWN == LENGTH == 2D geom length` (verified to ~1 cm). | **Enables cutting geometry at any point.** |
| **WATERSHED_CODE_50K** | Legacy 1:50K code (different scheme). | Kept for cross-ref only. |

### Consequences that drive the design

1. **Merge on BLK, not WSC, not GNIS.** BLK is the one reliable "single stream path" key
   (1:1 with WSC, carries the mainstem/side-channel distinction). WSC is shared by side
   channels, so "same WSC ⇒ same stream" is **false**. GNIS is missing on side channels.
2. **A fid is not a section boundary.** Segments split at topology events (confluences,
   side-channel junctions, waterbody entry/exit) — not at our chosen split points, and a
   single fid can be **12.5 km** long. So sections do **not** align to whole fids; we
   **re-cut geometry** using route measures. (This corrects the earlier draft's "a section
   is an ordered set of whole fids" — wrong.)
3. **Side channels are separate BLKs sharing the main channel's WSC.** This is exactly the
   `(WSC, BLK)` grouping the current `graph_builder.propagate_names_by_watershed` uses, and
   the hook for `(name, source)` tuples.
4. **Under-lake = `WATERBODY_KEY ∈ lakes∪manmade`** (verified: `freshwater_atlas.py:1071`),
   **not** edge_type. Double-line rivers (1200/1250) also carry a waterbody_key but to a
   *river* polygon, so they must NOT collapse as lakes.
5. **The network flows downstream and is a near-DAG** rooted at ocean/border/lake outlets.
   Native vertex order = downstream→upstream (`coords[0]` = mouth). `outdegree==0` = outlets,
   `indegree==0` = headwaters. Braided/side channels create parallel BLKs and small cycles
   at braided reaches — traversal must be cycle-tolerant.

## The `(name, source)` tuple model

Today names collapse to a single scalar `gnis_name`/`gnis_id`. We replace that with an
ordered list of **`(name, source)`** tuples per BLK/section. Sources (priority high→low for
display; all indexed for search):

| source | meaning | example |
|--------|---------|---------|
| `override` | manual `feature_display_names.json` | "Seabird Island North Side Channel" |
| `gazette` | direct GNIS on this BLK | Fraser main channel → ("Fraser River", gazette) |
| `side-channel` | inherited from the same-WSC main-channel BLK | Seabird channel also gets ("Fraser River", side-channel) |
| `upstream-inherited` | nearest upstream named edge (unnamed headwater runs) | ("Fraser River", upstream-inherited) |

Display picks the highest-priority tuple; search indexes every tuple; the client can render
"Tributary of / Channel of X" from the source tag. This generalizes the current
`name_variants`-with-provenance (built late in `reach_builder`) by computing it **at graph
build time**, where `(WSC, BLK)` already discriminates the cases.

Normalization note: GNIS_ID is float in raw data (`3429.0`) → str via `int(float(x))`
(`data_extractor.py:104`). Preserve this so keys match. Endpoint node identity is the
rounded `"x_y"` string (3 dp, `graph_builder.py:71`). Access data only through
`FWADataAccessor`.

## One graph — nodes are streams, edges are "flows into"

There is a **single inverted graph** (an earlier draft's "two granularities" is abandoned):

- A **node** is a **stream piece** — a BLK cut at its lake-runs (and, later, curated splits),
  NOT at every tributary confluence — **or a lake** (one node per `wbk`). Most BLKs → one
  piece; a BLK through a lake → below-lake piece + lake node + above-lake piece. **Geometry is
  not on the node** — a `node_id → geom` sidecar holds it, so the graph is pure topology.
- An **edge** is **"A flows into B"** — the upstream node A drains into B at a confluence
  measure on B (`kind` = confluence / lake_in / lake_out). A mainstem piece has **many incoming
  edges** (its tributaries) and one outgoing; a lake's incoming edges are its **inlets**, its
  outgoing its **outlet(s)**.

So the mainstem is **one node per lake-bounded reach** regardless of tributary count — no
per-confluence segmentation, fids never appear in the graph. `location_identifier` is null
unless a lake/split subdivided the BLK.

**Tributaries of a node = its ancestors** (walk incoming edges upstream). Because each stream
flows into exactly one downstream stream, the confluence-parent leak is avoided by construction
(the Harrison is what the Chehalis flows *into* — a descendant, never an ancestor). Guards are
applied **at graph-build time** (`10`): the **WSC-descendant filter** on stream→stream edges
(braided/cross-watershed cases; drops the edge at creation) and the **`EDGE_TYPE=2300` barrier**
(`is_barrier` nodes, stopped by `ancestors(guarded=True)`). With lakes as nodes the
Columbia/Kootenay canal drains into a lake node, so the 2300 barrier — not the WSC filter — is
the operative guard there.

## Section / node identity

- **Chosen id (ABI):** the **readable** form —
  - stream piece: `f"{blk}:{int(down_m)}"` (BLK + start route measure from the mouth);
  - lake: `f"lake:{wbk}"`.

  Stable when a split is added *elsewhere*; a split *inside* a piece re-cuts it and changes the
  affected piece's start-measure id (expected). A measure-independent hash
  `sha1(blk | lower | upper | lake_wbk)[:16]` stays available (`cutting.section_id`) if ever
  needed. Never derive an id from fid order.
- **Display identity:** `(name_tuples, location_identifier, lake_wbk)` — "gnis name / location
  identifier (null if no splits) / lake wbk".

## Lakes

- One **WATERBODY_KEY = one logical lake** (only `wbk ∈ lakes∪manmade` — wetlands are NOT
  noded). **One graph node per lake** (`"lake:{wbk}"`); every BLK carrying that wbk attaches to
  it. Under-lake fids are the lake node's members; their stitched geometry is the lake node's
  entry in the geometry sidecar (the `under_lake_streams` render layer).
- **Inlet/outlet fall out of graph adjacency** — `up_adj(lake)` = inlets, `down_adj(lake)` =
  outlet(s) (multi-outlet is fine; 0.5% of lakes). No fid-incidence scan needed.
- **Name:** the lake's own `GNIS_NAME_1` when present (null ~96.7% of the time) → else a
  threading river's name; through-river GNIS names are kept as `through_names` metadata.
- A lake is a natural flow junction; whether a lake *stops* the tributary walk is a
  regulation-time choice (see `03`).

## OSM — still no

OSM lacks `fwa_watershed_code`, `blue_line_key`, `waterbody_key`, order/magnitude, and clean
directionality — every join key matching depends on. Combining is a solved, one-time,
build-time cost. Stay on FWA.
