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

- A **node** is a **stream** — a BLK-merged chain, subdivided into **sections** only at lakes
  and curated splits (NOT at every tributary confluence). Most BLKs → one node; a BLK through
  a lake → two section nodes + a lake node.
- An **edge** is **"A flows into B"** — the tributary/upstream node A drains into B at a
  confluence measure on B. A mainstem node therefore has **many incoming edges** (its
  tributaries) and **one outgoing edge** (its own mouth).

So the mainstem is **one node** regardless of how many tributaries join it (Adams River: one
node, 435 tributaries) — no per-confluence segmentation, and fids never appear in the graph.
`location_identifier` is null unless a lake/split subdivided the BLK.

**Tributaries of a node = its ancestors** (walk incoming edges upstream). Because each stream
flows into exactly one downstream stream, the confluence-parent leak is avoided by
construction (the Harrison is what the Chehalis flows *into* — a descendant, never an
ancestor). Two guards still apply on the closure (`10`): the **WSC-descendant filter** (keep
ancestors within the drainage subtree — braided/multi-mouth edge cases) and the
**`EDGE_TYPE=2300` barrier** (connector/canal nodes), plus a **lake barrier** once lakes are
nodes.

## Section identity

- **Stable id:** hash over the section's **two immediate bounds** only:
  `section_id = sha1(blk | lower.boundary_id | upper.boundary_id | lake_wbk)[:16]`, where
  each `boundary_id` is a stable string from a fixed domain (`"outlet"`, `"headwaters"`,
  `"lake:{wbk}"`, `"split:{authored_split_id}"`). Hashing only the two bounds is what keeps
  ids stable when a split is added *elsewhere* on the river (an unrelated section's bounds
  don't change); adding a split *inside* a section legitimately mints two new ids. Never hash
  from fid order.
  - **Debuggability option:** a human-readable form `f"{blk}:{int(lower_route_measure)}"`
    (BLK + start measure from the mouth) is equally stable under unrelated splits and far
    easier to trace in logs. Pick one and keep it fixed — it's an ABI. (Lean: the readable
    form; note that renaming an authored `split.id` or moving a *downstream* bound changes
    ids either way, so treat split ids + bounds as stable keys.)
- **Display identity:** `(name_tuples, location_identifier, lake_wbk)` — the user's
  "gnis name / location identifier (null if no splits) / lake wbk".

## Lakes

- One **WATERBODY_KEY = one logical lake** (grouped at `freshwater_atlas.py:366-420`).
- **Collapse the whole lake to a single graph node**; every BLK whose flow lines carry that
  waterbody_key attaches to it. Under-lake connector segments are absorbed into the lake
  node (retained separately only as render geometry for the `under_lake_streams` layer).
- **Inlet/outlet is not a field** — derive: outlet = most-downstream node of the lake's flow
  lines (`outdegree` leaving the wbk set / lowest route measure); inlets = the rest.
- A lake node is a **tributary barrier** (see `03`).

## OSM — still no

OSM lacks `fwa_watershed_code`, `blue_line_key`, `waterbody_key`, order/magnitude, and clean
directionality — every join key matching depends on. Combining is a solved, one-time,
build-time cost. Stay on FWA.
