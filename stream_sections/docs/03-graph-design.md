# 03 — The Graph Design (real, start here)

The concrete design for combining FWA into **one inverted stream graph**. Read `02` first for
terms. There is a **single graph** (no "fine vs coarse" split): a **node is a stream** (a
BLK-merged chain, later subdivided into sections); an **edge is "flows into"** (a tributary
draining its mainstem at a confluence measure). Tributaries of a node = its **ancestors**.

## Output artifacts

1. **`blk_chains`** — every blue line merged into one ordered chain (route measures,
   under-lake runs, `(name, source)` tuples, distinct `edge_types`).
2. **`stream graph`** — `StreamNode`s (a **stream piece** = a BLK cut at its lake-runs; or a
   **lake** node, one per `wbk`) + `FlowEdge`s (`from_node` flows into `to_node` at
   `at_measure`, `kind` ∈ confluence/lake_in/lake_out), with `up_adj` (incoming = tributaries)
   and `down_adj` (outgoing). **Geometry-free**; a `geometries.pkl` sidecar maps `node_id →
   geom`.
3. **`sections`** (next step) — stream-piece nodes subdivided further at **curated** splits only
   (lakes are already handled in the graph step), with cut geometry + `location_identifier`.

## Ordering

`merge (S1) → names (S2) → build the graph, splitting BLKs at lakes into piece + lake nodes
(S3+S4) → curated splits (S5a) → tributary reachability (S5b)`. **Lake splitting happens in
the graph build** (from the fid `wbk`-run, no polygon); curated splitting is the only remaining
sectionizer step. Guards (WSC filter, 2300 barrier) are applied **during** the graph build.

## Step 1 — Merge into per-BLK chains

Group `streams` rows by `BLUE_LINE_KEY`, sort by `DOWNSTREAM_ROUTE_MEASURE` → a contiguous,
vertex-stitched chain mouth→source (99.995% share endpoints — verified; no snapping). Record:
`blk`, `fwa_watershed_code` (1:1), ordered `fids[]` with `[down_m, up_m]`, merged 2D geometry,
`mouth_measure`, `length_m`, `gnis`, `stream_order`/`stream_magnitude` (max), and **waterbody
runs** (sub-spans where `WATERBODY_KEY ∈ lakes∪manmade`; double-line rivers stay open). Skip
`999-999999…` sentinel WSC/BLKs. **Merge on BLK** — the one reliable single-path key.

## Step 2 — Name resolution → `(name, source)` tuples

Per BLK (using the `(WSC, BLK)` index): `override` (from `feature_display_names.json`) →
`gazette` (direct GNIS) → `side-channel` (unnamed BLK inherits the same-WSC main-channel name;
gives the Seabird channel its `(Fraser, side-channel)`) → `upstream-inherited` (nearest
upstream named edge; deferred — needs the graph). Keep the ordered list; display = top by
priority, search = all. (upstream-inherited is the one deferred piece — it runs after S4.)

## Step 3 — Split BLKs at lakes (in the combine, from the fid `wbk`-run)

Assign every fid an **owner**: a fid whose `WATERBODY_KEY ∈ lakes∪manmade` belongs to that
lake's node (`"lake:{wbk}"`); a contiguous run of non-lake fids is a **stream piece**
(`"{blk}:{int(down_m)}"`). A lake fid **breaks** the piece, so a BLK threading a lake becomes
below-lake piece + lake node + above-lake piece. Wetlands are **not** lake wbks → no split
(verified: an extent with 3138 lake fids has 4819 wetland fids). No polygon geometry needed —
the fids already terminate at the lake edge. **Do not** cut at tributary confluences (that was
the 361-segment mistake). A BLK with no lake stays **one** piece.

Node growth is bounded (Adams extent: 8393 BLKs → 9254 pieces + 1567 lakes; 7773 BLKs stay one
piece, max 8). Piece geometry = `substring` of the merged BLK line over `[down_m, up_m]`; lake
geometry = the stitched under-lake fid lines. **2-point trap**: the 0.69% all-2-point BLKs
interpolate (`cutting.substring_cut`).

## Step 4 — Build the inverted graph (nodes = pieces/lakes, edges = flows-into)

Edges come from **fid endpoint incidence**: at a coordinate `C`, every owner with a fid whose
**downstream** end is `C` (it sits above `C`) flows into the owner whose **upstream** end is `C`
(it sits below `C`). Same-owner boundaries — a piece's internal fid joints, and a mainstem
spanning a confluence — emit **no** edge, so a mainstem stays **one node** with many incoming
tributary edges; a tributary/lake boundary emits one edge. A lake node therefore accrues its
**inlets** as `up_adj` and its **outlet(s)** as `down_adj` (multi-outlet is fine — 0.5% of
lakes). Edge `kind` = confluence / lake_in / lake_out. Forks create rare cycles — tolerated
with a visited set; edges are sorted for deterministic output.

**Guards applied here (not deferred):**
- **WSC-descendant filter** on **stream→stream** edges only: create `T→M` iff
  `T.wsc.startswith(M.wsc)`. Drops braiding-reversed and cross-watershed edges at creation
  (only 7/8243 Adams edges — all builder mis-picks, 0 real tributaries). Lake-incident edges
  skip the filter (lakes are legitimate junctions).
- **`edge_types` preserved** through the merge so a `2300` piece is `is_barrier`.

**Why this is right:** the mainstem is one node per lake-bounded reach regardless of tributary
count; fids never appear in the graph; the **Chehalis→Harrison leak vanishes structurally**
(the Harrison is what the Chehalis *flows into* — a descendant, never an ancestor). Validated:
Adams Lake's outlet is the Lower Adams and its inlet the Upper Adams; Lower Adams' ancestors
reach the whole watershed **through** the lake node — the up/down-of-lake split for free.

## Step 5 — Tributary reachability = ancestors (guards already in the graph)

Tributaries of a node = its **ancestors** (`graph.ancestors(guarded=True)`, walking `up_adj`).
Computed once per node, reused by every regulation on it — replacing the per-regulation BFS
over the 2.37 GB micro-graph. Because the WSC filter is already baked into the edge set, the
raw closure is free of braiding/cross-watershed leaks; the walk applies the remaining guard:

- **`EDGE_TYPE=2300` barrier** — `ancestors(guarded=True)` does not include or traverse a
  `is_barrier` node (the Kootenay↔Columbia canal, blk 356366076). Once lakes are nodes the
  canal drains into a **lake** node, so the WSC filter (stream-stream only) no longer touches
  that edge — **the 2300 barrier is the operative guard there** (spike `10` S2 upheld).
- **Lake barrier** — lakes are nodes; a lake regulation can choose to stop or cross the walk
  (parameterizable: cross when the reg *is* the lake). The `sections`/`match` step owns this.

## Spike results (`10`) — settled

- ✅ **BLK is a safe atom** — 0.03% of named GNIS span >1 BLK; 99.995% contiguity.
- ✅ **Lake-transition cutting is bounded** — 84.7% single section, 96.6% ≤3, max 180.
- ❌ **SCC condensation is a no-op** (FWA already a DAG) → keep the **WSC filter**.
- ❌ **Lake-collapse ≠ 2300 rule** → keep the **2300 barrier**.
- ⚠️ **2-point geometry** — 0.69% of multi-segment BLKs need interpolation.

## Worked examples (acceptance targets)

- **Adams River + Adams Lake** → Lower Adams piece + Adams Lake node + Upper Adams piece **from
  the lake split alone** (no curated split); replaces the duplicate override rows. Validated:
  lake outlet = Lower Adams, Adams inlet = Upper Adams.
- **Fraser + Seabird channel** → Fraser node `(Fraser, gazette)`; side-channel node
  `(Seabird…, override)` + `(Fraser, side-channel)`; side channel drops at low zoom.
- **Similkameen** (crosses a zone, one reg set) → **one** node; zone handled as attribute /
  curated `mu_boundary` split only if regs differ (`07`).
- **Wigwam** (hand divide) → split at a `point`/`line` cut into two labelled sections.

## Curated splits run BEFORE the tributary walk (so a section is a node)

`sectionizer.split_graph_at` subdivides a piece node at each curated `SplitPoint` (P_low keeps
its id, P_high = `"{blk}:{int(M)}"`), rewires incoming tributary edges by `at_measure`, adds a
`continuation` edge P_high→P_low, and labels each piece via the 04 table. `build.py` applies it
**right after the graph build, before persisting** — so every tributary walk sees sections.

This is what makes **"tributaries of X between A and B"** work: with A and B as split nodes, the
A–B reach is a node, and `tributaries.tributaries_between(section)` = `ancestors(section)` minus
the upstream mainstem entering at B (its `continuation`/`lake_out` edge + that subtree) — i.e.
only the side tributaries joining inside A–B. Pure set arithmetic, only possible because the
section is a real node.

## Current implementation status

`graph.py::build_stream_graph` implements S1–S4 **including lakes as nodes** and both guards
(validated: 10821 nodes = 9254 pieces + 1567 lakes / 10929 edges on the Adams extent, integrity
OK; Adams up/down-of-lake split confirmed). `build_section_geometries` writes the geometry
sidecar. `sectionizer.py` applies curated splits (point/line anchors; validated on real Adams
geometry — a mid-fid cut split 11705 m → 5853 + 5853). `tributaries.py` provides the guarded
closure + `tributaries_between`. **Next:** remaining anchor resolvers (lake/confluence/
mu_boundary), `location_identifier` uniqueness validation, then the match step (`08`).
