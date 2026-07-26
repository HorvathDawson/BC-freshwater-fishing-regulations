# 03 — The Graph Design (real, start here)

The concrete design for combining FWA into **one inverted stream graph**. Read `02` first for
terms. There is a **single graph** (no "fine vs coarse" split): a **node is a stream** (a
BLK-merged chain, later subdivided into sections); an **edge is "flows into"** (a tributary
draining its mainstem at a confluence measure). Tributaries of a node = its **ancestors**.

## Output artifacts

1. **`blk_chains`** — every blue line merged into one ordered chain (route measures,
   under-lake runs, `(name, source)` tuples).
2. **`stream graph`** — `StreamNode`s (one per BLK now; per section after splitting) + `FlowEdge`s
   (`from_node` flows into `to_node` at `at_measure`), with `up_adj` (incoming = tributaries)
   and `down_adj` (outgoing).
3. **`sections`** (next step, not yet built) — BLK nodes subdivided at lakes + curated splits,
   with **new cut geometry**, `location_identifier`, and lakes promoted to their own nodes.

## Ordering

`merge (S1) → names (S2) → split BLKs into sections (S3) → build the graph connecting sections
(S4) → tributary reachability (S5)`. Splitting happens **before/at graph building**, not
after. (The current implementation builds the graph at whole-BLK granularity; S3 splitting is
the next increment and refines each BLK node into a chain of section nodes.)

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

## Step 3 — Split BLK chains into sections (at lakes + curated splits)

Cut each BLK chain **only** at:
- **lake boundaries** — a BLK through a lake → downstream-of-lake + upstream-of-lake sections;
  the lake becomes its **own node** between them; the under-lake run belongs to the lake node.
- **curated splits** (`04`) — a cut line / polygon boundary; resolve to a route measure per
  crossed channel and cut geometry with `shapely.ops.substring`.

**Do not** cut at tributary confluences — that was the mistake that produced 361 segments for
Adams. A BLK with no lake/split stays **one** section node. `section_id` = the readable
`f"{blk}:{int(start_measure)}"` or the two-bound hash (`02`). **2-point-geometry trap**: the
0.69% of multi-segment BLKs that are entirely 2-point can't be vertex-cut — interpolate
(`cutting.substring_cut`). Recompute per-section minzoom from the section's own max magnitude.

## Step 4 — Build the inverted graph (nodes = sections, edges = flows-into)

For each BLK/section, find the stream it drains into: at its **mouth node** (the down_node of
its lowest-`down_m` fid), the fid whose **up_node == mouth** and whose BLK differs is the
downstream mainstem; its measure at that node is the **confluence measure**. Emit
`FlowEdge(from=tributary, to=mainstem, at_measure)`. A mainstem accrues **many incoming
edges** (its tributaries) and has **one outgoing edge** (its own mouth). At a lake, edges go
`stream → lake` and `lake → outlet stream`. Forks/distributaries (a side channel leaving and
rejoining) create rare cycles — tolerate with a visited set.

Once sections exist, a tributary edge attaches to the **specific section** whose measure range
contains the confluence, and consecutive sections of one BLK are joined by continuation edges.

**Why this is right:** the mainstem is one node regardless of tributary count; fids never
appear in the graph; and the **Chehalis→Harrison leak vanishes structurally** — the Harrison
is what the Chehalis *flows into* (a descendant), so it is never an ancestor. (Measured on the
Adams extent: Adams River = **1 node with 435 tributaries**, was 361 segments.)

## Step 5 — Tributary reachability = ancestors (+ two required guards)

Tributaries of a node = its **ancestors** (`graph.ancestors`, walking `up_adj`). Computed once
per node, reused by every regulation on it — replacing the per-regulation BFS over the 2.37 GB
micro-graph. Two guards from the spike (`10`) still apply on top of the closure:

- **WSC-descendant filter** — keep only ancestors whose `FWA_WATERSHED_CODE` is a descendant
  of the node's trimmed WSC (the drainage subtree). The inverted graph avoids the *confluence*
  leak by construction, but this still guards braided/distributary edge cases and multi-mouth
  tributaries. **Keep it** (`10` S1: 145/145 Chehalis, 0 Harrison).
- **`EDGE_TYPE=2300` barrier** — do not traverse through connector/canal nodes (Kootenay↔
  Columbia canal, blk 356366076). Lake-collapse does **not** replace this (the canal bypasses
  the lake polygon; `10` S2: leak 86→0). Keep the strict missing-edge_type guard.
- **Lake barrier** — once lakes are nodes, stop at a regulated lake (parameterizable:
  cross when the reg *is* the lake).

## Spike results (`10`) — settled

- ✅ **BLK is a safe atom** — 0.03% of named GNIS span >1 BLK; 99.995% contiguity.
- ✅ **Lake-transition cutting is bounded** — 84.7% single section, 96.6% ≤3, max 180.
- ❌ **SCC condensation is a no-op** (FWA already a DAG) → keep the **WSC filter**.
- ❌ **Lake-collapse ≠ 2300 rule** → keep the **2300 barrier**.
- ⚠️ **2-point geometry** — 0.69% of multi-segment BLKs need interpolation.

## Worked examples (acceptance targets)

- **Adams River + Adams Lake** → 2 section nodes (`downstream/upstream of Adams Lake`) with
  the lake as a node between them; replaces the duplicate override rows.
- **Fraser + Seabird channel** → Fraser node `(Fraser, gazette)`; side-channel node
  `(Seabird…, override)` + `(Fraser, side-channel)`; side channel drops at low zoom.
- **Similkameen** (crosses a zone, one reg set) → **one** node; zone handled as attribute /
  curated `mu_boundary` split only if regs differ (`07`).
- **Wigwam** (hand divide) → split at a `point`/`line` cut into two labelled sections.

## Current implementation status

`graph.py::build_stream_graph` implements S1/S2/S4 at whole-BLK granularity (validated:
8393 nodes / 8243 edges on the Adams extent, integrity OK). **Next:** S3 (split at lakes +
resolve curated splits → section nodes) and S5's guarded walk (`tributaries.py`).
