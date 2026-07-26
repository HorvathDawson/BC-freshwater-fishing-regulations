# 03 — The Graph Design (real, start here)

This is the concrete design for combining FWA into the section graph. Read `02` first for
terms. Every step cites the verified data model. This is the foundation everything else
(sections, tiles, matching, storage) builds on.

## Output of this stage (target artifacts)

1. **`blk_chains`** — every blue line merged into one ordered chain with route measures +
   `(name, source)` tuples.
2. **`topology`** — the fine contracted graph: nodes = confluences + lake nodes + ends;
   edges = segments (BLK-runs between nodes), directed downstream, cycle-tolerant.
3. **`sections`** — the coarse matching/display units: BLK-runs cut only at lakes + hand
   splits, with **new cut geometry**, name tuples, aggregates, and **tributary_section_ids**.

## Step 1 — Merge into per-BLK chains

Group `streams` rows by `BLUE_LINE_KEY`. Within a BLK, **sort by
`DOWNSTREAM_ROUTE_MEASURE`** → a contiguous, vertex-stitched chain mouth→source (consecutive
fids share an exact endpoint vertex; no geometric snapping needed — verified). Per chain
record:

- `blk`, `fwa_watershed_code` (1:1), ordered `fids[]` with their `[down_m, up_m]` route
  spans, merged 2D geometry, total `length_m`.
- `gnis_id`/`gnis_name` if present; `stream_order` (max), `stream_magnitude` (max — drives
  minzoom).
- **waterbody runs**: the sub-spans where `WATERBODY_KEY ∈ lakes∪manmade` (under-lake) vs
  open channel, plus which wbk. (Double-line river wbks stay "open".)

Skip the `999-999999…` sentinel WSC/BLKs (`graph_builder.py:147`).

**Why BLK-first:** it is the one reliable single-path key; keeps side channels separable for
tile zoom-drop; 1:1 with WSC so no ambiguity; matches how the geometry is already stitched.

## Step 2 — Name resolution → `(name, source)` tuples

Compute per BLK (using the `(WSC, BLK)` index — the discriminator verified in `02`):

1. `override` — if any of this BLK's fids/blk/wbk is in `feature_display_names.json`, add
   `(display_name, override)` and each `name_variant` as `(variant, override)`.
2. `gazette` — if the BLK has GNIS, add `(gnis_name, gazette)`.
3. `side-channel` — if this BLK is **unnamed** but its WSC has a **named sibling BLK** (the
   main channel), add `(main_name, side-channel)`. (This is the current
   `propagate_names_by_watershed`, but emitting a *tagged tuple* instead of overwriting.)
   → gives the Seabird channel its `(Fraser River, side-channel)` variant.
4. `upstream-inherited` — for still-unnamed BLKs, BFS upstream along same-WSC edges to the
   nearest named edge (current `annotate_unnamed_context`), add `(name, upstream-inherited)`.

Keep the whole ordered list. This is where the current 3-layer propagation lives — port it
here, once, tagged. Display = highest-priority tuple; search = all tuples.

## Step 3 — Build the topology graph (fine)

**Nodes:**
- **Endpoint nodes** at every `"x_y"` (3 dp) where BLK segments meet. Collapse degree-2
  pass-through endpoints (no other BLK attaches) so a BLK run between real junctions is one
  edge.
- **Lake nodes** — one per lake/manmade `WATERBODY_KEY`. Every BLK endpoint touching that
  wbk connects to this single node (the "collapse lake to one node where all in/out BLKs
  meet"). Under-lake connector segments are absorbed into the node.
- **End nodes** — outlets (`outdegree==0`) and headwaters (`indegree==0`).

**Edges = segments:** a maximal BLK-run between two nodes, directed **downstream**
(upstream_node → downstream_node), carrying `blk, wsc, gnis, order, magnitude, edge_type,
route span, geometry`. Keep **reverse adjacency** for upstream walks.

**Confluence-parent leak — KEEP the WSC-hierarchy filter (empirically required, `10` S1).**
The spike found FWA is **already a DAG** (0 non-trivial SCCs), so SCC condensation is a
**no-op** — it does nothing and does not help. The Chehalis→Harrison leak is *not* a braiding
cycle; it is **confluence topology**: the Chehalis *mouth* node's upstream predecessors
include the Harrison *mainstem* segment, so a naive reverse walk climbs the parent river
(measured: 24/43 Harrison segments leaked into a Chehalis-seeded walk).

The correct, principled fix (measured: 145/145 Chehalis, 0 Harrison): constrain the upstream
walk to the **FWA watershed-code hierarchy**. A tributary of a stream is a segment whose
`FWA_WATERSHED_CODE` is a **descendant** (prefix-extension) of the stream's trimmed WSC. That
*is* the drainage subtree — not a hack. **Keep** the legacy excluded-WSC / parent-WSC logic;
reframe it as "stay within this stream's WSC subtree." (Contraction of degree-2 pass-through
nodes into maximal BLK-runs still happens; there just are no SCCs to condense.)

## Step 4 — Barriers baked into the graph

The old runtime BFS rules become **structural** here:

- **Lake barrier** — a lake node stops upstream tributary propagation (don't cross into a
  lake's inlets unless the seed *is* that lake). Structural, replaces the "set of all
  regulated wbks" hack. Walk is parameterizable: `cross_lakes=False` normally,
  `True`/seed-relative when the reg is the lake itself.
- **Connector / edge_type 2300 — KEEP the barrier rule (lake-collapse does NOT replace it,
  `10` S2).** The spike found the Kootenay↔Columbia canal **bypasses** Columbia Lake's
  polygon: the 6 `2300` bridge segments (blk 356366076, WSC 300-625474) sit under *other*
  waterbodies (unnamed lake 328966159, wetland 329524174), so collapsing Columbia Lake
  catches **0/6** of them and stops the leak only by geometric accident (severing an unrelated
  `1450` seg). Blocking the `EDGE_TYPE=2300` segments is the precise, reliable barrier
  (measured leak 86→0). **Keep** the 2300 "enter-but-don't-exit" rule and the strict
  missing-edge_type guard. Also note the bridge shares the *Kootenay's* WSC, so the WSC filter
  alone can't separate them — the 2300 rule is independently necessary. Lake-collapse remains
  useful for the *common* lake barrier — it is a complement, not a replacement.
- **Mainstem backtracking — NOT eliminated by directionality alone** (spike-corrected). When
  a reg seeds at/near a confluence (e.g. the Chehalis mouth), the reverse walk climbs the
  parent mainstem because the confluence node has the parent as an upstream predecessor. The
  **WSC-descendant filter** (above) is what stops it. **Keep it.**
- **Hand-split barrier** — an optional `barrier:true` split (a dam) becomes a barrier node
  too (see `04`).

## Step 5 — Sections (coarse) + geometry re-cut

For each BLK chain, cut it into sections **only** at:
- **lake boundaries** (a BLK through a lake → downstream-of-lake section + upstream-of-lake
  section; the under-lake run belongs to the lake node, not a section), and
- **hand-defined split points** (`04`), resolved to a route measure and cut with
  `shapely.ops.substring(geom, start−mouth_measure, end−mouth_measure)`.

Do **not** cut at tributary confluences.

**Split → graph effect (two kinds):**
- **non-barrier split** (the normal case, e.g. Adams Lake, a landmark): a **section
  boundary only** — it does *not* become a topology node and does *not* split any segment.
  Flow/tributary reachability is unchanged; it only cuts section geometry + names the pieces.
- **barrier split** (`barrier:true`, e.g. a dam): inserts a real barrier topology node and
  splits the underlying segment so the upstream walk stops there.

**Geometry-cut trap:** some FWA rows fall back to 2-point LineStrings; those cannot be
measure-cut precisely — fall back to the nearest micro-segment (fid) boundary at/above the
target measure. Validate cut coverage == BLK coverage with no gaps. Each section gets:
- `section_id = hash(blk, ordered_boundary_ids, lake_wbk)`,
- `name_tuples`, `display_name`, `location_identifier` (auto-generated per `04`), `lake_wbk`,
- `fwa_watershed_code`, `stream_order`, `max_magnitude` → **per-section minzoom**
  (recompute the percentile from the section's own max magnitude, not per-BLK),
- **new cut `geometry`** + `bbox` + `length_km`,
- `member_segment_ids` (which topology edges it covers),
- `zone_reg_map` placeholder (filled at match stage, `07`).

Lakes themselves are section-like display units too (a polygon "section" keyed by wbk), so
regulations on a lake attach to the lake node/section.

## Step 6 — Tributary reachability → `tributary_section_ids`

For each section, walk the **topology graph** upstream from all nodes spanned by the
section, collecting segments, honoring barriers (Step 4). Map collected segments → their
owning sections → `tributary_section_ids`. Cache by seed-set (as the current enricher does).

This is the "use the tree to find all upstream items." It replaces the per-regulation BFS
over 2.37 GB: now it is a walk over the small contracted graph, computed once per section and
reused for every regulation that lands on that section.

## Worked examples (acceptance targets)

- **Adams River + Adams Lake** → two sections: `(Adams River, "downstream of Adams Lake")`
  and `(…, "upstream of Adams Lake")`, split at the lake node. Replaces the duplicate
  `overrides.json` rows. Tributary walk from the upstream section stops at the lake.
- **Fraser + Seabird Island channel** → main Fraser BLK = section with `(Fraser, gazette)`;
  the side-channel BLK = its own section with `(Seabird…, override)` + `(Fraser,
  side-channel)`; both present at high zoom, side channel drops at low zoom.
- **Similkameen River** (crosses a zone, one reg set) → **one** section spanning the zone
  boundary; zone handled as attribute (`07`), not a geometry split.
- **Wigwam River** (hand divide at a route point) → split via `linear_feature_id`/route
  measure into two sections with generated `between/upstream/downstream` labels.

## Spike results (`10`) — what's settled vs still open

Settled by the empirical spike:
- ✅ **BLK is a safe atom** — only 0.03% of named GNIS span >1 BLK; sections need never
  combine BLKs. Segments are 99.995% contiguous.
- ✅ **Lake-transition cutting is bounded** — 84.7% of BLKs are a single section, 96.6% ≤3,
  max 180.
- ❌ **SCC condensation is a no-op** (FWA already a DAG) → **keep the WSC-hierarchy filter**.
- ❌ **Lake-collapse does not replace the 2300 rule** → **keep the 2300 barrier**.
- ⚠️ **2-point geometry**: 12.97% of segments, but only 0.69% of multi-segment BLKs are
  *entirely* 2-point — those need interpolation, not vertex-cut. Handle in `cutting.py`.

Still to verify during build:
1. Lake-node collapse leaves no orphaned under-lake segments / no double-counting.
2. `substring` cut aligns to ~cm on real hand splits (incl. the 2-point fallback path).
3. Full-run section count + tile coverage with no gaps.
