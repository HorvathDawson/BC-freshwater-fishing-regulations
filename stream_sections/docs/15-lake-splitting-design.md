# Lake Subdivision — Sub-Region Overlay (future-option design)

**Status: NOT to be built yet.** This is a keep-the-option-open design. It records *how* we
would subdivide a lake into named/regulated sub-regions when we decide to, and — most
importantly — insists that whatever we build **plays as nicely with matching as stream sections
already do**. Nothing here changes the graph, the sectionizer, or the current split model. It is
a plan we can pick up later without repainting ourselves into a corner now.

> Where this doc and the code disagree once implementation starts, the code + `splits.schema.md`
> win. This is a *design*, verified against today's `graph.py` / `models.py` / `border.py` /
> `sectionizer.py` / `tributaries.py`, not a spec of shipped behaviour.

---

## 1. Purpose & status

A stream section answers "which *reach* of a river does this rule cover?". A growing set of
regulations instead scope to **part of a lake** — a rule that closes or annotates a *region*
of the waterbody, leaving the rest open:

- **A line across the lake** — "easterly of a line drawn from a boundary sign…", "north of a
  line between the two shore signs". The line partitions the polygon into two portions.
- **A radius / distance from a feature** — "within 100 m of the outlet", "within a 400 m radius
  of the mouth of Pinkut Creek", "within 100 m of the weir". A circle (buffer) partitions the
  polygon into an inner region and the rest.
- **A named bay or vague/mapped area** — "in the outlet bay within 100 m of the head of the
  outlet stream", "Davis Bay (in Finlay Reach of Williston Lake)", "swimming areas", "see map on
  page 28 for area closure". No clean geometry exists; the sub-region must be authored by hand or
  left map-only.

**Counts** (from `docs/14-locators-to-curate.md`, the curated-locator inventory; the doc groups
by *anchor kind*, these are the lake-relevant subsets, approximate):

| Lake sub-region shape | ≈ count | Source anchor kinds in doc 14 |
|-----------------------|--------:|-------------------------------|
| Line across a lake | ~27 | most of `line_between_signs` (31) applied to a lake, + a few `boundary_signs_generic` |
| Radius / buffer / outlet-bay | ~19 | most of `radius_buffer` (23) + `lake_inlet_outlet` (5) that resolve to a distance region |
| Named bay / vague / map-only | ~13 | the lake share of `map_or_vague` (26) + `area_park_polygon` (18) that name a bay |

So ~46 lake regs are *mechanically* authorable (line + buffer) and ~13 stay manual. These are the
regs that a stream section **cannot** represent, because a lake is a 2-D region, not a 1-D reach.

---

## 2. Why lakes break the current split model

The stream sectionizer is **1-D linear referencing**, end to end:

- A curated split is a `SplitPoint{blk, route_measure}` (see `models.py`, `anchors.py`). Every
  anchor — point, line, confluence, mu_boundary, area_boundary — resolves to a single absolute
  `DOWNSTREAM_ROUTE_MEASURE` on a target BLK.
- `sectionizer._split_one` subdivides a stream node with `substring(g, lo, M)` and reattaches the
  incoming tributary edges by their confluence `at_measure` (`< M` → lower piece, `≥ M` → upper).
  Everything is ordered along the mouth→source route.

A lake has **no route measure**. Worse, the lake node itself does not even carry the lake
polygon:

- A `NodeKind.lake` node (`graph.py::build_stream_graph`, the `lake_fids` loop) is built from the
  fids routing **through** the lake — its `member_fids` are the under-lake channel segments. In
  the geometry sidecar (`build_section_geometries`) its geometry is the **stitched channel line**
  (`cutting.merge_ordered(...)`), *not* the lake polygon. It has no `down_m`/`up_m`.
- The lake **polygon** (keyed by `wbk`) is **not carried in the graph at all**. It lives only in
  the source `lakes` / `manmade` layer. The graph build only receives `lake_kind` (wbk → "lake"|
  "manmade"), `lake_names`, `lake_overrides` — never the geometry.
- Streams connect to a lake via `lake_in` / `lake_out` `FlowEdge`s, each carrying the confluence
  coordinate (`edge.x` / `edge.y`).

So we cannot reuse `_split_one`: there is no measure to cut at, and there is no polygon in hand to
cut. Lake subdivision is a **different geometric operation** on a **different geometry source**.

---

## 3. What we must represent

Whatever the model, four things have to exist:

1. **The lake polygon** — fetched by `wbk` from the `lakes` / `manmade` source layer at build
   time (the same layer `lake_kind` was derived from). This is the one new geometry input.
2. **A region-cut geometry** — the thing that carves the polygon:
   - a **line** (extend two shore-sign coords to fully cross the polygon), or
   - a **buffer circle** (a point + radius, for "within N m of …"), or
   - an **explicit sub-polygon** (an authored bay / swimming area with no derivable geometry).
3. **Sub-regions** — the resulting pieces, each with a **stable id** and a **`location_identifier`**
   (e.g. "north portion of Roche Lake", "within 100 m of the outlet", a named bay). Stable so the
   id is part of the section ABI (same contract as a `SplitDef.id`).
4. **Membership resolution by point-in-polygon** — given a clicked point (or any coordinate), which
   sub-region contains it; **and** given each inlet/outlet confluence coord (`edge.x/edge.y`),
   which sub-region that stream connection falls in. This is exactly the test `border._pieces_in_
   polygon` already runs for streams, at a finer granularity.

Point (4) is the crux: membership is a **containment fact**, computed geometrically, not a
topological cut. That is the same shape as the just-shipped `area_boundary` / `in_areas` pattern
(§4B), which is why (B) below is cheap.

---

## 4. Two models — compare, recommend (B)

### (A) True node subdivision — split the polygon into sub-region NODES

Cut the polygon into sub-region polygons, promote each to its own graph node, and re-plumb the
`lake_in` / `lake_out` edges by assigning each confluence to the sub-node that contains it.

**Why it fights the topology.** The lake node exists because a *through-channel* threads the
waterbody (its `member_fids` are that channel). That channel crosses **every** sub-region on its
way through. So "which sub-node carries the flow?" has no clean answer — you either duplicate the
channel across sub-nodes, or you introduce artificial sub-node→sub-node edges that don't
correspond to any FWA feature. Tributary walks (`ancestors`, `lake_tributaries`,
`tributaries_between`) all assume nodes are real flow features; sub-region nodes would need
special-casing throughout. Heavy, invasive, and it buys nothing the regs actually need — the regs
close/annotate a *portion*, they do not re-route water.

### (B) Sub-region overlay on ONE lake node — **RECOMMENDED**

Keep the single lake flow-node exactly as it is (topology untouched, `member_fids`, `lake_in`/
`lake_out`, `through_names`, tributary walks all unchanged). Attach to it a **list of sub-region
polygons**, each with a label / `location_identifier` and (later) its matched reg. Resolution:

- **Click / coordinate** → point-in-sub-polygon (`poly.contains(mp)`), the `border._pieces_in_
  polygon` core reused verbatim.
- **Inlet/outlet assignment** → run the same containment test on each `lake_in`/`lake_out` edge's
  `(x, y)` confluence coord, so "the outlet region" knows which stream is its outlet.
- **No flow re-plumbing.** Edges never move. There is exactly one lake node, as today.

This mirrors `area_boundary` → `in_areas` (a piece records which polygons contain it, a purely
geometric membership fact) but at **sub-lake granularity**: instead of "this stream piece is
*within* Garibaldi Park", it is "this lake *sub-region* is the north portion / the outlet buffer /
Davis Bay". The real regs only ever need this: they close or annotate **part** of a lake and
leave the rest open, so an overlay that can say "a clicked point is in region R, region R carries
reg X" is sufficient. We do **not** need sub-regions to participate in flow.

**Recommendation: (B).** It is additive, reuses existing point-in-polygon machinery, and keeps the
matcher lake-agnostic (§6).

---

## 5. Authoring shape — a `lake_region` split

A curated lake sub-region slots into `splits.json` the same way `area_boundary` does — a new
`AnchorType.lake_region` on `SplitAnchor`, resolved at build time. The `target` is the lake
(`wbk`, or a `gnis_id` that resolves to the lake's wbk). The anchor's `region` field selects the
carving method, and the resolver reads the lake polygon by `wbk` from the source layer.

> These are **jsonc sketches** of the *future* schema, not fields that exist today. `is_lonlat`
> follows the same convention as the existing point/line anchors (author in lon/lat, transform to
> EPSG:3005). Sub-region ids are `{split.id}:{slug}` so they are stable + part of the ABI.

### 5a. `line` — a line across the lake (two shore signs)

```jsonc
{
  "id": "roche-lake-north-closure",
  "target": { "wbk": "00100ROCH" },              // or gnis_id -> lake wbk
  "anchor": {
    "type": "lake_region",
    "region": "line",
    "coords": [[-120.514, 50.487], [-120.498, 50.491]],  // the 2 shore-sign coords
    "is_lonlat": true
  },
  "label": "north portion of Roche Lake"
}
```

Resolution: **extend** the 2 coords into a line long enough to fully cross the polygon (shapely's
`split` requires the splitter to cut clean through — a segment that stops inside the polygon does
nothing), then `shapely.ops.split(lake_poly, line)` → 2 sub-polygons. **Label the side by
GEOMETRY, not the prose.** Compute each sub-polygon's centroid and pick the side by centroid
position (e.g. the one whose centroid is further north gets the "north" label). Do **not** trust
the word "north" in the regulation text — cardinal words in the synopsis are unreliable, the same
lesson learned on the park inside/outside decision (point-in-polygon is the source of truth, not
the prose; see `border.mark_inside_area` / `location_identifier`'s `in_areas` note).

### 5b. `buffer` — within N m of a feature (outlet / mouth / weir)

```jsonc
{
  "id": "buckinghorse-outlet-100m",
  "target": { "wbk": "00200BUCK" },
  "anchor": {
    "type": "lake_region",
    "region": "buffer",
    "point": [-124.031, 56.118],                 // the outlet/mouth/weir coord
    "radius_m": 100,
    "is_lonlat": true
  },
  "label": "within 100 m of the outlet"
}
```

Resolution: `circle = point.buffer(radius_m)` (in EPSG:3005, metres). Two sub-regions:
`lake ∩ circle` (the "within N m" region) and `lake − circle` (the rest). The feature point can be
authored directly, or — for "within N m of *the outlet*" — derived from the lake node's
`lake_outlets` edge coord (`edge.x/edge.y`), so the buffer centres on the real confluence.

### 5c. `polygon` — an authored sub-polygon (named bay / swimming area)

```jsonc
{
  "id": "williston-davis-bay",
  "target": { "wbk": "00700WILL" },
  "anchor": {
    "type": "lake_region",
    "region": "polygon",
    "polygon": [[-124.20, 56.02], [-124.17, 56.02], [-124.17, 56.05], [-124.20, 56.05]],
    "is_lonlat": true
  },
  "label": "Davis Bay"
}
```

Resolution: the authored polygon **is** the sub-region (optionally clipped to the lake:
`lake ∩ polygon`); the complement is the rest of the lake. This is **Tier-2, manual** — for named
bays and "swimming areas" that have no derivable geometry. A human digitizes the bay on a map.

---

## 6. How it plays with MATCHING (must be as nice as streams)

This is the whole point of the design. **A lake sub-region must resolve for the matcher exactly
like a stream section does — no lake special-casing.**

A stream section carries an id + a `location_identifier` ("downstream of Adams Lake"); a reg
attaches to it via the **waterbody selector** (the name) **narrowed by the reg's locator** (the
reach). A lake sub-region carries the *same two things*:

- an **id** (`{split.id}:{slug}`, stable), and
- a **`location_identifier`** ("north portion of Roche Lake" / "within 100 m of the outlet" /
  "Davis Bay").

So the matcher narrows a lake the identical way it narrows a river reach: **waterbody selector
(the lake name) narrowed by the reg's locator (the sub-region label)**. "Roche Lake, north of the
line" resolves to `roche-lake-north-closure:north` exactly as "Adams River upstream of Adams Lake"
resolves to the above-lake piece. The narrowing pattern is one and the same — reach-on-a-stream and
region-on-a-lake are the same selector+locator shape.

Because membership is the `in_areas`-style geometric fact (§3.4, §4B), **area/park closures compose
over lake sub-regions too**: a lake wholly or partly inside a park can still be flagged the same
way a stream piece is, and a sub-region can be both "the outlet buffer" and "within Garibaldi
Park". The composition is set-membership, so it stacks.

**Spell it out for whoever builds the matcher:** the matcher should treat a lake sub-region as just
another selectable feature with an id + a locator label. It should **not** need a lake code path.
If a change to the matcher is required to support lakes beyond registering the new locator vocab
("north portion of…", "within N m of…", a bay name), the overlay model has been mis-implemented.

---

## 7. Coverage & tiers

- **Tier-1 (mechanical): `line` + `buffer`.** Covers ~46 of the lake regs (§1). Fully authorable
  from coords/radius in `splits.json`; deterministic; no map digitizing beyond reading two sign
  coords or one feature coord off a map.
- **Tier-2 (manual): `polygon` (named bays) + vague / map-only.** ~13 regs. Requires a human to
  digitize the bay, or stays **map-only** (the reg is shown on the map but not machine-scoped).
  Acceptable — these are rare and inherently ambiguous ("swimming areas", "see map on page 28").

Tiering matches the rest of the curation effort in doc 14: do the mechanical majority cheaply,
leave the vague tail manual.

---

## 8. Gotchas & open decisions (for when we build it)

- **shapely `split` needs the line to fully cross the polygon.** A shore-to-shore line authored
  from two sign coords usually stops *on* the shore; extend both ends outward (e.g. project along
  the line's bearing past the polygon bbox) before `split`, or `split` silently returns the
  polygon uncut.
- **Centroid-side labeling, not prose.** Assign "north"/"south"/"east"/"west" from sub-polygon
  centroid geometry, never from the regulation wording (§5a). Record the geometric basis so a
  reviewer can eyeball it, same spirit as `splits.resolved.json`.
- **Confluence → sub-region assignment by containment.** Assign each `lake_in`/`lake_out` edge to a
  sub-region via `poly.contains(Point(edge.x, edge.y))`. Decide the tie-break when a confluence
  sits on the cut line (nudge inward, or assign to the larger region deterministically).
- **Deterministic sub-region ids.** Prefer an **authored region slug** (`{split.id}:{slug}`, e.g.
  `roche-lake-north-closure:north`) over a geometry-derived key, so the id survives a polygon-data
  refresh. If a geometry-derived key is used, derive it from a stable property (centroid rounded /
  side), never from vertex order.
- **Lake-polygon source layer (`lakes` vs `manmade`).** The polygon must be fetched by `wbk` from
  the *correct* source layer — `lake_kind[wbk]` already tells the build whether a wbk is `"lake"` or
  `"manmade"`. Route the fetch by that, or a reservoir (`manmade`) sub-region won't be found.
- **`mark_inside_area` currently skips lake nodes.** `border._pieces_in_polygon` filters to
  `node.kind == NodeKind.stream`, so a lake wholly inside a park is **not** flagged `in_areas`
  today (only its in-park *streams* are). When lakes get an `in_areas`-style membership, extend the
  inside-area pass to lake nodes (and, under model B, to lake sub-regions) so a lake-in-a-park
  closure composes. Small, related, and worth doing at the same time.

### Decide, when we build it

1. **Confirm model (B)** (overlay) over (A) (node subdivision) — recommended here.
2. Exact `SplitAnchor` fields for `lake_region` (`region`, `coords`/`point`+`radius_m`/`polygon`,
   `is_lonlat`) and where the sub-region overlay is stored (on the lake `StreamNode`, or a parallel
   `lake_regions` sidecar keyed by `wbk`, mirroring how geometry is a sidecar today).
3. The `location_identifier` vocabulary the matcher registers ("north/south/… portion of X",
   "within N m of the outlet/mouth", bay names) — the only lake-specific thing the matcher learns.
4. Whether Tier-2 named bays are authored polygons or stay map-only for the first pass.
5. Extend `mark_inside_area` (and `_pieces_in_polygon`) to lake nodes / sub-regions.
