# 04 — Curated Split Boundary System (design)

The "define a split location that is carried into the section description" idea, made
**general**: one side ("downstream of X"), both sides ("between X and Y"), N splits → N+1
sections — from one declarative definition per stream.

> **Authoritative mechanics live in `pipeline/splits.schema.md`** (anchor kinds, target
> scope, proximity). This doc covers the *concepts* and the auto-`location_identifier` rule.
> Where the two differ, the schema wins.

## Scope: curated splits are for NON-lake boundaries

A lake is **not** a curated split. Lakes are handled in the combine phase: a BLK is cut at the
contiguous run of fids whose `wbk` is a lake/manmade waterbody (the under-lake distinction is
carried on the fids themselves — no polygon needed), which promotes the lake to its own node
**and** splits the through-river into its below-lake / above-lake sections automatically. So
"Adams River **upstream of Adams Lake**" vs "**downstream of Adams Lake**" fall out for free
from the lake node — no authored split is required for them (verified: Adams Lake's own
`BLUE_LINE_KEY` *is* the Adams River BLK, with the under-lake fids flagged by `wbk`).

Curated splits therefore exist only for boundaries the data does **not** already give us:
- a falls / dam / bridge partway along a reach,
- an **MU / zone boundary** where regulations genuinely differ (Fraser-type, see `06`),
- a named-confluence boundary a synopsis entry references,
- an **admin / park boundary** a reg scopes to (Garibaldi-type — see "Area reaches" below).

Most regulations are the **same** above and below a lake — those need no split at all; both
lake-side sections simply match the whole-river reg by name (see "How regulations attach").

## Core concepts

### Anchor — the cut geometry
Every cut is a **line or a polygon boundary** (never a bare point/fid). The anchor kinds are
exactly those in `models.py::AnchorType` — see `splits.schema.md` for fields:

| Anchor | Cut geometry |
|--------|--------------|
| `point` | an auto **perpendicular line** across the target mainstem at that coord (length bounded by `proximity_m`, so it also catches nearby side channels but nothing far) |
| `line` | an explicit cut line (≥2 coords) |
| `lake` | a lake polygon boundary (`wbk`) — for the rare case a *curated* boundary should sit on a lake edge that the combine-phase split did not already create |
| `mu_boundary` | the shared boundary line between `mu_a` and `mu_b` (needs both). Collapses to **one** split even where the river runs along the boundary (multi-crossing → dedupe within `proximity_m`, else keep the median + record a `concern`). Verified on the Fraser: the region-2/3 adjacent pair is **`2-18/3-14`** (not `2-17/3-15`, which don't touch), crossing once at m≈196 967. |
| `confluence` | an auto cut line where the tributary meets the target mainstem. Prefer **`tributary_wsc`** over `tributary_blk`: since BLK↔WSC is 1:1 they resolve identically, but the WSC lets the resolver **self-validate** (the tributary's trimmed WSC must be a strict descendant of the parent's, e.g. Burnt Bridge `…777225` ⊃ `…777225-504013`); a failed check keeps the split but records a `concern`. |
| `border` (auto) | the BC provincial outline (WMU union). Not hand-authored — `border.py` emits one per crossing of a cross-border BLK; the piece beyond the outline is flagged `out_of_bc` (see below). |
| `area_boundary` | an admin / park polygon boundary (`area_layer` + `area_name`, optional `wsc_descendants` to target the whole WSC subtree). Cuts the named water **and its WSC descendants** at every crossing, then flags the **inside** pieces `in_areas` — the mirror of `border` (same cut machinery, opposite pieces flagged). See "Area reaches" below. |

All anchors **resolve once at build time** to `SplitPoint(blk, route_measure, fid, offset_m)`
along a blue line. After resolution every anchor is identical downstream — that is what makes
the system general. The resolved points are written to `splits.resolved.json` so a human can
eyeball "did the cut land where I meant?" and builds stay deterministic.

**Geometry cut (verified):** FWA carries route measures and 2D length matches to ~1 cm, so a
cut at absolute measure `M` slices the containing fid at local offset `M − mouth_measure` via
`shapely.ops.substring` — **mid-fid**, not only at fid boundaries. Sections thus carry **new
cut geometry**, not whole fids. (Handle the 0.69% all-2-point BLKs with interpolation.)

### Section (the output)
Walking a stream mouth→source and cutting at each resolved split (plus the lake boundaries the
combine phase already produced) yields an ordered list of sections. Each records `section_id`,
`blk`, `name_tuples`/`display_name`, `lake_wbk` (if it abuts a lake), the **new cut geometry**,
`member_fids` (provenance only — the graph uses ancestors, not fids), its two bounds, and an
auto `location_identifier`.

## Auto-generating `location_identifier`

A section has exactly two bounds — `lower_bound` L (toward the mouth) and `upper_bound` U
(toward the source) — each either a split, a lake, or a natural end:

| lower_bound (L) | upper_bound (U) | location_identifier |
|-----------------|-----------------|---------------------|
| outlet | headwaters | `null` *(stream has no boundaries)* |
| outlet | X | `"downstream of {X}"` |
| X | headwaters | `"upstream of {X}"` |
| X | Y | `"between {X} and {Y}"` |

`{X}` is the boundary's human name — a lake name, a confluence tributary's `stream_name`, or an
authored `label`. Direction words come **purely from flow geometry**: the bound nearer the
mouth → "downstream of", nearer the source → "upstream of". Because a section only ever has two
bounds, this one table covers every case and any number of splits, and labels stay **stable and
local** — adding a split elsewhere on the river does not relabel unrelated sections.

**Area override:** a piece flagged `in_areas` reads `within {area}` instead of the two-bound
label (see "Area reaches" below).

**Disambiguation:** if two boundaries share a name, append the authored `id` or a distance
qualifier. Validate `location_identifier` uniqueness within a stream at build time (fail loud).

## Proximity pickup — a curated point reuses an existing boundary

When a curated split resolves to a measure within `proximity_m` of a boundary that **already
exists** on that BLK (a lake edge, a `border` split, or an earlier curated cut), the sectionizer
**reuses and relabels** that boundary instead of cutting a near-duplicate sliver. This is how the
authored waters express themselves without needing exact geometry:

- Kootenay **"downstream of the Idaho border"** is a **point** anchor that snaps onto the auto
  `border` split at the 49th parallel (m≈168 730, `picked_up: true`) — the reg names a boundary
  that only the border pass creates.
- The `lake` anchor is inherently a pickup: the combine phase already split the BLK at the lake, so
  a `lake` split lands on the existing boundary and just relabels it — Adams Lake (both boundaries
  `picked_up: true`) and **Koocanusa Reservoir** (Lake Koocanusa is FWA manmade wbk 328961702, a
  real node threading the Kootenay — verified in the build; earlier notes calling it "absent from
  FWA" were wrong).

Pickup is recorded per split (`picked_up`) in `splits.resolved.json` and the gpkg `split_points`
layer, so it's never silent.

## `concern` — never-silent caveats

A split may carry an optional authored `_concern` (free text), and the resolver adds its own for
inferred/ambiguous cases (MU multi-crossing collapse, a failed confluence WSC-descendant check).
Examples: Burnt Bridge's tributary is FWA-unnamed, so **"Sitkatapa Creek"** is inferred from
Sitkatapa Lake up its second fork (WSC `…504013-327666`) — flagged, and added as a name variant
with the same note. Concerns surface in `splits.resolved.json` + the gpkg; they never block a
build.

## Border reaches (`out_of_bc`) — like under-lake, but kept

A few reg streams leave BC and return (the Kootenay loops through Montana/Idaho — FWA carries the
full geometry). The `border` pass splits such a BLK at the provincial outline and flags the
outside pieces `out_of_bc`. Unlike an under-lake reach (absorbed into the lake node), the geometry
is **kept** so the client can draw it dotted, and the piece is **not** a flow barrier — BC regs
simply don't apply there.

## Area reaches (`in_areas`) — the mirror of `out_of_bc`

An `area_boundary` split is the same machinery pointed the other way: it cuts a named water and
its WSC descendants at every crossing of an admin/park polygon and flags the pieces that fall
**inside** (`in_areas`), for regs scoped to a park or WMA (e.g. "all waters within Garibaldi
Park"). `border.py::mark_inside_area` walks the target BLKs (WSC-prefix scoped via
`wsc_descendants`), and an inside piece — whichever side of the boundary it lies on — reads
**"within {area}"** as its `location_identifier`, overriding the plain upstream/downstream label.
This handles the two tricky shapes correctly: a stream that **passes through** yields three
pieces with only the middle flagged, and a stream that **ends inside** the park (boundary →
headwaters) still reads "within", not the misleading "upstream of" the bounds alone would give.
Geometry is kept on every piece; scope is WSC-gated, so an inside stream on a different WSC is
ignored.

## How regulations attach to sections

- "Adams River" (no qualifier) → matches **all** Adams River sections (below-lake, above-lake,
  …) by name. This is the common "same reg above and below the lake" case — nothing special.
- "Adams River (upstream of Adams Lake)" → the section whose `upper_bound` is above / whose
  `location_identifier == "upstream of Adams Lake"`. The two synopsis rows land on two distinct
  sections, **replacing the old duplicate-override-row hack** where both pointed at one gnis.

Matching precedence: (1) explicit `section_id` in an override, (2) parsed location text mapped
to a boundary, (3) whole-stream fallback (all sections) when no location is given.

## Tributary interaction

Splits define section boundaries; the graph (nodes = sections/lakes, edges = flows-into)
carries connectivity. A curated split is a **labeling/matching** boundary, not a flow barrier:
tributary reachability is the guarded ancestor walk over the graph, and the only flow barriers
are **lakes** and **EDGE_TYPE=2300** connectors — both already applied at graph-build time (the
WSC-descendant edge filter + the 2300 `is_barrier` node). There is **no** per-split `barrier`
flag; if a dam should stop propagation, that is a lake/2300 property of the geometry, not an
authored flag on the split.

## "[Includes Tributaries] EXCEPT …" — pure set algebra

Once the splits exist, a compound reg is a set difference over node ids — no geometry re-walk.
Real case: *ATNARKO / BELLA COOLA RIVERS [Includes Tributaries] EXCEPT Burnt Bridge Cr. upstream
of Sitkatapa Cr., Hunlen Cr. upstream of Hunlen Falls, Young Cr. upstream of Hwy 20*.

```
base   = with_tributaries(Atnarko) ∪ with_tributaries(Bella Coola)     # rivers + all upstream
except = tribs(Hunlen ↑ Hunlen Falls) ∪ tribs(Burnt Bridge ↑ Sitkatapa) ∪ tribs(Young ↑ Hwy 20)
result = base − except
```

Each "X upstream of Y" is the **upper piece** the Y-split already made (`piece_above(blk, label)`
finds it by the split's boundary label); its exclusion set is that piece's own guarded ancestor
closure (`tributary_node_ids`). `reach_except(base_ids, except_ids)` in `tributaries.py` is the
whole thing. The un-split downstream pieces (below each falls/road) and any un-split creek stay in
`result`.

An EXCEPT reach with its own entry self-declares (a direct match beats the inherited one). One
without its own entry is named as a hand-curated carve-out `Extent`: entry-wide in
`Tributaries.excludes`, or per-rule in `Rule.tributary_excludes` when only one rule (e.g. a
seasonal "No Fishing in tributaries except Quinsam River") should drop it. Same set difference.

```
        ocean ── Bella Coola ─────────────────────────────  headwaters
                   ▲Ordinary   ▲Young      ▲Burnt Bridge   ▲Atnarko
                   (no split)  ┊Hwy 20      ┊Sitkatapa       └─ Hunlen ┊Hunlen Falls
                               │                                        │
        EXCEPT drops:      Young↑Hwy20   BurntBridge↑Sitkatapa    Hunlen↑Falls
        result keeps:      everything else, incl. the reaches BELOW each ┊ split
```

## Generality checklist

- ✅ one-side / two-side / N splits → N+1 sections, each labeled by its two immediate bounds.
- ✅ any curated anchor kind (point/line/lake/mu_boundary/confluence) via one resolve-to-`(blk,
  measure)` step.
- ✅ lake above/below sections come **free** from the combine-phase lake split — not authored.
- ✅ description carried on the section (`location_identifier`), auto-generated, stable, local.
- ✅ deterministic + reviewable (resolved points stored back to `splits.resolved.json`).
- ✅ MU boundary → exactly one split (median + `concern` if a weaving river crosses N times).
- ✅ confluence self-validates by WSC descendant check; `concern` on failure, never a hard stop.
- ✅ proximity pickup: a curated point reuses a nearby lake/border/earlier boundary (Kootenay).
- ✅ cross-border BLKs split at the BC outline; outside pieces `out_of_bc` (kept, dotted, not a barrier).
- ✅ admin/park `area_boundary` splits a water + its WSC descendants; inside pieces `in_areas` → "within {area}".
- ✅ "[Includes Tributaries] EXCEPT …" = `reach_except` set difference over the split pieces.
