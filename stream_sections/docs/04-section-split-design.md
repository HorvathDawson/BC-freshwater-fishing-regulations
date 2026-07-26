# 04 — Curated Split Boundary System (design)

The "define a split location that is carried into the section description" idea, made
**general**: one side ("downstream of X"), both sides ("between X and Y"), N splits → N+1
sections — from one declarative definition per stream.

> **Authoritative mechanics live in `stream_sections/splits.schema.md`** (anchor kinds, target
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
- an **MU / zone boundary** where regulations genuinely differ (Fraser-type, see `07`),
- a named-confluence boundary a synopsis entry references.

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
| `mu_boundary` | the shared boundary line between `mu_a` and `mu_b` (needs both) |
| `confluence` | an auto cut line where `tributary_blk` meets the target mainstem |

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

**Disambiguation:** if two boundaries share a name, append the authored `id` or a distance
qualifier. Validate `location_identifier` uniqueness within a stream at build time (fail loud).

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

## Generality checklist

- ✅ one-side / two-side / N splits → N+1 sections, each labeled by its two immediate bounds.
- ✅ any curated anchor kind (point/line/lake/mu_boundary/confluence) via one resolve-to-`(blk,
  measure)` step.
- ✅ lake above/below sections come **free** from the combine-phase lake split — not authored.
- ✅ description carried on the section (`location_identifier`), auto-generated, stable, local.
- ✅ deterministic + reviewable (resolved points stored back to `splits.resolved.json`).
