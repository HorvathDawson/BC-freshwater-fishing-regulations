# 04 — General Hand-Split Boundary System (deep design)

This is the "define a split location that is carried into the section description" item the
user asked to make **fully general**: one side ("downstream of X" / "upstream of X"), both
sides ("downstream of X, upstream of Y"), and everything in between — from one declarative
definition.

## Core concepts

### Split point
A **split point** is a location along a single stream where we cut. It is defined
*declaratively* by how to *find* it, and resolved *once at build time* to a concrete
position on the stream (a boundary between two adjacent `linear_feature_id`s).

A split point's **anchor** (how to find it) is one of:

| Anchor type | Field(s) | Resolves to | Example |
|-------------|----------|-------------|---------|
| `lake` | `wbk` | the fid where the stream enters/exits that lake | Adams Lake |
| `confluence` | `tributary_gnis_id` (or `tributary_blk`) | the confluence node fid | "at the mouth of the Coquihalla" |
| `linear_feature_id` | `fid` | that exact fid boundary | Wigwam River divide @ 706869683 |
| `landmark` | `name` + resolved `fid` | a named point feature snapped to nearest stream fid | "the CPR Bridge" |
| `point` | `lat, lng` | nearest stream fid boundary (linear-referenced) | manual coordinate |
| `border` | admin polygon edge | fid where stream crosses the boundary | "the Idaho border" |

All anchor types **normalize to a single resolved position**: `(blk, route_measure)` — an
absolute `DOWNSTREAM_ROUTE_MEASURE` value along the blue line. Once resolved, every anchor
type is identical downstream in the pipeline. This is what makes it general.

**Geometry cut (verified):** FWA carries `DOWNSTREAM_ROUTE_MEASURE`/`UPSTREAM_ROUTE_MEASURE`
/`LENGTH_METRE` on every row, and 2D geometry length matches route measure to ~1 cm. So a
split at absolute measure `M` cuts the containing segment at local offset
`M − DOWNSTREAM_ROUTE_MEASURE` via `shapely.ops.substring` — we can split **mid-segment**,
not only at fid boundaries. This is why sections carry **new cut geometry** (see `02` §"two
granularities" and `03` Step 5), and why the tiles ship section geometry rather than the old
fids.

### Split definition (the hand-authored input)

Authored per stream (keyed by gnis_id, or blk when a gnis spans identities). One stream can
have any number of split points. Proposed schema (JSON, sits alongside `overrides.json` or
folded into it):

```json
{
  "gnis_id": "39257",
  "stream_name": "Adams River",
  "splits": [
    { "id": "adams_lake", "anchor": { "type": "lake", "wbk": "9200..." } }
  ]
}
```

Multi-split example (a river cut in two places → three sections):

```json
{
  "gnis_id": "12345",
  "stream_name": "Example River",
  "splits": [
    { "id": "hwy_bridge", "anchor": { "type": "landmark", "name": "Highway 1 Bridge" } },
    { "id": "falls",      "anchor": { "type": "linear_feature_id", "fid": "706..." } }
  ]
}
```

### Section (the output)

The split builder walks the stream's fids **in flow order** (downstream→upstream, using the
directed graph / FWA downstream_route_measure) and cuts at each resolved split position,
producing an ordered list of sections. Each section records:

```
section {
  section_id,                      # stable hash of (blk, ordered boundary ids, lake_wbk)
  blk, name_tuples, display_name,  # name_tuples per 02; display = highest-priority tuple
  lake_wbk,                        # non-null if this section abuts / is a lake run
  geometry,                        # NEW cut geometry (substring), not whole fids
  member_fids: [...],              # composing FWA fids (provenance; graph uses ancestors)
  lower_bound: split_id | "outlet",     # toward the mouth
  upper_bound: split_id | "headwaters", # toward the source
  location_identifier              # AUTO-GENERATED, see below
}
```

## Auto-generating `location_identifier` (the general rule)

Given a section with `lower_bound` L (downstream side) and `upper_bound` U (upstream side),
where each bound is either a split point or a natural end:

| lower_bound (L) | upper_bound (U) | location_identifier |
|-----------------|-----------------|---------------------|
| outlet | headwaters | `null`  *(stream has no splits)* |
| outlet | split X | `"downstream of {X}"` |
| split X | headwaters | `"upstream of {X}"` |
| split X | split Y | `"between {X} and {Y}"` *(equivalently "downstream of Y, upstream of X")* |

Where `{X}` is the split point's human name (`stream_name` of a confluence tributary, lake
name, landmark name, or an authored `label`). Direction words come **purely from flow
geometry**: the bound nearer the mouth → "downstream of"; nearer the source → "upstream of".
This single table covers every case the user listed and any number of splits, because a
section only ever has exactly two bounds.

**Consistency rule:** always describe a section by its two immediate bounds. This makes
labels stable and local — adding a third split elsewhere on the river does not relabel
unrelated sections.

**Disambiguation:** if two split points share a name (e.g. two "Falls"), append the authored
`id` or a distance qualifier. Validate uniqueness of generated `location_identifier` within
a gnis at build time (fail loud, like `test_overrides_validation.py` does today).

## Resolving anchors to positions (build-time, once)

1. **lake**: from `wbk`, get the lake's outlet/inlet fids (the existing
   `_wbk_to_fids` / lake-outlet machinery in `feature_resolver.py` already computes lake
   outlet fids). The split sits at the outlet (downstream side) and, for a through-flowing
   lake, the inlet (upstream side) — a lake naturally produces a barrier node, so an
   "upstream of Adams Lake" section starts at the lake's upstream inlet.
2. **confluence**: find the graph node where `tributary_gnis_id` meets this stream; the
   split boundary is the fid immediately upstream of that node on the mainstem.
3. **linear_feature_id**: use the fid's `DOWNSTREAM_ROUTE_MEASURE` (or its up-measure) as
   the cut position — exact, already the mechanism behind Wigwam/Shuswap in `overrides.json`.
4. **landmark / point / border**: project the coordinate (or admin-boundary crossing) onto
   the blue line to get a route measure (`geom.project` → measure), then cut. Store the
   resolved `(blk, route_measure)` back into the split definition (this is exactly
   `Landmark.fid`/position in `matching/reg_models.py` — "leave as None until that linking
   step is done").

**Store resolved fids back** into the authored file (or a generated sidecar) so builds are
deterministic and reviewable, and so a human can eyeball "did the split land where I meant?"
— mirroring how `overrides.json` already notes "divide is approximately at LFID 706869683".

## How regulations attach to sections

A regulation that says "Adams River (upstream of Adams Lake)" now resolves cleanly:
- `_natural_search` finds the Adams River gnis.
- the `location_text` "upstream of Adams Lake" matches the section whose
  `location_identifier == "upstream of Adams Lake"` (or, more robustly, whose `upper_bound`
  is above / `lower_bound` is the `adams_lake` split).
- **This replaces the duplicate-override-row hack** where both halves pointed at the same
  gnis. The two synopsis rows now land on two distinct sections.

Matching precedence: (1) explicit section_id in an override, (2) parsed `location_text`
mapped to a split id, (3) whole-gnis fallback (all sections) when no location given.

## Tributary interaction

Splits define sections; the **contracted tree** connects them. A split point is also a tree
node, so "tributaries upstream of the section boundary" fall out of the upstream walk from
that section (see `03` G6). A section-level split does not by itself break tributary flow —
only lakes and 2300 edges do (barriers). A hand split is a *labeling/matching* boundary, not
necessarily a *flow* barrier, **unless** the authored split sets `barrier: true` (e.g. a dam
that should stop upstream propagation). Add an optional `barrier` flag to the split schema
for that case.

## Generality checklist (maps to the user's ask)

- ✅ one-side split ("upstream of X" / "downstream of X") — 1 split → 2 sections.
- ✅ two-side split ("downstream of X, upstream of Y") — 2 splits → the middle section is
  `between X and Y`.
- ✅ N splits → N+1 sections, each labeled by its two immediate bounds.
- ✅ any anchor kind (lake / confluence / fid / landmark / point / border) via one
  normalize-to-position step.
- ✅ description carried on the section (`location_identifier`), auto-generated, stable,
  local.
- ✅ optional flow-barrier semantics via `barrier: true`.
- ✅ deterministic + reviewable (resolved fids stored back).
