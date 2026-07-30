# Manual stream splits — schema & how to author them

A **split** defines a **cut geometry — always a line or a polygon boundary** — that slices
the channels it crosses. Each crossed channel becomes two **sections** with auto-generated
`location_identifier`s (e.g. "upstream of Adams Lake"). Authored as JSON; see
`splits.example.json`. Loaded/validated by `SplitDef.from_dict` (`models.py`).

## Entry shape

```jsonc
{
  "id": "adams_lake",            // STABLE key — part of the section_id ABI; don't rename casually
  "blk"|"wsc"|"gnis_id": ...,    // TARGET scope (optional, at most one) — which channels are eligible
  "anchor": { "type": ... },     // the CUT GEOMETRY (line or polygon boundary)
  "label": "Adams Lake",         // human name used in the generated location_identifier
  "proximity_m": 500,            // max distance to the cut geometry AND the pickup radius (default 500)
  "_concern": "…"                // OPTIONAL free-text caveat (inferred name, etc.) — never blocks a build
}
```

## The cut is always a line or a boundary

| `anchor.type` | fields | the cut geometry |
|---------------|--------|------------------|
| `point` **(primary)** | `coord:[x,y]`, `is_lonlat` | a short **line perpendicular** to the target mainstem at the nearest point, extended ±`proximity_m` so it also crosses nearby side channels. |
| `point`/`confluence` **offset** | `offset_m`, `offset_dir` (`"upstream"`\|`"downstream"`) | shifts the resolved cut `offset_m` metres **along the channel** from the projected coord/mouth (e.g. "100 m downstream of the falls"). Follows the streamline, not straight-line; clamped to the channel ends (a clamp records a `concern`). Default `offset_m:0` = no shift. |
| `line` | `coords:[[x,y],…]` (≥2), `is_lonlat` | the **explicit line** you author; cuts every eligible channel it crosses. |
| `lake` | `wbk` | the **lake polygon boundary**; cut where the stream crosses in/out. |
| `mu_boundary` | `mu_a`, `mu_b` (**both required**) | the **shared boundary line** between the two MUs; collapses to **one** split even if the river weaves across it (median crossing + a `concern` recording the count). |
| `confluence` | `tributary_wsc` **(preferred)** or `tributary_blk` | a cut line where that tributary meets the target mainstem. Prefer `tributary_wsc`: it **self-validates** (the tributary's trimmed WSC must be a strict descendant of the parent's; a mismatch keeps the split but records a `concern`). BLK↔WSC is 1:1, so either resolves identically. |

A cut never lands "at a fid boundary" — it is a geometric line/boundary, and each channel is
cut exactly where it intersects.

`border` is a further anchor kind but is **not authored** — `border.py` emits it automatically at
every crossing of a cross-border BLK with the BC outline; the outside piece is flagged
`out_of_bc` (geometry kept, drawn dotted, not a flow barrier).

**Proximity pickup.** If a `point`/`confluence`/`lake` split resolves within `proximity_m` of a
boundary that already exists on the BLK (a lake edge, a `border` split, or an earlier cut), the
sectionizer **reuses and relabels** that boundary instead of cutting a near-duplicate — set
`picked_up:true` in `splits.resolved.json`. This lets a reg's wording name an existing boundary
(Kootenay "Idaho border" / "Koocanusa Reservoir" snap onto the auto border splits).

## TARGET scope — which channels are eligible (optional, at most one)

| field | eligible channels |
|-------|-------------------|
| `blk` | only this blue line |
| `wsc` | this river **and its side channels** (they share the WSC) — **proximity-limited**, so far-away same-WSC channels are never cut |
| `gnis_id` | the named stream's blk(s) |

`point` and `confluence` anchors **require** a target (they need a mainstem to cut across).
`line`/`lake`/`mu_boundary` may omit it (the geometry defines the scope), but a target still
narrows eligibility. Every match is additionally gated by `proximity_m`.

## What resolution produces

Each eligible channel the cut geometry crosses yields a
`SplitPoint(blk, route_measure, fid, label, offset_m, proximity_m, picked_up, concern)`.
`offset_m` (distance from the cut geometry to the crossing), `picked_up` (reused an existing
boundary), and `concern` are written to `splits.resolved.json` so you can confirm the cut landed
where you meant. The sectionizer then cuts geometry with `shapely.ops.substring` at each
`route_measure` and labels the sections:

| section bounds | `location_identifier` |
|----------------|-----------------------|
| outlet → headwaters (no split) | `null` |
| outlet → split X | `downstream of {X.label}` |
| split X → headwaters | `upstream of {X.label}` |
| split X → split Y | `between {X.label} and {Y.label}` |

## Stability rules (ABI)

- `id` and the target are stable keys; renaming an `id` changes bounded sections' `section_id`s.
- Moving a cut's **position** re-cuts geometry but keeps `section_id`s.
- Adding a split on one stretch never changes sections elsewhere.
- Build validation fails loud on duplicate generated `location_identifier`s within one gnis.
