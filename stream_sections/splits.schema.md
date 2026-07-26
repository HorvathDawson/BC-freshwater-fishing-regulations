# Manual stream splits — schema & how to author them

A **split** cuts one or more blue lines at one location, producing separate **sections** with
auto-generated `location_identifier`s (e.g. "upstream of Adams Lake"). This is how you say
*which stream* and *where*. Authored as JSON; see `splits.example.json`. Loaded by
`SplitDef.from_dict` (see `models.py`).

## Entry shape

```jsonc
{
  "id": "adams_lake",         // STABLE key — part of the section_id ABI; never rename casually
  "blk"|"wsc"|"gnis_id": ..., // TARGET: exactly one (which stream(s) to cut) — see below
  "anchor": { ... },          // WHERE to cut — see below
  "label": "Adams Lake",      // human name used in the generated location_identifier
  "barrier": false            // optional; true = also a flow barrier (dam) that stops tributaries
}
```

## TARGET — which stream(s) to cut (exactly one)

| field | meaning | # cuts |
|-------|---------|--------|
| `blk` | cut this single blue line | 1 |
| `wsc` | cut **every** blue line sharing this watershed code (main channel **and** all side channels) | N (one per BLK) |
| `gnis_id` | resolve to the named stream's BLK(s), then behave like `blk` | 1+ |

`wsc` is the braided-river form: one coordinate cuts the mainstem and every side channel at
the same place, so a section boundary crosses the whole river cleanly. (Recall BLK→WSC is
1:1; side channels share the mainstem's WSC — so a WSC target is exactly "this river and its
side channels".)

## ANCHOR — where to cut

| `type` | fields | resolution |
|--------|--------|------------|
| `point` **(primary)** | `coord: [x,y]`, `is_lonlat` | snap the coordinate to the **nearest point on each targeted BLK's geometry**, take that point's route measure, cut. `is_lonlat:true` → coord is `[lng,lat]` (WGS84, as read off a map); else EPSG:3005 `[x,y]`. |
| `lake` | `wbk` | cut where the targeted stream enters/exits the lake (lake outlet/inlet route measure). |
| `linear_feature_id` | `fid` | cut at that fid's downstream boundary — exact, no snapping. |
| `mu_boundary` | `mu_id` | cut where the targeted stream crosses the MU boundary polygon (a located point cut). |
| `landmark` | `name` (+ resolved `coord`) | resolve a named point feature to a coordinate, then as `point`. |
| `confluence` | `tributary_gnis_id` | cut at the confluence with the named tributary. |

**The primary workflow** is: pick a coordinate off a map, and say either `blk` (one channel)
or `wsc` (the whole braided river). Everything else is for cases where a coordinate is
awkward (a known lake, an exact fid, an admin boundary).

## What resolution produces

Each targeted BLK yields a `SplitPoint(blk, route_measure, fid, label, barrier, snap_dist_m)`.
`snap_dist_m` (distance from your coordinate to the snapped point) is written to
`splits.resolved.json` so you can eyeball that the cut landed where you meant — if it's large,
your coordinate was off the channel. The sectionizer then cuts geometry with
`shapely.ops.substring` at each `route_measure` and generates the section labels:

| section bounds | `location_identifier` |
|----------------|-----------------------|
| outlet → headwaters (no split) | `null` |
| outlet → split X | `downstream of {X.label}` |
| split X → headwaters | `upstream of {X.label}` |
| split X → split Y | `between {X.label} and {Y.label}` |

## Stability rules (ABI)

- `id` and the target (`blk`/`wsc`/`gnis_id`) are stable keys. Renaming an `id` changes the
  `section_id`s of the sections it bounds.
- Changing only an anchor's **position** (moving the coordinate) re-cuts geometry but keeps
  `section_id`s (the bound identity is the split `id`, not its measure).
- Adding a split on one stretch never changes sections elsewhere.
- Build validation fails loud on duplicate generated `location_identifier`s within one gnis.
