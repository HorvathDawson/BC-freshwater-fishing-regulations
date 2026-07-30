# Locators to curate — how-to

The bulk locator reference now lives in **`14-locators-to-curate.json`** (this repo, same
directory). **That JSON is the editable source of truth for manual point curation** — edit it
directly, one object per distinct (regulation, locator string). This md is just the guide.

622 locator objects, converted 1:1 from the old md table (grouped by split **anchor kind**).

## Schema (per locator object)

| field | meaning |
|---|---|
| `id` | stable unique slug (kebab of name + short content hash) |
| `name_verbatim` | regulation title exactly as in the source (may be truncated) |
| `region` | management region, e.g. `REGION 2` (may be empty) |
| `mus` | array of management-unit codes, e.g. `["2-8","2-4"]` |
| `src` | where the locator came from: `name` \| `entry` \| `rule` \| `except` |
| `locator_text` | the locator string to resolve (may be truncated) |
| `anchor_kind` | bucket this locator sits under (see mapping below) |
| `status` | `todo` \| `curated` \| `manual` \| `not_applicable` (default `todo`) |
| `target` | resolved feature target: `{blk:..}` / `{wsc:..}` / `{gnis:..}`, or `""` |
| `coord` | `[lon, lat]` once a single point is chosen, else `null` |
| `split_id` | id of the split authored in `splits.json`, or `""` |
| `label` | short human label for the point/section, or `""` |
| `notes` | free text; carries any human research/annotation verbatim |

Top level also has `_readme`, `_schema`, and `anchor_kinds` (in-file docs).

## anchor_kind → anchor / operator

| anchor_kind | maps to |
|---|---|
| `coordinate` | point (exact coord — easiest) |
| `falls_canyon_obstacle` | point via obstacles layer (FISS_OBSTACLES) |
| `dam_weir_fence` | point (infrastructure) |
| `bridge_road_km` | point (bridge/road/km marker) |
| `confluence_tributary` | confluence (tributary mouth) |
| `lake_reach` | lake (often already split) |
| `lake_inlet_outlet` | NEW `lake_io` op (lake_inlets ∪ lake_outlets) |
| `radius_buffer` | NEW `buffer` op (point+radius) |
| `line_between_signs` | line (author 2 endpoints) / area for lakes |
| `area_park_polygon` | `area_boundary` (polygon) |
| `boundary_signs_generic` | point (locate signs from map) |
| `except_negative` | EXCEPT set-difference / negative member |
| `map_or_vague` | manual / map-only (no clean anchor) |
| `other_reach` | reach (generic) |

`point` / `confluence` / `line` / `lake` exist today; `area_boundary`, `lake_io`, and `buffer`
are Phase-5 additions (see `docs/16`).

## Curating a row (worked example)

Find the row in `14-locators-to-curate.json` and fill in a target and/or coordinate, then flip
`status` to `curated`. For CAYOOSH CREEK, whose `notes` already hold the researched falls point:

Before:
```json
{
  "id": "cayoosh-creek-xxxxxx",
  "name_verbatim": "CAYOOSH CREEK",
  "anchor_kind": "falls_canyon_obstacle",
  "status": "todo",
  "target": "",
  "coord": null,
  "notes": "FISS Database: survey_id 15309 - OBJECTID: 71737068 -- Lat: 50.65710° N, Lon: 121.98259° W"
}
```

After (coord lifted out of `notes`, status set):
```json
{
  "id": "cayoosh-creek-xxxxxx",
  "name_verbatim": "CAYOOSH CREEK",
  "anchor_kind": "falls_canyon_obstacle",
  "status": "curated",
  "target": "",
  "coord": [-121.98259, 50.65710],
  "label": "Cayoosh falls",
  "notes": "FISS survey_id 15309 / OBJECTID 71737068"
}
```

Note `coord` is `[lon, lat]` (west longitude is negative). Use `target` instead when a feature
id is cleaner than a point, e.g. `"target": {"blk": "356353929", "wsc": "100-190442-..."}`.

## Count by anchor_kind

| anchor_kind | count |
|---|--:|
| `coordinate` | 5 |
| `falls_canyon_obstacle` | 81 |
| `dam_weir_fence` | 43 |
| `bridge_road_km` | 116 |
| `confluence_tributary` | 127 |
| `lake_reach` | 58 |
| `lake_inlet_outlet` | 5 |
| `radius_buffer` | 23 |
| `line_between_signs` | 31 |
| `area_park_polygon` | 18 |
| `boundary_signs_generic` | 12 |
| `except_negative` | 19 |
| `map_or_vague` | 26 |
| `other_reach` | 58 |
| **total** | **622** |

Rows with human research already carried over into `notes`: **18** (all under
`falls_canyon_obstacle`; two marked `status: manual` = "already in split").
