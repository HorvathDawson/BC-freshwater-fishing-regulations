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
| `anchor_kind` | the split `anchor.type` this locator maps to (see mapping below) |
| `resolver_hint` | *how* to find the point (`falls_obstacle`, `dam_weir_fence`, `bridge_road_km`, …) — metadata only |
| `status` | `todo` \| `curated` \| `manual` \| `not_applicable` \| `deferred` (default `todo`) |
| `target` | resolved feature target: `{blk:..}` / `{wsc:..}` / `{gnis:..}`, or `""` |
| `coord` | `[lon, lat]` once a single point is chosen, else `null` |
| `label` | short human label for the point/section, or `""` |
| `notes` | free text; carries any human research/annotation verbatim (authored-split links live here as `authored split: <id>`) |

Top level also has `_readme`, `_schema`, `anchor_kinds`, and `resolver_hints` (in-file docs).

## anchor_kind → split anchor.type

`anchor_kind` now **is** the split `anchor.type` (so a curated row maps 1:1 to a split). The old
granular buckets moved to `resolver_hint`, which only records *how* to locate the point.

| anchor_kind | split anchor.type | typical resolver_hint(s) |
|---|---|---|
| `point` | point | `coordinate`, `falls_obstacle`, `dam_weir_fence`, `bridge_road_km`, `boundary_signs` |
| `confluence` | confluence | `tributary` |
| `line` | line | `between_signs` |
| `lake` | lake | `lake_reach` |
| `area_boundary` | area_boundary | `park_polygon` |
| `mu_boundary` | mu_boundary | — |
| `lake_io` | NEW lake_io op | `inlet_outlet` |
| `buffer` | NEW buffer op | `radius` |
| `not_a_split` | (no cut) | `except_negative`, `map_or_vague` |
| `unclassified` | TBD during curation | `other_reach` |

`point` / `confluence` / `line` / `lake` / `area_boundary` / `mu_boundary` exist today; `lake_io`
and `buffer` are Phase-5 additions (see `docs/16`).

## Curating a row (worked example)

Find the row in `14-locators-to-curate.json` and fill in a target and/or coordinate, then flip
`status` to `curated`. For CAYOOSH CREEK, whose `notes` already hold the researched falls point:

Before:
```json
{
  "id": "cayoosh-creek-xxxxxx",
  "name_verbatim": "CAYOOSH CREEK",
  "anchor_kind": "point",
  "resolver_hint": "falls_obstacle",
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
  "anchor_kind": "point",
  "resolver_hint": "falls_obstacle",
  "status": "curated",
  "target": "",
  "coord": [-121.98259, 50.65710],
  "label": "Cayoosh falls",
  "notes": "FISS survey_id 15309 / OBJECTID 71737068"
}
```

Note `coord` is `[lon, lat]` (west longitude is negative). Use `target` instead when a feature
id is cleaner than a point, e.g. `"target": {"blk": "356353929", "wsc": "100-190442-..."}`.

## Count by anchor_kind (post-migration)

| anchor_kind | count |
|---|--:|
| `point` | 252 |
| `confluence` | 150 |
| `lake` | 69 |
| `unclassified` | 58 |
| `not_a_split` | 45 |
| `line` | 31 |
| `buffer` | 23 |
| `area_boundary` | 18 |
| `lake_io` | 5 |
| **total** | **651** |

Authored-split links (previously `split_id`) are now folded into `notes` as
`authored split: <id>` — 18 rows.
