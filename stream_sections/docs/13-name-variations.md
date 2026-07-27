# 13 — Unified Name Variations (graph-applied)

Consolidate every static name/alias a feature has into ONE compiled file that the graph applies
as `(name, source, note)` tuples — replacing the old per-fid inheritance and the scattered
override files. Regulation *interpretation* stays in match; the *names* it references are
harvested here so both names survive.

## Principle: identity vs interpretation

- **Identity names** (what a feature is called — gazette, manual display, gauge, stocking/bathy
  common name, an alias a synopsis uses) are regulation-independent → live on the **graph node**
  as `name_tuples`. Search + display read them; computed once.
- **Reg interpretation** (skip a synopsis row; region/MU-scoped disambiguation) stays in
  **match**. BUT the *names* those rules mention are still harvested into the tuples (Heber River
  gains the variant "Heber Creek", with a note) — "we still want both names, one is a variant".

## `NameTuple` (extended)

`(name, source, note)`. `note` = free-text provenance ("WSC gauge 08NM241; above Greyback
Lake"). `NameSource` (display priority high→low): `override` > `gazette` > `side_channel` >
`upstream_inherited` > `gauge` > `stocking` > `bathymetry` > `synopsis`. A curated decision to
*display* a non-gazette name (e.g. Two Forty-One from a gauge) is authored as `override` so it
beats the inherited gazette on that piece; the `note` records the true origin.

## The compiled file: `name_variants.json`

```json
[
  { "target": {"blk": "356569726"},
    "reach": {"from_m": 26850},                                  // optional — a sub-piece of the blk
    "names": [{"name": "Two Forty-One Creek", "source": "override",
               "note": "WSC gauge 08NM241; blk lumped under Penticton Creek, above Greyback Lake"}] },

  { "target": {"wbk": "329459226"},
    "names": [{"name": "Clark Lake", "source": "stocking", "note": "stocking source_id 175278; #1 (West)"}] },

  { "target": {"gnis_id": "17501"},
    "names": [{"name": "Long Lake", "source": "override", "note": "alias; disambiguated from Long Lake (Nanaimo)"}] }
]
```

- `target`: exactly one of `blk` / `wbk` / `gnis_id` / `wsc`.
- `reach` (optional, sub-piece targeting, **fid-free**): `{from_m, to_m}` measure window, or
  `{upstream_of_wbk}` / `{downstream_of_wbk}` (relative to a lake that already split the blk).
- `names`: one or more `{name, source, note}`.

### Why not key by section_id / "rename by identifier"
Considered (the "rename by blk/identifier" idea). **Rejected as the authored key**: a
`section_id` is `"{blk}:{int(down_m)}"`, derived at build time, and it *shifts* whenever a lake
or split up/downstream changes the measure. Authoring against it is fragile. Author against the
stable **blk (+ reach)**; resolve to the concrete piece(s) at build, and write the resolved
section_ids to a review sidecar (`name_variants.resolved.json`) like `splits.resolved.json`.

## Compiler (`stream_sections/name_variants_compile.py`) — one-off bootstrap

Reads the current sources and emits `name_variants.json`. Future sources (stocking/bathy/gauges
in new formats) get their own small appenders; this is just the initial merge.

| Source | → target | names harvested | note |
|--------|----------|-----------------|------|
| `feature_display_names.json` | `blue_line_keys`→blk, `waterbody_keys`→wbk, `linear_feature_ids`→blk+reach (resolve fids→blk & min/max measure via FWA, **once**, so the output is fid-free) | `display_name` (override) + `name_variants` | its `note` |
| `overrides.json` | `gnis_ids`/`blue_line_keys`/`waterbody_keys`/`fwa_watershed_codes` | `criteria.name_verbatim`, `canonical_name`, `name_variants[]`, `variant_of.name_verbatim` (all `synopsis`/`override`) | `note`/`skip_reason` + region+MUs (the scope stays in overrides.json for match; only the *name* comes here) |
| `wbid_overrides.json` + stocking match | `waterbody_keys`→wbk | stocking common names | `stocking` provenance |
| FWA gazette | (live at build, not in the file) | stream `GNIS_NAME`; lake `GNIS_NAME_1/2/3` | — |

## Application at graph build (`names.py`)

After nodes exist, for each `name_variants.json` entry:
1. resolve `target` → node(s): `blk`→that blk's stream pieces; `wbk`→the `lake:{wbk}` node;
   `gnis_id`→nodes with that gnis; `wsc`→nodes with that wsc.
2. if `reach` given, restrict to pieces inside the measure window / relative to the lake.
3. append `(name, source, note)` tuples (dedupe by name); re-sort by source priority;
   `display_name` = top tuple.

Lakes also receive their FWA gazette names `GNIS_NAME_1/2/3` (live; `_3` NaN-cleaned) as
`gazette` tuples — this is where GNIS_NAME_3 lands in the lake node.

## Cases covered (acceptance checklist)

- [x] whole-blk unnamed side channel (`blue_line_keys`) — most of feature_display_names.
- [x] multi-blk display name (one entry per blk).
- [x] lake names: `GNIS_NAME_1/2/3` + override + stocking/bathy common names → the lake node.
- [x] gnis alias ("Long Lake" for gnis 17501).
- [x] `variant_of` — both names on the canonical feature (Heber River + "Heber Creek" note).
- [x] **sub-piece, different name on a shared blk, fid-free** (Two Forty-One above Greyback Lake
      via blk + `reach.from_m`) — the piece the lake split already made; override beats the
      inherited Penticton on that piece only.
- [x] provenance `note` per variant.

## Matching (future, not this work)

- Search resolves a reg's `name_verbatim` against **all** node `name_tuples` (any source).
- **Missing-variant detection**: a reg name that matches no tuple is flagged as a candidate to
  add to `name_variants.json` (with a note) — the "tell us if we're missing a variation" hook.
