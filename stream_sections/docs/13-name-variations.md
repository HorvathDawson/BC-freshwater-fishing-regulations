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
`upstream_inherited` > `gauge` > `stocking` > `bathymetry` > `alias` > `synopsis`.

**Display-worthy vs alias-only (critical).** A curated decision to *display* a non-gazette name
(Two Forty-One from a gauge; an unnamed polygon's assigned name) is tagged `override` so it
beats gazette on that piece. But an alternate name that must stay *searchable only* (e.g. "Arrow
Reservoir" for Upper Arrow Lake — the source note says "alias only, keep the lake name") is
tagged `alias`, below gazette, so the official name still displays. The distinction is authored,
not guessed: `feature_display_names.display_name` → `override`; its `name_variants[]` and every
harvested regulation/stocking/bathy name → `alias`/`stocking`/`bathymetry`/`synopsis` (all
below gazette). `note` records the true origin regardless of the display tag.

**Display casing.** Stocking/bathy/synopsis names are UPPERCASE and abbreviated ("UPPER ARROW
L.", "LONG LAKE"). The stored tuple keeps the raw name (for search); `display_name` is
title-cased with a small abbreviation expansion (`L.`→`Lake`, `Cr.`→`Creek`, `R.`→`River`) ONLY
when the chosen display tuple is all-caps — override/gazette (already correctly cased, e.g.
"McArthur") are left untouched.

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
| `feature_display_names.json` | `blue_line_keys`→blk, `waterbody_keys`→wbk, `linear_feature_ids`→blk+reach (resolve fids→blk & min/max measure via FWA, **once**; assert single-blk + contiguous, so output is fid-free) | `display_name`→**override**; `name_variants[]`→**alias** | its `note` |
| `overrides.json` | `gnis_ids`/`waterbody_keys`/**`waterbody_poly_ids`→wbk (via lakes layer)**/`fwa_watershed_codes`/`blue_line_keys` | **only when the entry maps 1:1 to a single id**: `criteria.name_verbatim`→`synopsis`, `canonical_name`→`alias`, `name_variants[]`→`alias`. Compound/group verbatims (multi-id) are NOT harvested per member (Q6). | `note`/`skip_reason` + region+MUs (scope stays in overrides.json for match) |
| `overrides.json` `variant_of` (17, no own id) | resolve variant_of→canonical entry's id; else **gazette name+region → gnis/blk** (Heber, Bear R., Little Campbell have no override) | the variant `name_verbatim`→`alias` on the canonical | `variant_of` provenance |
| `anglerinfo_matches.json.wbk_names` | `waterbody_keys`→wbk (already `str(wbk)`-keyed) | stocking names→`stocking`, bathy→`bathymetry` | source tag |
| FWA gazette | (live at build, not in the file) | stream `GNIS_NAME`; lake `GNIS_NAME_1/2/3` (unioned across polygon rows) | — |

### Out of the identity model (documented, not silently dropped)
- **lon/lat-only ungazetted points** (`ungazetted_location`, no id): `MARSH POND`,
  `SKEENA/KISPIOX CONFLUENCE` — no blk/wbk/gnis/wsc.
- **sub-lake arms/bays** whose only id is the whole-lake gnis (`NATION ARM`, `DAVIS BAY` →
  gnis 28522 = all of Williston Lake) — would mislabel the reservoir; skipped.
- **`admin_targets`** park/WMA groups (Strathcona, Bowron, …) — area scopes, not features.
- The compiler **logs** every skipped/unresolved entry (never silent).

## Application at graph build (`names.py`)

After nodes exist, for each `name_variants.json` entry:
1. resolve `target` → node(s): `blk`→that blk's stream pieces; `wbk`→the `lake:{wbk}` node;
   `gnis_id`→nodes with that gnis; `wsc`→nodes with that wsc.
2. if `reach` given, restrict to pieces inside the measure window / relative to the lake.
3. append `(name, source, note)` tuples (dedupe by name); re-sort by source priority;
   `display_name` = top tuple.

Lakes also receive their FWA gazette names `GNIS_NAME_1/2/3` (live; `_3` NaN-cleaned, unioned
across the wbk's polygon rows) as `gazette` tuples.

**Unresolved targets are logged, not dropped silently.** A `wbk` target whose waterbody is not
in the `lakes`/`manmade` layers has no lake node (e.g. Cahilty Lake wbk `329321459` is not in
`lakes`). Either extend lake-noding to cover **named** wetland/other waterbodies referenced by a
name-variant, or accept + log the miss. TODO: confirm coverage; a named waterbody a regulation
uses must end up as a node.

**Sub-feature search pollution (known, low-priority).** A lake's stocking `wbk_names` include
arm/bay names ("Beaton Arm", "Galena Bay" on Upper Arrow Lake). They stay as low-priority
searchable `stocking` tuples (never display); searching an arm name returns the whole lake.
Acceptable for now; flag for a later filter.

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
