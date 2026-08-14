# 05 — Unified Name Variations (graph-applied)

Every static name/alias a feature has is consolidated into ONE compiled file that the graph
applies as `(name, source, note)` tuples — replacing the old per-fid inheritance and the
scattered override files. This is built: the compiler emits `name_variants.json` and `names.py`
applies it at graph build. Regulation *interpretation* stays in match; the *names* it references
are harvested here so both names survive.

## Principle: identity vs interpretation

- **Identity names** (what a feature is called — gazette, manual display, gauge, stocking/bathy
  common name, an alias a synopsis uses) are regulation-independent → live on the **graph node**
  as `name_tuples`. Search + display read them; computed once.
- **Reg interpretation** (skip a synopsis row; region/MU-scoped disambiguation) stays in
  **match**. BUT the *names* those rules mention are still harvested into the tuples (Heber River
  gains the variant "Heber Creek", with a note) — "we still want both names, one is a variant".

## `NameTuple` (extended)

`(name, source, note)`. `source` is **provenance** (`gazette`, `gauge`, `regulation`,
`stocking`, `bathymetry`, `marker`, plus computed `side_channel`/`upstream_inherited`);
`note` = free-text context. Default display priority = the `NameSource` declaration order
(gazette high). `note` records the true origin (e.g. "WSC gauge 08NM241…").

**Display-worthy is a separate `display: true` flag, NOT the source (critical).** Provenance and
"which name to show" are orthogonal — a gauge name can be either the label (Two Forty-One) or a
searchable alias (Arrow Reservoir). So the file marks the label explicitly:
- `feature_display_names.display_name` → its name gets `display: true` (beats gazette on the
  target, even though its provenance is `gauge`/`regulation`).
- `name_variants[]`, harvested regulation verbatims, stocking/bathy names → **no** `display`
  flag → searchable only; the highest-priority tuple (usually gazette) is the label.

This replaces the earlier override-vs-alias source hack: "Arrow Reservoir" (gauge, no display)
stays searchable while "Upper Arrow Lake" (gazette) displays; "Two Forty-One Creek" (gauge,
`display: true`) beats the inherited "Penticton Creek".

**Display casing.** Stocking/bathy/synopsis names are UPPERCASE and abbreviated ("UPPER ARROW
L.", "LONG LAKE"). The stored tuple keeps the raw name (for search); `display_name` is
title-cased with a small abbreviation expansion (`L.`→`Lake`, `Cr.`→`Creek`, `R.`→`River`) ONLY
when the chosen display tuple is all-caps — override/gazette (already correctly cased, e.g.
"McArthur") are left untouched.

## The compiled file: `name_variants.json`

```json
[
  { "target": {"blks": ["356569726"]},
    "reach": {"from_m": 26850, "to_m": 34677},                   // optional — a sub-piece of the blk
    "names": [{"name": "Two Forty-One Creek", "source": "gauge", "display": true,
               "note": "WSC gauge 08NM241; blk lumped under Penticton Creek, above Greyback Lake"}] },

  { "target": {"blks": ["355994571","355994568","355994563","355994564"]},   // multi-blk side channel
    "names": [{"name": "Jeperson Side Channel", "source": "regulation", "display": true, "note": "…"}] },

  { "target": {"wbks": ["328961689"]},
    "names": [{"name": "Arrow Reservoir", "source": "gauge", "note": "alias only — keep the lake name"}] },

  { "target": {"gnis_ids": ["20215"]},
    "names": [{"name": "Heber Creek", "source": "regulation", "note": "variant_of Heber River"}] }
]
```

- `target`: **lists** — any of `blks` / `wbks` / `gnis_ids` / `wscs` (one entry can name a
  multi-blk side channel, or several ids at once). Singular keys (`blk`…) are still accepted.
- `reach` (optional, sub-piece targeting, **fid-free**): `{from_m, to_m}` measure window (the
  piece the lake split already made — no fids).
- `names`: one or more `{name, source, note, display?}`.
  - **`source`** = provenance: `gauge` (note says gauge), `regulation` (from overrides / the
    manual display-name file), `stocking`/`bathymetry`/`marker` (anglerinfo), or live `gazette`.
  - **`display: true`** = this is the label for the target even if its source ranks below
    gazette (how the gauge-sourced "Two Forty-One Creek" beats the inherited "Penticton Creek").
    Omitted ⇒ the name is **searchable only**; the highest-priority tuple (usually gazette)
    displays. So "Arrow Reservoir" (no `display`) stays searchable while "Upper Arrow Lake"
    (gazette) is the label — replaces the old override-vs-alias hack.

## Compiler (`pipeline/oneoff/name_variants_compile.py`) — one-off bootstrap

Reads the current sources and emits `name_variants.json`. Future sources (stocking/bathy/gauges
in new formats) get their own small appenders; this is just the initial merge.

| Source | → target | names harvested | note |
|--------|----------|-----------------|------|
| `feature_display_names.json` | `blue_line_keys`→`blks`, `waterbody_keys`→`wbks`, `linear_feature_ids`→`blks`+reach (resolve fids→blk & min/max measure via FWA, **once**; assert single-blk + contiguous, so output is fid-free) | source = **`gauge`** if the note mentions a gauge else **`regulation`**; `display_name`→ that name with **`display: true`**; `name_variants[]`→ same source, searchable | its `note` |
| `overrides.json` | `gnis_ids`/`waterbody_keys`/**`waterbody_poly_ids`→wbk (via lakes layer)**/`fwa_watershed_codes`/`blue_line_keys` | **only when the entry maps 1:1 to a single id**: `criteria.name_verbatim`, `canonical_name`, `name_variants[]` → **`regulation`** (searchable). Compound/group verbatims (multi-id) NOT harvested per member (Q6). | `note`/`skip_reason` + region+MUs (scope stays in overrides.json for match) |
| `overrides.json` `variant_of` (17, no own id) | resolve variant_of→canonical entry's id; else **gazette name → gnis** (Heber→gnis 20215; Little Campbell/Bear R. ambiguous → logged) | the variant `name_verbatim`→**`regulation`** on the canonical | `variant_of` provenance |
| `anglerinfo_matches.json.wbk_names` | `waterbody_keys`→wbk (already `str(wbk)`-keyed) | stocking names→`stocking`, bathy→`bathymetry` | source tag |
| `_MANUAL` (in the compiler) | grounded names inferred while authoring **curated splits** (docs/04) that no source file carries | `regulation` | each records WHY + a `concern` on the split |
| FWA gazette | (live at build, not in the file) | stream `GNIS_NAME`; lake `GNIS_NAME_1/2/3` (unioned across polygon rows) | — |

**Grounded split-inferred names (`_MANUAL`).** When a split references an FWA-unnamed feature we
infer a name and record it here so search/display can find it. Current entry: **"Sitkatapa
Creek"** (blk 360844922) — the unnamed direct tributary of Burnt Bridge Creek, named for Sitkatapa
Lake up its upper fork; backs the `burnt_bridge_at_sitkatapa` split and carries the same
low-confidence note the split's `concern` does. A future source: the FISS **obstacles** layer's
`GAZETTED_NAME` (fetched as `obstacles`) can seed falls/dam names the same way.

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

## Wetlands — overlay, NOT nodes (how lakes and wetlands differ)

A named waterbody a regulation references must be attachable, but wetlands must behave unlike
lakes: **do not remove the under-wetland stream, do not split the BLK, and do not act as a
tributary barrier.** So:

- **Lakes / manmade** (`get_lake_wbk_kind`): become a graph **node**; the BLK is split at the
  wbk-run and the under-lake fids are absorbed into the lake node (a barrier-capable junction).
- **Wetlands** (and river-polygon wbks): stay a pure **overlay**. The stream keeps flowing
  through unbroken; each stream piece records the non-lake wbks its fids pass through in
  `StreamNode.member_wbks`. A name-variant `target.wbks` resolves to a lake node by `wbk` **or**
  to any stream piece whose `member_wbks` contains it — so a wetland name rides on the
  through-stream piece with no node, no split, no barrier.

```
lake W:   … ──stream── [ lake:W node ] ──stream── …     (split, absorbed, barrier-capable)
wetland WET: … ─────────stream piece─────────── …        (unbroken; piece.member_wbks = {WET})
                         ▲ name "Cattail Marsh" (target wbks:[WET]) overlays this piece
```

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

## Appendix — deferred & rationale

**Why not key by `section_id` / "rename by identifier".** Considered (the "rename by
blk/identifier" idea) and **rejected as the authored key**: a `section_id` is
`"{blk}:{int(down_m)}"`, derived at build time, and it *shifts* whenever a lake or split
up/downstream changes the measure. Authoring against it is fragile. Author against the stable
**blk (+ reach)**; resolve to the concrete piece(s) at build, and write the resolved section_ids
to a review sidecar (`name_variants.resolved.json`) like `splits.resolved.json`.

**Matching (future, not this work; see `10`/`16`).**
- Search resolves a reg's `name_verbatim` against **all** node `name_tuples` (any source).
- **Missing-variant detection**: a reg name that matches no tuple is flagged as a candidate to
  add to `name_variants.json` (with a note) — the "tell us if we're missing a variation" hook.


## TODO — future name sources

- [ ] **Federal power-driven-vessel schedule (SOR/2008-120):** the "Waters on Which Power-driven
      Vessels and Vessels Driven by Electrical Propulsion Are Prohibited" schedule
      (https://laws-lois.justice.gc.ca/eng/regulations/SOR-2008-120/section-sched743254-20220221.html)
      lists many **unnamed lakes by coordinate + a "local name"**. Harvest these as name variants
      (source = federal schedule; note the local name + the coord it resolves against) — a rich
      source of names for FWA-unnamed waterbodies. Also doubles as a vessel-restriction reg source.
data for Waters on Which Power-driven Vessels and Vessels Driven by Electrical Propulsion Are Prohibited has a lot of coords with unnamed lakes with "local name"