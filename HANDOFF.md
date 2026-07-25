# Handoff — InfoPanel regs + land-access cleanup

## What changed

### 1. Zone regs sort to the bottom of the rules list
`webapp/src/components/InfoPanel.tsx`
- `sourceOrder.zone = 8` (below synopsis/tributary/provincial).
- Removed zone from `hasHighPriorityProvReg` — a zone `closed` reg no longer
  floats to the top; only **provincial** closures do. Zone stays pinned last.

### 2. National Parks / Ecological Reserves fold into "Land Use"
`webapp/src/components/InfoPanel.tsx`
- `PROV_NAT_PARKS_CLOSED` and `PROV_ECO_RESERVES_CLOSED` now route into the
  `land_access` group (same as the indigenous advisory), matched **by
  regulation_id** — the built reg JSON does not carry `scope_location`, only the
  id (this is why the first `scope_location`-based attempt silently did nothing).

### 3. Amber "restricted" land-access tier removed entirely
The conditional/permit-based (amber) tier is gone from matching AND the map.
Kept: `access=private`, `access=no`, `landuse=military`, closed watersheds
(all classify as `restriction_level: "closed"` → red).

- `pipeline/enrichment/base_regulations.json` — deleted `LAND_ACCESS_RESTRICTED`
  reg; reworded `LAND_ACCESS_CLOSED` details to present tense (no map-color ref).
- `pipeline/atlas/freshwater_atlas.py` — skip any land_access record whose
  `restriction_level != "closed"` at atlas load.
- `webapp/src/map/styles.ts` — removed amber color; `LAND_ACCESS_COLOR_EXPR` is
  now a flat red constant.
- `webapp/src/components/Map.tsx` — removed amber hatch layer + amber hatch
  image; dropped `admin_land_access-hatch-restricted` from the style-id map.
- `pipeline/tiles/layer_manifest.py` — legend text updated (red only).

## Rebuild required
- Panel/matching changes → `python -m pipeline --step enrich`
- Map amber removal → needs atlas rebuild + tile re-export:
  `python -m pipeline --step atlas tiles enrich`  (or `--step all`)

`enrich` alone is NOT enough for the map — it loads the existing atlas pickle;
the restricted-polygon filter only runs on an `atlas` rebuild.

## Known inert leftovers (safe to ignore or clean later)
- `webapp/src/utils/featureUtils.ts` — `land_access_restricted` entries in the
  FeatureType registry (dead; no restricted feature exists anymore).
- `webapp/src/components/InfoPanel.tsx` — `ACCESS_SEVERITY.LAND_ACCESS_RESTRICTED`
  in the land-use dedup logic (dead).
- `webapp/src/components/Map.tsx` — `createHorizontalLinePattern` helper now
  unused.
Left in place to avoid churn on the shared `FeatureType` union.

## Verified
- `tsc --noEmit` clean after all frontend edits.
- `restriction_level` classifier confirmed: `_CLOSED_ACCESS_VALUES = {"no",
  "private"}` + `landuse=military` → "closed"; everything else → "restricted".
