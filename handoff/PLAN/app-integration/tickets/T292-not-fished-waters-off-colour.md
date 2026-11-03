# T292 — Sewage basins, tailing ponds and dumps drawn in an off colour, never as fishing water

Status: PARTIAL — app + pipeline done and the tiles carry `kind`; left: the shots of the named tap sheet (owner 2026-10-29: "make sure they are coloured properly too in the ui. they should be a off color").

## Work
1. Find how FWA waterbodies typed Settling Basin-Sewage, Mine-Tailing Pond, Dump (and any similar man-made non-fishing
   type: e.g. Settling Basin-Industrial, Reservoir-Fish Hatchery? — list them, ask before adding beyond the owner's three)
   reach the map today (atlas lake layer and/or basemap water) and what colour/status they get (regs colour? open/closed?).
2. Draw them in a distinct muted "not a fishing water" colour (theme token, all palettes incl. colour-blind; legend entry),
   carry no regulation colouring, and a tap says what it is ("Sewage lagoon — not a fishing water") instead of opening regs.
   Through the atlas/tiles attributes (tiles and bundle same vintage) — no rule binding changes.
3. Search never offers them as waters.

## Acceptance
Screenshots of a sewage lagoon and a tailing pond (light/dark/colour-blind); binding diff shows no rule change; tap text.

Depends: none. Size: S–M. Owner: no.

## Findings (2026-10-29)
- FWA `manmade` layer: Settling Basin-Sewage EA26700110 **214**, Mine-Tailing Pond AP90300100 **59**, Dump AP09200000
  **2** (275, none named). Since T152 (live r-c706f5eaa2a3) they are out of `lake`, in the decorative tile layer
  `not_fishing_water` (275 features, no attributes), bound by no rule (reach subtracts them), in no water item (only 22
  `area` items contain them), not searchable. Drawn as a 0.45 wash of #BDB3A0 — over the basemap's own OSM water, which
  sits under most of them, so they still read pale blue (T152 shot). A tap did nothing.
- Other manmade types: Reservoir-Definite GB24300000 1,480, Reservoir-Intermittent GB90100110 34, Reservoir-Indefinite
  GB90100000 15, Canal GA03950000 8 (Vedder, Sumas Lake Canal). None clearly non-fishing; not touched. `lakes` and
  `wetlands` layers hold no such type.

## Outcome
- Pipeline: tile attribute `kind` (sewage | tailings | dump) on `not_fishing_water` (`fishing_water.NOT_FISHING_KIND`,
  layers.py, export.py, tile-contract.json); still decorative (no id). No binding code touched.
- Style: near-opaque off-colour fill + dashed edge (`not_fishing_water__edge`), tokens `color.notfishing.fill/edge`,
  `opacity.notfishing`, `width.notfishing.edge`, `dash.notfishing`: light #B1AF8B/#9F9556 0.9, dark #55562E/#ABAB87
  0.85, cvd #D1C6A9/#776C40 0.9 (ΔE ≥ 12 gate passes). Static in every view: no status/regs colouring.
- Tap: after the water sections, before tidal (`NOT_FISHING_LAYERS`, `pressNotFishing` web + native) → `NotFishingSheet`
  (`@app/core` `notFishingSheet`): "Sewage lagoon — not a fishing water" / "Mine tailings pond — …" / "Dump — …";
  a tile with no kind (today's atlas) says "Not a fishing water" generically.
- Legend: Standard map legend row "Not a fishing water" (fill + dashed rim); Help page row with the same swatch.
- Search: unchanged — none is a water item.
- Shots (headless, current tiles, dev app): `T292-shots/` sewage (Kamloops) + tailings (Gibraltar) × light/dark/cvd,
  plus the tap sheet.

## Left
- Shots of the named tap sheet (the tiles carry `kind` now).
