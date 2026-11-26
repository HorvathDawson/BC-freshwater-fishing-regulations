# T292 — Sewage basins, tailing ponds and dumps drawn in an off colour, never as fishing water

Status: PARTIAL — f67bd7142: app + pipeline done and the tiles carry `kind` (since the 10-30 build); left (Claude): the shots of the named tap sheet on current tiles (owner 2026-10-29: "make sure they are coloured properly too in the ui. they should be a off color").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T292 | Sewage basins, tailing ponds, dumps: off "not a fishing water" colour, no regs, tap says what it is, not in search | PARTIAL | f67bd7142 | left (Claude): the named tap-sheet shot on current tiles |

## What
FWA waterbodies typed Settling Basin-Sewage, Mine-Tailing Pond, Dump are drawn in a muted "not a fishing water" colour
(theme token, all palettes incl. colour-blind; legend entry), carry no regulation colouring, a tap says what it is
("Sewage lagoon — not a fishing water") instead of opening regs, and search never offers them. Through the atlas/tiles
attributes (tiles and bundle same vintage) — no rule binding changes. Ask before adding types beyond the owner's three.

Done (f67bd7142):
- FWA `manmade` layer: Settling Basin-Sewage EA26700110 **214**, Mine-Tailing Pond AP90300100 **59**, Dump AP09200000
  **2** (275, none named); since T152 in the decorative tile layer `not_fishing_water`, bound by no rule, in no water
  item, not searchable. Other manmade types (Reservoir-Definite GB24300000 1,480, Reservoir-Intermittent GB90100110 34,
  Reservoir-Indefinite GB90100000 15, Canal GA03950000 8: Vedder, Sumas Lake Canal): none clearly non-fishing; not touched.
- Pipeline: tile attribute `kind` (sewage | tailings | dump) on `not_fishing_water` (`fishing_water.NOT_FISHING_KIND`,
  layers.py, export.py, tile-contract.json); still decorative (no id).
- Style: near-opaque fill + dashed edge (`not_fishing_water__edge`), tokens `color.notfishing.fill/edge`,
  `opacity.notfishing`, `width.notfishing.edge`, `dash.notfishing`: light #B1AF8B/#9F9556 0.9, dark #55562E/#ABAB87
  0.85, cvd #D1C6A9/#776C40 0.9 (ΔE ≥ 12 gate passes).
- Tap: after the water sections, before tidal (`NOT_FISHING_LAYERS`, `pressNotFishing` web + native) → `NotFishingSheet`
  (`@app/core` `notFishingSheet`): "Sewage lagoon — not a fishing water" / "Mine tailings pond — …" / "Dump — …";
  a tile with no kind says "Not a fishing water" generically. Legend + Help page row with the same swatch.
- Shots: `T292-shots/` sewage (Kamloops) + tailings (Gibraltar) × light/dark/cvd, plus the tap sheet.

## Acceptance
Screenshots of a sewage lagoon and a tailing pond (light/dark/colour-blind); binding diff shows no rule change; tap text.

Depends: none. Size: S–M. Owner: no.

## Left
- Shots of the named tap sheet (the tiles carry `kind` now).
