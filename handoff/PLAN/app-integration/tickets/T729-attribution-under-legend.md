# T729 — Map attribution goes under the legends, on every map tab

Status: OPEN (before closed testing) — code needs a change in `Shell.tsx`, held by lane T717 (see Left)

Source: owner 2026-11-14, verbatim: "attribution on app goes under the legends on the different map tabs."

## Reading (ours; confirm with the owner)
The owner's words are an instruction about where the credit sits: UNDER the legend, in the bottom block, not over the map
above it. (The first reading, "the legend covers the credit", is not what the code does: see Cause.) The credit is a licence
requirement (OpenStreetMap / Esri): it stays, always visible or one tap away; only its place changes.

## Cause (now)
- `map/src/MapCredits.tsx:66` `bottom = G.bottom + insetBottom`: the "©" chip (and Esri's badge while satellite is drawn)
  sits `insetBottom` px ABOVE the map's bottom edge.
- `ui-native/src/Shell.tsx:762` measures the foot block (`map-foot`: the legend / Runs foot) into `footH`, and
  `Shell.tsx:637` hands it to the browse map as `insetBottom`: so on Map, Conditions and Runs the credit rides on top of the
  legend (T275/T309: "lifted above the foot"). `map/src/Map.native.tsx:1007` / `Map.web.tsx:143` pass it through; the scale
  bar uses the same inset (`Map.native.tsx:1017`).
- The water page's map (`water/WaterMapView.tsx:118`) passes no inset: its bar is below the map, not over it; the credit
  already sits at the map's own bottom edge there, above the bar.

## Fix (one rule)
The credit row belongs to the foot block, as its LAST row (below the legend, above the tab bar), so its place follows the
legend's real height by construction, with no per-tab number:
- the map stops lifting the credit by the foot (a separate prop from the scale's inset: the scale stays above the foot);
- the foot block (`map-foot`, `Shell.tsx:761`) reserves one chip row under whatever legend is in it (collapsed or open,
  any tab, no legend at all) and the chip + Esri badge are drawn in that row (right-aligned, square, card, as now).
- No fallback: when the foot is not measured yet the row is simply at the bottom.

## Acceptance
- Unit: for Map / Conditions / Runs and legend open / folded / absent, the credit chip's bottom edge is at or below the
  legend's bottom edge and clear of the tab bar; satellite adds the Esri badge in the same row; fails before the fix.
- `MapCredits.render.test.tsx` updated (the `insetBottom={120}` case) to the new rule.
- Emulator/visual check (owed; T717 owns the emulator): Map, Conditions, Runs tabs x legend open/folded x light/dark x
  satellite on/off, plus the water page's map.

## Left
- Needs `ui-native/src/Shell.tsx` (dirty from T717): the foot's credit row and the separate credit/scale insets.
- Emulator check (above).
