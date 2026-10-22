# Map polish: the refinement pass, map styling (owner 2026-10-22)

Owner, 2026-10-22: "right now it just feels unrefined." This lane did the five MAP STYLING items of that pass
(`handoff/PLAN/app-integration/decisions.md`, "OWNER 2026-10-22 — refinement pass"). The More tab, the top-bar sync icon,
the sheet backdrop motion and the Conditions legend/histogram are other lanes' and were not touched here; nor were the
Lucide glyph paths.

## Shots

`before/` is `redesign/stream-sections` at `aaed2706`; `after/` is this branch. Both ran in headless Chrome against this
worktree's Expo web server, with data from the main checkout's `serve-tiles.mjs` (the live release, read-only, on a port
of this lane's own). Names are `<before|after>-<mode>-<zoom>-<phone|desktop>-<light|dark>.jpg` (JPEG q85 at full 2x, `scripts/to_jpeg.py`):

- **modes**: `regs` = the Map tab coloured by regulations; `plain` = the Map tab with the status index withheld, so every
  water is neutral (the state before the day's index is in); `satellite` = regs over Esri imagery; `conditions` = the
  Conditions tab (flow). Private land is switched ON in every shot, because the owner judged it on.
- **zooms**: z6 southern B.C. (regions, the basin field); z9 the Lower Mainland and Fraser Valley (towns, reserves,
  closures, rivers); z12 Pitt Meadows (Blaney Bog and Codd Wetland closed, UBC Malcolm Knapp Research Forest by permit,
  parcels); z15 the edge of Blaney Bog (a closure edge, private land, a creek).
- **viewports**: phone 390x844 and desktop 1440x900, at 2x.
- `after/handover-*`: the Conditions map either side of the new handover, z9.0 / z9.5 / z9.9 / z10.0.

Scripts: `scripts/shots.mjs` (every shot above; the camera is set exactly through the dev-only `__map`), `probe.mjs`,
`features.mjs`, `eval.mjs` (finding scenes and reading paint off the live map), `sheet.py` (side-by-side sheets), and
`label-search.ts` (the water-label ink search behind item 2).

## What changed, and why

### 1. The Conditions watershed field: to z10, and nothing drawn under it

- **The handover moved from z9 to z10.** The basin field now draws to z10 and the rivers come back at z10
  (`basin.maxzoom`, `minzoomByMode.standing` on stream / lake / lake_flowing / wetland_flowing, Shell `HANDOVER_Z`).
  - **Why z10.** In `after/handover-*`, z9 to z9.9 is still a valley-wide view: Pitt, Stave and Harrison lakes and the
    Fraser all fit on a phone. The coloured rivers at that scale are 1 to 2 px hairlines over a whole valley. The field
    says the same thing in one shape you can read. By z10 a phone shows about 40 km, the reaches are wide enough to
    carry the ramp and to tap, and the field would be one flat colour across the screen. A half step (9.5) would still leave
    z9.5 to z10 as hairlines, which is the band where a valley fills the screen and the field reads best. (Not shot as
    an alternative; judged from the handover shots.)
  - **The home camera moved from z9.4 to z10** (Shell `HOME`). Any handover above 9.4 puts the app's first view of
    Conditions under the field, where taps are refused (`HANDOVER_Z`). At z10 both tabs still open on the Fraser Valley,
    slightly closer in, on tappable rivers. One zoom out shows the field.
  - **No tile rebuild.** The tiles still carry basins only to z8 (`BASIN_HANDOVER_Z = 9` in the tile builder, unchanged).
    The basin layer now reads its own source, `basins`. This is the same atlas archive with `maxzoom: 8`, so MapLibre
    overzooms the z8 basin tiles up to z10 and never asks for z9 tiles, which have no basins in them. A basin is a
    catchment of thousands of km², so z8 geometry at z10 has no visible stair-stepping. Feature-state goes to the new
    source unchanged on the web (`stateTarget` reads the layer's source). On a phone the field's values were already
    written into the style (`areaLayer`, now any source on the atlas archive). `promoteId` is set for both atlas sources.
- **Nothing under the field.** While the field is drawn, every layer it covers takes the handover as its floor. That is
  the streams and lakes, their names, outlines, arrows and no-baseline dashes, the wetlands, the routes through lakes,
  contours, and the basemap's own river lines (`field.covers` in `layers.source.json`, applied by `zoomFor` /
  `fieldMoved` in `adapters/contract.ts`). Before, the lake outlines, the lake names (from z8) and the dashes all drew
  through it (`before-conditions-z6-*`). The floor is released with the field, so the Map tab and the temperature view
  keep every layer's own floor.
- **The field is not drawn when it answers nothing.** Its `plain` colour was a #D5D5D5 wash at 0.55 opacity over the
  WHOLE Map tab below z9. That is the grey cast in `before-regs-z6-*`, where the province looked drained of landcover.
  The layer is now drawn only in `standing` (`field.drawnIn`), so on the Map tab it is neither drawn nor fetched.
- **Conditions hides the boundaries for real.** The view's `hide` named `region` and `mu`, but their generated edges and
  numbers kept drawing over the field: a grey line and a "3" across the flow colours. A hidden layer now takes its
  `__edge` / `__label` / `__arrows` companions with it (build-style).

### 2. One label style

The rules live in `packages/map/src/labels.ts`, and `tools/label-colours.test.ts` checks them.

- **Three inks and one halo.** Water names use `color.label.water`. Every name a reader steers by uses
  `color.label.place`: towns and islands, and also our parks, reserves, closures and Indigenous land. Those area names
  used to be drawn in crimson, aubergine, green and brown, so a closure's name read as one more closed thing. Roads,
  addresses, neighbourhoods and points of interest use `color.label.minor` (new, an alias of the boundary grey). Region
  and unit numbers use the boundary grey. Every name sits on the map's paper (`color.outside`), with one halo width
  (1.4–1.5 px, 2 px for the large admin numbers). Area names use Regular type, water names Italic, admin numbers Medium.
  The size ramps are evened out by kind.
- **The basemap's names wear the same inks.** Protomaps drew points of interest in a park green and a station blue a
  step from `status.base`, and region names in a pale grey. Its name layers are now painted from our tokens. Road
  shields keep their own text, which sits on the shield.
- **Satellite.** Every name, ours and the basemap's, turns light with a near-black halo over imagery
  (`color.ui.imagery.label` / `-water` / `-halo`, the same in every theme), and goes back to its theme ink when the drawn
  map returns. This is switched in the same pass as the ground (`setBasemap(h, kind, theme)`).
- **No label colour collides with a status colour.** The water inks moved: light #0D47A1 → #0820A8, dark #A8CDF2 →
  #C8D0F0, cvd #123C63 → #0030B8. The light one sat ΔE 7.0 from `status.open` under tritanopia, and every theme was
  under 12 from restricted or open for some reader. Each ink now holds ΔE ≥ 12 from all five statuses for the worst
  reader (normal vision and protan / deutan / tritan, Viénot and Machado), and 4.5:1 on the paper. 12 is the style
  build's own floor; `scripts/label-search.ts` shows no blue reaches 15 in light or dark. The quiet inks (roads, admin
  numbers) are held to 3:1, the large-text and non-text floor, because a darker grey collides with the mask and the place
  ink.
- **Names without record ids.** The access and protected-area names carry the source id ("Codd Wetland Ecological
  Conservancy Area [14448346]"). The label drops a trailing " [...]" (`trimId`, a style expression, no rebuild).
- **Priority** is unchanged: water names first, then admin numbers, then areas, then the basemap's, so the important
  names win collisions.

### 3. Private land: quiet, and from z11

The fill went from 0.22 to 0.10 (0.08 in dark). The edge went from a 1.5 px solid line to a 0.8 px hairline at 0.5
opacity (0.42 in dark). Light uses a softer brown, #9A6A4E. The very salmon dark-theme fill is gone. The layer still
sits under every water colour. It starts at **z11 in the style**, but the tiles carry parcels only from z12, so **z11
needs a tile rebuild** with `parcel` minzoom 11 in `pipeline/deliver/tiles/layers.py`. That is NOT done here (no heavy
jobs). Until then the layer starts at z12, as it did before.

### 4. Restricted and closed areas

- **A hatch that thins zoomed out.** Fill opacity is now a zoom curve of `opacity.closure`: ×0.45 at z6, ×0.7 at z9,
  full at z12, ×0.8 at z16 (`opacity.ramp`, new in the build). Zoomed out, a closure was a few pixels of dense crimson
  weave. Now it stays a mark without becoming a smudge.
- **A finer weave.** Closed areas (no access, ecological reserves) use a cross-hatch at an 8 px pitch with 1.0 px lines,
  where it was 11 px with 1.3 px lines. Permit areas use ONE diagonal at 6 px, so severity is carried by the pattern as
  well as the colour, for a reader who cannot separate the two hues. National and historic parks use one diagonal at
  8 px.
- **A crisp edge close in.** From z10, every closed and permit area gets a solid edge in its own status colour at 0.85
  opacity. It is a hairline at z10 (0.6 px), 1.2 px at z13 and 2 px at z16 (an outline width zoom curve, new in the
  build). Before, the edge started at z11 at 0.35 opacity, and the boundary was a guess at the edge of a weave.
- All colours are still palette D's status tokens. On the phone the hatch is not drawn (no `addImage`, N2-F1), and the
  zoom curve and the edge apply there too.

### 5. The scale bar

The scale bar is now a ruler: a 1.5 px rule with 7 px end ticks, the distance written above its left end in Archivo
semibold 11 px with tabular figures, and a halo in the card colour instead of a panel. It was a translucent card with a
2 px frame on three sides. It is still metric, still at most 92 px wide, still bottom-right, and still kept exact by
MapLibre through a pinch. The phone draws the same ruler (`Map.native.tsx`). The `--map-scale-bg` variable, `scaleBg`
and the `opacity.ui.scale` token went with the panel.

## Tests

- New: `tools/label-colours.test.ts` (14 tests). It checks that no label ink is within the floor of a status for any
  reader, that every ink reads on its halo on both grounds, that every name layer (ours and the basemap's) wears a label
  ink, that every name over imagery is light on dark, and the trimmed-name expression (whole-string). A mutation check
  passed: putting the old water inks back fails the first test (light ΔE 12.9 / 7.0).
- Updated to the new truth: `handover.test.ts` (z10, plus the field's own source stopping at the tiles' last basin
  zoom, and two new behaviour tests: everything covered takes the floor while the field is drawn, and every floor is
  released when it is not); `overzoom.test.ts` (the `basins` source's maxzoom is the tile builder's basin maxzoom);
  `tile-contract.test.ts` (layers on any atlas-archive source are held to the tile contract); `flow-arrows.test.ts`
  (arrows from z10); `parity.test.ts` / `sections.test.ts` (the basin's feature-state target is `basins/basin`; the
  Conditions hide list includes the companions); `style-spec.test.ts` (validates the `basins` source);
  `ConditionsLayers` / `MapRail` / `Search` shell tests (the expanded hide list, and the home camera at z10).

## For the merger

- `pnpm -C app style:build` is clean and the generated files are committed. The full
  `REGS_STORE_REQUIRED=1 pnpm -C app check` gave exit 0: 292 test files passed and 4 skipped; 7496 tests passed and 41
  skipped.
- **No tile rebuild is needed for anything shipped here.** One item needs a rebuild to finish: private land from z11.
  To do it, set `parcel` minzoom to 11 in `pipeline/deliver/tiles/layers.py` and rebuild the atlas. The parcel layer
  gets bigger at z11, so measure that first. Until then, nothing changes below z12.
- If the tile builder's `BASIN_HANDOVER_Z` ever changes, set `sources.basins.maxzoom` in `layers.source.json` to the
  new basin maxzoom. `tools/handover.test.ts` and `tools/overzoom.test.ts` both fail until the two agree.
- Conflict risk: `Shell.tsx` (`HOME`, `HANDOVER_Z`) and the shell tests whose strings include the camera (MapRail,
  Search, ConditionsLayers). If another lane changed those strings, the camera is now `49.15,-121.85,10` and the
  Conditions hide list is `mu,mu__edge,mu__label,region,region__edge,region__label`.

## Not done

- The Conditions legend/histogram, the More tab, the sync icon, the sheet backdrop motion and the Lucide glyphs belong
  to other lanes.
- Nothing was checked on a phone device. The native style is covered by the native-style and parity tests only: the
  floors, the field's visibility, the label paints and the ruler.
- The basemap's own water still shows faintly through the field (the ocean and wide rivers are one Protomaps layer).
  Hiding it would take the coastline with it.
- Point-of-interest and shield icons keep the Protomaps sprites. Only their text was unified.
- The record ids are trimmed from area names in the style. The pipeline still writes them into `name`; this needs a
  rebuild to fix at the source.
