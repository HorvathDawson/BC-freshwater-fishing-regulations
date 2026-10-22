# Map polish: the refinement pass, map styling (owner 2026-10-22)

Owner, 2026-10-22: "right now it just feels unrefined." This lane did the five MAP STYLING items of that pass
(`handoff/PLAN/app-integration/decisions.md`, "OWNER 2026-10-22 — refinement pass"). The More tab, the top-bar sync icon,
the sheet backdrop motion and the Conditions legend/histogram are other lanes' and were not touched here; nor were the
Lucide glyph paths.

## Shots

`before/` is `redesign/stream-sections` at `aaed2706`; `after/` is this branch. Both ran in headless Chrome against this
worktree's Expo web server, with data from the main checkout's `serve-tiles.mjs` (the live release, read-only, on a port
of this lane's own). Names are `<before|after>-<mode>-<zoom>-<phone|desktop>-<light|dark>.jpg` (JPEG q85 at full 2x, `scripts/to_jpeg.py`):

- **modes**: `regs` = the Map tab coloured by regulations; `plain` = the Map tab with every regulations-coloured layer
  repainted in its plain mode (`scripts/plain-paint.ts`: exactly what `paintFor` paints when no day is installed, set
  through the dev-only `__map`); `satellite` = regs over Esri imagery; `conditions` = the Conditions tab (flow). Private
  land is switched ON in every shot, because the owner judged it on.
  - **The first `plain` shots were not plain** (review 2026-10-22). They blocked the status index by URL, but the
    release path reads the day's status from `paint`, which nothing blocked, so every "plain" shot was the regs map.
    Blocking `paint` is not an option: the web boot fetches it before the map. The app has no plain mode to switch to,
    so the legend card in the `plain` frames still names Regulations; the WATER in them is plain.
- **zooms**: z6 southern B.C. (regions, the basin field); z9 the Lower Mainland and Fraser Valley (towns, reserves,
  closures, rivers); z12 Pitt Meadows (Blaney Bog and Codd Wetland closed, UBC Malcolm Knapp Research Forest by permit,
  parcels); z15 the edge of Blaney Bog (a closure edge, private land, a creek).
- **viewports**: phone 390x844 and desktop 1440x900, at 2x.
- `after/handover-*` (phone and desktop, light and dark): the Conditions map either side of the new handover, z9.0 / z9.5 / z9.9 / z10.0.

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
  - **No tile rebuild needed.** The tiles still carry basins only to z8 (`BASIN_HANDOVER_Z = 9` in the tile builder,
    unchanged). The basin layer now reads its own source, `basins`. This is the same atlas archive with `maxzoom: 8`, so
    MapLibre overzooms the z8 basin tiles up to z10 and never asks for z9 tiles, which have no basins in them.
    Feature-state goes to the new source unchanged on the web (`stateTarget` reads the layer's source). On a phone the
    field's values were already written into the style (`areaLayer`, now any source on the atlas archive). `promoteId`
    is set for both atlas sources.
  - **The overzoomed field opened gaps. That is fixed** (review 2026-10-22). The first version claimed "z8 geometry at
    z10 has no visible stair-stepping". That was wrong: the first `after/handover-*` shots showed white and black
    wedges along the basin borders at Gill Bar, Cheam and Chilliwack, and a sawtooth edge. The cause is in the tile
    build, not in the zoom. `basins.py` simplified each basin on its own at 500 m, so two neighbours cut their shared
    border differently. Measured on the shipped `basin.geojsonl`: 3,552 slivers and 3,694 overlaps, the widest about
    700 m across. That is 7 px at z10, and the translucent field drew each overlap as a darker seam.
    - **In the style, now:** the field is **opaque**. Each colour is pre-mixed on the paper at `opacity.basin`
      (`field.solid`, `mixColours` in `adapters/contract.ts`), so a basin has the same colour the 0.55 field had on
      blank paper and overlaps are invisible. A **seam** in each basin's own colour is drawn *under* the field
      (`basin__edge`, `outline.under`). It shows only where no basin is drawn, and its width curve closes a 700 m gap at
      every zoom the field draws (`width.basin.seam`; `tools/handover.test.ts` holds the curve to the measured gap).
      The phone reads the seam's colour from the field's own table (`native-style.ts` `areaKey`).
    - **At the source, on the next atlas build:** `basins.py` now simplifies the groups **as one coverage**
      (`simplify_cover`, `shapely.coverage_simplify`), so every shared border is simplified once. On the real layer at
      500 m: 0 overlaps and no slivers (one zero-width ring). Tested in `pipeline/tests/test_tiles.py`; the old
      per-polygon simplify fails that test.
    - **The sawtooth edge** (the right-hand basin in `after-conditions-z9-*`) is the 500 m tolerance itself: it was one
      pixel at z8, and overzoomed to z10 every 500 m run is a 5 px straight edge. No style can add detail the tile
      does not carry, so this one waits for the build. `SIMPLIFY_M` is now 100 m, one pixel at z10 at 49 N
      (`handover.test.ts` holds it to a pixel at the handover): 494k vertices of the source's 5.46M, 8.5 s. The z8
      tiles are thinned again by tippecanoe's own per-zoom simplification, so the low zooms do not grow much.
- **Nothing under the field.** While the field is drawn, every layer it covers takes the handover as its floor. That is
  the streams and lakes, their names, outlines, arrows and no-baseline dashes, the wetlands, the routes through lakes,
  contours, and the basemap's own river lines (`field.covers` in `layers.source.json`, applied by `zoomFor` /
  `fieldMoved` in `adapters/contract.ts`). Before, the lake outlines, the lake names (from z8) and the dashes all drew
  through it (`before-conditions-z6-*`). The floor is released with the field, so the Map tab and the temperature view
  keep every layer's own floor.
  - **The first version still let things through** (review 2026-10-22). The basemap's water (the Fraser, Harrison,
    Stave and Pitt lakes) showed as darker shapes through the 0.55 field. The closed and permit hatches showed too
    (Liumchen, Keyhole Canyon), and so did the gauge dots. Now the field is opaque, so nothing drawn under it (the
    basemap's water, roads and landcover, our areas and hatches) can show through. The things drawn *above* every fill
    are also covered: the parks, reserves, closures, permit areas and Indigenous land, with their edges and names, and
    the gauge dots and their pills (`covers.runtime`: `gauge-dot`, `gauge-label`). Only the basemap's place names and
    road shields stay on the field, because they are how you know where you are.
  - **The cost:** the field hides the roads and the coastline detail under it below z10. That is the trade the owner
    asked for ("disappear when it is visible"). Towns stay named, and the BC mask still draws the border.
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
- **A water's name never reads as a town's** (review 2026-10-22). Dark's #C8D0F0 cleared the statuses, but it sat
  ΔE 8.9 from the place ink #D2D8E1 (3.9 under tritanopia). In `after-regs-z9-desktop-dark` Pitt, Harrison and Chehalis
  Lake looked the same pale grey-white as Hemlock Valley, and only the italic told them apart. No blue can be the dark
  water ink: every blue that clears 4.5:1 on the dark paper is under 12 from `status.base`, `status.restricted` or
  `status.open` for some reader (best: ΔE 11.0). So dark water names are now a muted sea-green, **#5AB49C**. It is
  ΔE 26.7 from the place ink (15.7 for the worst reader), 13.5 from every status for the worst reader, ΔE76 ≥ 12 from
  every map colour including the flow ramp (the style build's rule; #18C09C, the first pick, was 6.9 from `flow.f4`), and
  7.9:1 on the paper. The imagery water ink moved #D3EBFF → **#B4DCFF**, from 12.1 to 18.0 against the imagery place
  ink. `label-colours.test.ts` now holds water against place: ΔE ≥ 15 for normal vision and ≥ 10 for the worst reader,
  on the drawn map and over imagery. Light (18.7 / 13.5) and cvd (19.2 / 15.1) already passed.
- **Names without record ids.** The access and protected-area names carry the source id ("Codd Wetland Ecological
  Conservancy Area [14448346]"). The label drops a trailing " [...]" (`trimId`, a style expression, no rebuild).
- **Priority** is unchanged: water names first, then admin numbers, then areas, then the basemap's, so the important
  names win collisions.

### 3. Private land: quiet, and from z11

The fill went from 0.22 to 0.10 (0.08 in dark). The edge went from a 1.5 px solid line to a 0.8 px hairline at 0.5
opacity (0.42 in dark). Light uses a softer brown, #9A6A4E. The very salmon dark-theme fill is gone. The layer still
sits under every water colour.

**From z11: built in the pipeline, NOT yet on screen** (review 2026-10-22: the first version called this built when
only the style had moved). The style asks for parcels from z11, and the tile builder now writes them from z11
(`pipeline/deliver/tiles/layers.py` `parcel` minzoom 11, `tile-contract.json` regenerated, `test_tiles.py` holds it).
The atlas served today was built at z12, so **nothing changes on screen until the next atlas build**, and none of the
shots here show private land at z11. Cost, measured with the build's own tippecanoe flags on the shipped
`parcel.geojsonl`: z11+z12 is 5.4 MB against 3.3 MB for z12 alone, so about **+2 MB** on a 521 MB atlas. The parcel
layer file carries `tippecanoe.minzoom` per feature, so the rebuild has to re-export it (not `--tiles-only`).

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

- Review fixes (2026-10-22), each test written first and seen failing:
  - `handover.test.ts`: the field is opaque with every colour pre-mixed on the paper (light, dark, cvd); the seam is
    drawn directly under it, follows it, takes its colour, and is wider than the 700 m gap at every zoom it draws;
    the areas, their edges and names and the gauge dots and pills are covered and released; the seam is hidden with
    the field; the tile builder's `SIMPLIFY_M` is at most a pixel at the handover.
  - `label-colours.test.ts`: water ink against place ink, on the drawn map and over imagery (failed in all three
    themes before: dark 8.9 / 3.9, and 12.1 for the imagery pair).
  - `native-style.test.ts`: the seam reads the field's own table on a phone (no feature-state).
    `lake-outline.test.ts` names the seam as a third follower.
  - `pipeline/tests/test_tiles.py`: basins simplified as one coverage do not gap or overlap (the per-polygon
    simplify fails it); the parcel layer and the tile contract start at z11.

## For the merger

- `pnpm -C app style:build` is clean and the generated files are committed. The full
  `REGS_STORE_REQUIRED=1 pnpm -C app check` gave exit 0: 292 test files passed and 4 skipped; 7496 tests passed and 41
  skipped. After the review fixes (run unfiltered): exit 0, 292 files passed and 4 skipped, 7506 tests passed and 41
  skipped; `pipeline/tests/test_tiles.py` and the `test_tile_*` files pass.
- **Nothing shipped here needs a tile rebuild to look right.** Two pipeline changes take effect only on the next atlas
  build: **private land from z11** (`parcel` minzoom 11, about +2 MB) and **basins simplified as one coverage** (no
  slivers at the source; the style's seam covers them until then). Both are in the export, so the rebuild must
  re-export the `parcel` and `basin` layer files, not run `--tiles-only`. The basins re-export alone takes ~10 s;
  the parcel layer is part of the admin export. Run it as one heavy job, and promote the archive and the bundle together
  (same vintage).
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
- Private land at z11 is not on screen until the next atlas build (see item 3 and "For the merger").
- The basins in the shipped tiles still gap and overlap. The style hides that (opaque field plus seam), and the fix at
  the source lands with the next atlas build.
- Point-of-interest and shield icons keep the Protomaps sprites. Only their text was unified.
- The record ids are trimmed from area names in the style. The pipeline still writes them into `name`; this needs a
  rebuild to fix at the source.
