/**
 * (T282) THE MAP'S VALUES WITHOUT THE MAP: the style's colours, its layer switches and numbers, the tile endpoints —
 * everything `index.ts` exports but the `Map` component. A screen that only colours a legend or reads the handover zoom
 * imports this, so the web's first bundle does not carry maplibre-gl (~1 MB) until a map is drawn (the map itself
 * loads with `@app/ui-native`'s later chunk). Same objects as `index.ts` (re-exported, not copied).
 */
export { imageryTiles, type TileEndpoints } from "./endpoints";
export { hiddenLayers, shownLayers, STYLE_META, FIELD_HANDOVER_Z, resolveTheme, waterStatusColour, tempBandColour,
         salmonStatusColour, gearEdgeColour, runStateColour, tidalColours, notFishingColours } from "./style";
export { colour, translucent } from "./chrome";
