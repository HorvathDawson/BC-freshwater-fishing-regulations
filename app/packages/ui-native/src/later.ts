/**
 * (T282) EVERY PART THE WEB LOADS LATER, in one chunk (`part.tsx`). One module, not one `import()` per component: Metro
 * then emits a single extra chunk and keeps the page's own script tags as they were (several async chunks sharing code
 * add a `__common` chunk and a runtime chunk to index.html, which the stored SEO water pages do not name).
 *
 * (T910) The water's REGULATIONS PAGE is here (`WaterScreen`, ~150 KiB): a start on a water's own page (`/water/<slug>`)
 * asks for this chunk at once (`preloadParts`, apps/mobile startLink.web); any other start draws the map first (MapScreen
 * is in the first script: maplibre-gl was there anyway, through `@app/map`) and this chunk comes once the line is quiet
 * and the browser idle (Shell `preloadParts`), or when a part is first drawn (its loading face until then).
 */
export { SpotCapture } from "./SpotCapture";
export { ObstacleSheet } from "./ObstacleSheet";
export { NotFishingSheet } from "./NotFishingSheet";
export { RunsSheet } from "./RunsSheet";
export { SyncErrorSheet } from "./SyncErrorSheet";
export { SpotHost } from "./shell/SpotHost";
export { SpotsScreen } from "./SpotsScreen";
export { MoreScreen } from "./menu/MoreScreen";
export { WaterConditions, WaterMap } from "./WaterViews";
export { WaterMapView } from "./water/WaterMapView";
export { MenuHost } from "./menu/MenuHost";
// (T910) the water's Regulations page (shell/WaterModeHost): ~150 KiB of the first script
export { WaterScreen } from "./WaterScreen";
