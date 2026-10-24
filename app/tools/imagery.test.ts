/**
 * THE SATELLITE GROUND IS ESRI WORLD IMAGERY, ONLINE-ONLY (owner 2026-10-21, decisions.md), replacing EOX's 10 m 2016
 * mosaic. Pinned here, against the style both maps are built from:
 *
 *   - the source: Esri's World_Imagery MapServer tiles, `blankTile=false` (a place with no imagery at a zoom answers
 *     404 and the renderer overzooms the parent, instead of drawing Esri's grey "Map data not yet available" tile),
 *     to Esri's z19;
 *   - its credit is Esri's required wording, the same words the app's credit list uses (`IMAGERY_CREDIT`);
 *   - an ArcGIS Location Platform key, when the build has one, swaps the template whole (`at.imagery`);
 *   - the phone's style carries Esri's credit only while satellite is drawn (the web's control drops a hidden layer's
 *     source by itself: MapLibre lists only sources a visible layer uses);
 *   - the BC 'Access Only' imagery is never referenced.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { IMAGERY_CREDIT } from "@app/core";
import { runtimeStyle } from "@app/map";
import { imageryTiles } from "../packages/map/src/endpoints";
import { nativeStyle } from "../packages/map/src/native-style";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");
const ESRI = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
  + "?blankTile=false";
const AT = { atlas: "https://example.invalid/atlas.pmtiles", basemap: "https://example.invalid/basemap.pmtiles" };
type Src = { tiles?: string[]; attribution?: string; maxzoom?: number; tileSize?: number; type?: string };
const text = (html: string) => html.replace(/<[^>]+>/g, "");

describe("the satellite source", () => {
  it("is Esri World Imagery, to z19, credited as Esri requires — on both maps", () => {
    const web = runtimeStyle(AT, "light").sources.imagery as Src;
    const phone = nativeStyle({ at: AT, theme: "light", view: "plain", basemap: "satellite", gauges: "" })
      .style.sources.imagery as Src;
    const want = { type: "raster", tiles: [ESRI], tileSize: 256, maxzoom: 19,
                   attribution: 'Powered by <a href="https://www.esri.com">Esri</a> · Source: Esri, Vantor, '
                     + "Earthstar Geographics, and the GIS User Community" };
    expect({ web, phone }).toEqual({ web: want, phone: want });
  });

  it("its map credit and the app's credit line are the same words", () => {
    const style = runtimeStyle(AT, "light").sources.imagery as Src;
    expect(`Satellite imagery: ${text(style.attribution!)}`).toBe(IMAGERY_CREDIT);
  });

  it("a Location Platform key swaps the template whole (EXPO_PUBLIC_ARCGIS_API_KEY); none keeps Esri's own URL", () => {
    expect(imageryTiles(null)).toBe(ESRI);
    expect(imageryTiles("k-123")).toBe("https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer"
      + "/tile/{z}/{y}/{x}?blankTile=false&token=k-123");
    const keyed = runtimeStyle({ ...AT, imagery: imageryTiles("k-123") }, "light").sources.imagery as Src;
    expect(keyed.tiles).toEqual([imageryTiles("k-123")]);
  });

  it("EOX and the BC 'Access Only' imagery are gone from the style, the runtime and the app", () => {
    const files = ["app/packages/map/style/layers.source.json", "app/packages/map/style/style.json",
                   "app/packages/map/src/runtime-style.ts", "app/packages/map/src/endpoints.ts", "app/apps/mobile/src/App.tsx",
                   "app/apps/mobile/src/config.ts"];
    expect(files.filter((f) => /eox|openmaps\.gov\.bc\.ca|imagex/i.test(read(f)))).toEqual([]);
  });
});

describe("the phone's credit follows the ground drawn", () => {
  const credit = (basemap: "map" | "satellite") =>
    (nativeStyle({ at: AT, theme: "light", view: "plain", basemap, gauges: "" }).style.sources.imagery as Src)
      .attribution ?? null;
  it("Esri's credit while satellite is drawn; none on the drawn map", () => {
    expect({ satellite: credit("satellite") !== null, map: credit("map") }).toEqual({ satellite: true, map: null });
  });
});
