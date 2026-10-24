/**
 * WHERE THE MAP'S ARCHIVES AND ASSETS ARE SERVED FROM (`TileEndpoints`), and the satellite tile template a build uses
 * (`imageryTiles`). Read by the style composer (`runtime-style.ts`) and handed in by the app.
 */
import { MAP_STYLE } from "./style";


export interface TileEndpoints {
  /** Where the two archives are served from, with byte-range support. */
  atlas: string;
  basemap: string;
  /**
   * Label glyphs and icons. Protomaps hosts these; a shipped app needs its own copy, since
   * a map that silently loses every label when offline is worse than one with no labels.
   */
  glyphs?: string;
  sprite?: string;
  /**
   * The release's map assets folder (P11/P18, ending in "/"): `fonts/{fontstack}/{range}.pbf` and
   * `sprites/v4/<light|dark>` under it. The installed folder on a phone (always offline), the
   * release url on the web. `glyphs`/`sprite` win when given; absent, the Protomaps CDN.
   */
  assets?: string;
  /**
   * The outside-BC mask: a rectangle with the province punched out, as GeoJSON.
   * ~61 KB, and changing how it looks never needs a tile rebuild.
   */
  outside?: string;
  /**
   * The gauges themselves, as GeoJSON points — one per station on screen, each with a
   * `label` the map draws beside it ("15.7 m³/s · p4th").
   *
   * A RUNTIME SOURCE, not a tile layer, because these move every half hour and the tiles
   * move once a build. It is also only ever a few hundred points, so the whole set can be
   * handed over on each refresh rather than diffed.
   */
  gauges?: string;
  /**
   * The satellite imagery's tile template (`imageryTiles`), when the build names one: an ArcGIS Location Platform key
   * makes it Esri's keyed endpoint. Absent, the style's own (`layers.source.json`, source `imagery`).
   */
  imagery?: string;
}

/** Esri's keyed World Imagery tiles (ArcGIS Location Platform), the same MapServer tile scheme under a token. */
const ESRI_KEYED = "https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}";

/**
 * THE SATELLITE TILE TEMPLATE for a build: the style's own (Esri's ArcGIS Online MapServer, as the original app used)
 * with no key, or the Location Platform endpoint with the build's key (`EXPO_PUBLIC_ARCGIS_API_KEY`, set by the owner
 * in the build environment and restricted by referrer/app in the ArcGIS dashboard — never written into the code).
 */
export function imageryTiles(key: string | null): string {
  const own = (MAP_STYLE.sources as unknown as Record<string, { tiles?: string[] }>).imagery?.tiles?.[0] ?? "";
  if (!key) return own;
  const query = own.includes("?") ? own.slice(own.indexOf("?")) : "";
  return `${ESRI_KEYED}${query}${query ? "&" : "?"}token=${encodeURIComponent(key)}`;
}

/** The Protomaps CDN's glyphs and sprites, when the release names no assets folder. */
export const PM_ASSETS = "https://protomaps.github.io/basemaps-assets";
