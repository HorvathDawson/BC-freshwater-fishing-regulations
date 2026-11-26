/**
 * WHERE THE MAP'S ARCHIVES AND ASSETS ARE SERVED FROM (`TileEndpoints`), and the satellite tile template a build uses
 * (`imageryTiles`, of the imagery provider named). Read by the style composer (`runtime-style.ts`) and handed in by the app.
 */
import type { ImageryProvider } from "@app/core";
import { MAP_STYLE } from "./style";


export interface TileEndpoints {
  /** Where the two archives are served from, with byte-range support. */
  atlas: string;
  basemap: string;
  /**
   * Label glyphs and icons. a shipped app serves its own copy (never a third-party CDN), since
   * a map that silently loses every label when offline is worse than one with no labels.
   */
  glyphs?: string;
  sprite?: string;
  /**
   * The release's map assets folder (P11/P18, ending in "/"): `fonts/{fontstack}/{range}.pbf` and
   * `sprites/v4/<light|dark>` under it. The installed folder on a phone (always offline), the
   * release url on the web. `glyphs`/`sprite` win when given. The app always names it (`releaseTiles`: the release's
   * folder, else the environment's `<data origin>/map-assets/v1/`); absent (tests), `NO_ASSETS`, which names no host.
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
  /**
   * (T831) HOW THE ATLAS IS READ BY BYTE RANGE outside the renderer — the route overlay's copy of its lines
   * (`trace-geometry.ts`). A phone's atlas is an installed `file://` archive, which `fetch` cannot read a range of (it
   * reads the whole file), so the app hands its file reader (`@app/data/release/platform` `atlasRange`), and a phone draws no
   * route lines without one (reported, never fetched). Absent on the web: the archive is read over HTTP by `pmtiles`.
   */
  readRange?: ReadRange;
}

/** (T831) `length` bytes of the archive at `path` from `offset` — fewer only at its end. */
export type ReadRange = (path: string, offset: number, length: number) => Promise<Uint8Array>;

/**
 * Each provider's keyed tiles, the same tile scheme under a token. Esri: World Imagery on the ArcGIS Location Platform.
 */
const KEYED: Readonly<Record<ImageryProvider["id"], string>> = {
  esri: "https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
};

/**
 * THE SATELLITE TILE TEMPLATE for a build, of the provider named: the style's own (its source in `layers.source.json`;
 * Esri's ArcGIS Online MapServer, as the original app used) with no key, or the provider's keyed endpoint with the
 * build's key (Esri: `EXPO_PUBLIC_ARCGIS_API_KEY`, set by the owner in the build environment and restricted by
 * referrer/app in the ArcGIS dashboard — never written into the code). A style with no such source is refused by name.
 */
export function imageryTiles(p: ImageryProvider, key: string | null): string {
  const own = (MAP_STYLE.sources as unknown as Record<string, { tiles?: string[] }>)[p.source]?.tiles?.[0];
  if (own === undefined) throw new Error(`imagery provider "${p.id}": the style has no tiles for its source "${p.source}"`);
  if (!key) return own;
  const query = own.includes("?") ? own.slice(own.indexOf("?")) : "";
  return `${KEYED[p.id]}${query}${query ? "&" : "?"}token=${encodeURIComponent(key)}`;
}

/**
 * (T270) OUR glyphs and sprites sit at a fixed path under each data origin (`/map-assets/v1/`), for a build whose release
 * names no assets folder (a release normally does: `tiles.assets`); the app builds that url from its environment's origin
 * (`apps/mobile/src/env.ts` `endpointsFor` → `tiles.assets`).
 * (T404) NO HOST HERE: a caller that names no assets folder (tests only — the app always passes its environment's,
 * `releaseTiles` requires it) gets the bare path, which names no environment. It used to be staging's data host, which
 * would have drawn a production build's labels from staging without a word.
 */
export const NO_ASSETS = "/map-assets/v1/";
