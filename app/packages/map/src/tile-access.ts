/**
 * (T1154) HOW A RENDERER'S STYLE SOURCES ADDRESS THE MAP'S TWO ARCHIVES (`TileAccess`), apart from where the archives
 * are (`TileEndpoints.atlas` / `.basemap`: a file on a phone, an https url on the web). Those paths stay the release's:
 * `at.atlas` is also the pairing key and the route reader's path (`readRange`), so an engine's address scheme is never
 * written into them; it is applied here, when the style is composed (`runtimeStyle`).
 */

export interface TileAccess {
  /** The style source url that reads the archive at `path` (a `TileEndpoints` archive path). */
  readonly archive: (path: string) => string;
}

/**
 * MapLibre's PMTiles protocol: `pmtiles://` + the archive's own address, read by byte range. Both MapLibre renderers read
 * it (the web over https, the phone from its installed `file://` archives), so it is the one access the map has today.
 */
export const PMTILES: TileAccess = { archive: (path) => `pmtiles://${path}` };
