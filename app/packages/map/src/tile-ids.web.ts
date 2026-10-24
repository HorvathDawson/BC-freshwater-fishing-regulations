/**
 * A LOADED TILE'S SECTION IDS, read out of maplibre-gl — the cheap way, through its INTERNALS (colour-latency AP).
 *
 * The colouring engine (`colouring.ts`) labels a tile's sections in the tile's own load event, so they are
 * coloured on its first frame. GL JS hands the event the `Tile`, but no public call lists one tile's features. Its
 * internals do: `tile.latestFeatureIndex.loadVTLayers()` decodes the tile's own protobuf (on the main thread; cached
 * on the index, so a later `queryRenderedFeatures` reuses it), and `layer.feature(i).id` is the MVT id — which the
 * pipeline writes as the section handle (pipeline/deliver/tiles/layers.py), the same id `promoteId` lifts out of
 * `section_id` for feature-state.
 *
 * PINNED TO 5.24 (`GL_INTERNALS`; `tools/gl-internals.test.ts` fails on any other installed version and checks the
 * names below are still in its bundle and source). Duck-typed, never imported: on a build that does not look like 5.24
 * these return null and the map takes the PUBLIC route instead (`querySourceFeatures`, in web/handles.ts) — heavier,
 * measured in the colour-latency README, never wrong.
 *
 * Web only: the native binding has no per-tile event at all (`tile-ids.native.ts`).
 */

/** The maplibre-gl minor these internals were read from. */
export const GL_INTERNALS = "5.24";

type VTLayer = { length: number; feature(i: number): { id?: number } };
type GLTile = { state?: string; latestFeatureIndex?: { loadVTLayers?: () => Record<string, VTLayer | undefined> } | null };

/**
 * One tile's feature ids per source-layer, for the layers asked — or null when the tile does not carry 5.24's
 * feature index (another GL version, or a tile not loaded yet), so the caller falls back to the public route.
 */
export function tileIds(tile: unknown, layers: readonly string[]): Record<string, number[]> | null {
  const index = (tile as GLTile | null)?.latestFeatureIndex;
  if (!index || typeof index.loadVTLayers !== "function") return null;
  let vt: Record<string, VTLayer | undefined>;
  try { vt = index.loadVTLayers(); } catch { return null; }
  if (!vt || typeof vt !== "object") return null;
  const out: Record<string, number[]> = {};
  for (const name of layers) {
    const l = vt[name];
    if (!l || typeof l.feature !== "function") continue;
    const ids: number[] = [];
    for (let i = 0; i < l.length; i++) {
      const id = l.feature(i).id;
      if (typeof id === "number") ids.push(id);
    }
    out[name] = ids;
  }
  return out;
}

type GLMap = { style?: { tileManagers?: Record<string, GLManager>; sourceCaches?: Record<string, GLManager> } };
type GLManager = {
  _inViewTiles?: { getAllTiles?: () => unknown[] };
  _outOfViewCache?: { data?: Record<string, { value?: unknown }[]> };
};

/**
 * Every tile a source holds — in view AND in the out-of-view cache — or null on a build that does not look like 5.24.
 * The cache matters: a cached tile panned back into view fires no load event, so a tile loaded before an index arrived
 * would stay grey for good if only the visible ones were labelled.
 */
export function sourceTiles(map: unknown, sourceId: string): unknown[] | null {
  const style = (map as GLMap | null)?.style;
  const tm = style?.tileManagers?.[sourceId] ?? style?.sourceCaches?.[sourceId];
  const inView = tm?._inViewTiles?.getAllTiles;
  if (!tm || typeof inView !== "function") return null;
  const out = [...inView.call(tm._inViewTiles)];
  for (const list of Object.values(tm._outOfViewCache?.data ?? {}))
    for (const e of list ?? []) if (e?.value) out.push(e.value);
  return out.filter((t) => (t as GLTile).state === "loaded" || (t as GLTile).state === "reloading"
                           || (t as GLTile).state === "expired");
}
