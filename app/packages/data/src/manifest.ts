/**
 * THE MANIFEST (P10, MINIMAL-PACKAGING §5–§6): read it, work out what to download, and decide whether a data release may
 * be paired with the tiles held.
 *
 *   loadManifest   refuses any format but app-manifest/2.
 *   diff           per file by sha256: a data-only release lists only the changed files; the tiles are listed only
 *                  when `tiles.current.id` moves, the basemap only when its id moves, the map assets only when theirs do.
 *   pair           data pairs with tiles when `tiles.compatible` holds the tile version's sid space ("<handles16>.*").
 *                  On a mismatch, or a manifest that cannot be read, the LAST GOOD PAIR is kept and said so; tiles are
 *                  never coloured from an incompatible file.
 */
import { MANIFEST_FORMAT, type Manifest } from "./pack.generated";

export type { Manifest } from "./pack.generated";
export type FileName = string;
/** `<handles16>.<tilebuild12>` (pipeline/deliver/tiles/version.py). */
export type TileVersion = string;

export interface Held {
  tiles: TileVersion | null;
  basemap: string | null;
  assets?: string | null;
  /** The release the files came from, and each file's sha256. */
  release?: string | null;
  files: Readonly<Record<FileName, string>>;
}

export class ManifestError extends Error {}

export function parseManifest(json: unknown): Manifest {
  const m = json as Partial<Manifest> | null;
  if (!m || typeof m !== "object" || m.format !== MANIFEST_FORMAT)
    throw new ManifestError(`manifest: format ${JSON.stringify(m?.format)} is not ${MANIFEST_FORMAT}`);
  if (!m.files || !m.tiles || !Array.isArray(m.tiles.compatible) || typeof m.data_release !== "string")
    throw new ManifestError("manifest: missing files, tiles.compatible or data_release");
  return m as Manifest;
}

export async function loadManifest(url: string, signal?: AbortSignal,
                                   fetchImpl: typeof fetch = fetch): Promise<Manifest> {
  const res = await fetchImpl(url, { signal });
  if (!res.ok) throw new ManifestError(`manifest: HTTP ${res.status} for ${url}`);
  return parseManifest(await res.json());
}

export function diff(held: Held, m: Manifest): { files: FileName[]; tiles: boolean; basemap: boolean; assets: boolean } {
  const files = Object.keys(m.files).filter((k) => held.files[k] !== m.files[k]!.sha256).sort();
  const tileId = m.tiles.current?.id ?? null;
  return {
    files,
    tiles: tileId !== null && tileId !== held.tiles,
    basemap: (m.basemap?.id ?? null) !== null && m.basemap!.id !== held.basemap,
    assets: (m.tiles.assets?.id ?? null) !== null && m.tiles.assets!.id !== (held.assets ?? null),
  };
}

/** Does a tile version satisfy a `tiles.compatible` pattern ("<handles16>.*" or an exact id)? */
export function compatible(patterns: readonly string[], tiles: TileVersion): boolean {
  return patterns.some((p) => (p.endsWith(".*") ? tiles.startsWith(p.slice(0, -1)) : tiles === p));
}

export type Pairing =
  | { ok: true; tiles: TileVersion; release: string }
  | { ok: false; reason: "tiles-incompatible" | "format" | "offline"; keep: { tiles: TileVersion; release: string } | null };

/**
 * Pair the manifest's data with tiles: the manifest's current tiles when it names them, else the tiles held. `m` null is
 * "the manifest could not be read" (offline, or a format this app does not know: pass the reason).
 */
export function pair(m: Manifest | null, held: Held, why: "format" | "offline" = "offline"): Pairing {
  const keep = held.tiles && held.release ? { tiles: held.tiles, release: held.release } : null;
  if (m === null) return { ok: false, reason: why, keep };
  const tiles = m.tiles.current?.id ?? held.tiles;
  if (tiles && compatible(m.tiles.compatible, tiles)) return { ok: true, tiles, release: m.data_release };
  return { ok: false, reason: "tiles-incompatible", keep };
}
