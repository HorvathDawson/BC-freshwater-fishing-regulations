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
import { PAINT_READS } from "@app/core";
import { MANIFEST_FORMAT, PACK_FILES, PACK_OPTIONAL, PACK_WEB_ONLY, type Manifest } from "./pack.generated";

export type { Manifest, SynopsisAppend } from "./pack.generated";
export type FileName = string;
/** `<handles16>.<tilebuild12>` (pipeline/deliver/tiles/version.py). */
export type TileVersion = string;

export interface Held {
  tiles: TileVersion | null;
  basemap: string | null;
  assets?: string | null;
  /** (owner 2026-10-29) the sha256 of the synopsis PDF installed with the set (native): replaced only when it moves. */
  synopsis?: string | null;
  /** The release the files came from, and each file's sha256. */
  release?: string | null;
  files: Readonly<Record<FileName, string>>;
  /** Native: where the manifest this set was installed from is kept (one per set: a tile-only release shares the data
   *  folder, so the set it replaced keeps its own manifest until it is pruned). */
  manifest?: string;
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

/** (T403) Formats a reader takes beside the contract's own (an older one it still decodes). */
const ALSO_READS: Readonly<Record<string, readonly string[]>> = { paint: PAINT_READS };

type Readable = { ok: true } | { ok: false; reason: string };

/**
 * (T403) CAN THIS APP READ THIS RELEASE? Every file the manifest names that this app's contract knows must be in a format
 * its reader takes (`PACK_FILES`, and `ALSO_READS`); and a PUBLISHED release (stamped `published`: a fixture or a local
 * build is a partial set) must name every file of the contract but the optional ones — and, on a phone (`where`), the
 * web's own. A release made for an older app fails here (measured 2026-11-05: a phone updated to 0.5.0 opened the
 * r-4e709418ebd8 it held from 0.4.0 — salmon/3, no `lines` — and its readers threw on the map and the regs page).
 * Files the contract does not know are a newer release's and are not read: they pass.
 */
export function readsRelease(m: Manifest, where: "web" | "phone"): Readable {
  const wrong: string[] = [];
  for (const [name, f] of Object.entries(m.files)) {
    const want = PACK_FILES[name]?.format;
    if (want && f.format !== want && !(ALSO_READS[name] ?? []).includes(f.format)) wrong.push(`${name} is ${f.format}, not ${want}`);
  }
  const missing = m.published
    ? Object.keys(PACK_FILES).filter((n) => !m.files[n] && !PACK_OPTIONAL.includes(n)
                                            && !(where === "phone" && PACK_WEB_ONLY.includes(n))).sort()
    : [];
  if (!wrong.length && !missing.length) return { ok: true };
  return { ok: false, reason: `release ${m.data_release} is not one this app reads: `
    + [...wrong, ...(missing.length ? [`it has no ${missing.join(", ")}`] : [])].join("; ") };
}

/** Whether a PHONE holds this file: not an answer slice (T256, `slice_of`) and not a web-only file (T369, `web_only`:
 *  `lines` — links are web-only). */
export const phoneHolds = (f: { slice_of?: string; web_only?: boolean }): boolean => !f.slice_of && !f.web_only;

/**
 * (T256) The manifest a PHONE installs from: every file but the answer slices (`slice_of`: web only — a phone holds the
 * whole core and water, and the regulations are always all of B.C. on it) and (T369) the web-only files (`web_only`).
 */
export function withoutSlices(m: Manifest): Manifest {
  if (Object.values(m.files).every(phoneHolds)) return m;
  return { ...m, files: Object.fromEntries(Object.entries(m.files).filter(([, f]) => phoneHolds(f))) };
}

export function diff(held: Held, m: Manifest):
  { files: FileName[]; tiles: boolean; basemap: boolean; assets: boolean; synopsis: boolean } {
  const files = Object.keys(m.files).filter((k) => phoneHolds(m.files[k]!) && held.files[k] !== m.files[k]!.sha256).sort();
  const tileId = m.tiles.current?.id ?? null;
  return {
    files,
    tiles: tileId !== null && tileId !== held.tiles,
    basemap: (m.basemap?.id ?? null) !== null && m.basemap!.id !== held.basemap,
    assets: (m.tiles.assets?.id ?? null) !== null && m.tiles.assets!.id !== (held.assets ?? null),
    // (owner 2026-10-29) the rule book rides in the install set: fetched again only when its bytes move
    synopsis: (m.synopsis?.sha256 ?? null) !== null && m.synopsis!.sha256 !== (held.synopsis ?? null),
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
