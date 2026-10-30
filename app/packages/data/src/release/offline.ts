/**
 * (T238) THE OFFLINE MAPS PAGE'S DATA. Owner 2026-10-29: REGULATIONS ARE NEVER PER REGION — every data file (regs-<r>,
 * salmon, the answers, core, water …) is the province's, always kept; only the MAP is a choice by region.
 *
 *   REGULATIONS   every file the manifest names (`files`): one figure for all of B.C. A phone installs them all
 *                 (install.ts, pinned by install.regs.test.ts) whatever map is kept; the web's background prefetch (T253)
 *                 walks every region's over time. Nothing here can drop one.
 *   MAP           tiles (+ their mask) and the basemap — today ONE file each for the whole province (no per-region tile
 *                 split yet: T85/T202), so it is one row, never a region's.
 *   WEB           what the release cache holds of the regulations (`ReleaseLoader.stored`), read when the page opens.
 */
import type { Manifest } from "../manifest";
import type { FileName, ReleaseLoader } from "./loader";

export interface OfflineSizes {
  /** every data file the release names: the regulations and answers for all of B.C. */
  readonly regs: number;
  /** the map: tiles (+ their mask) and the basemap, one file each for the whole province */
  readonly map: { readonly tiles: number; readonly basemap: number };
}

/** What a manifest says each part costs (bytes as kept on the device: raw, as `install` writes and the cache holds). */
export function offlineSizes(m: Manifest): OfflineSizes {
  return {
    // (T256) not the answer slices: a subset of core / water for the web, never kept beside them on a phone
    regs: Object.values(m.files).reduce((n, f) => n + (f?.slice_of ? 0 : f?.bytes ?? 0), 0),
    map: { tiles: (m.tiles.current?.bytes ?? 0) + (m.tiles.mask?.bytes ?? 0), basemap: m.basemap?.bytes ?? 0 },
  };
}

/**
 * (web) How much of the regulations this browser keeps: the release cache asked file by file. null when the loader
 * cannot say (a phone holds its whole installed set; a test loader).
 */
export async function regsKept(loader: ReleaseLoader): Promise<number | null> {
  if (!loader.stored) return null;
  const m = loader.manifest;
  let held = 0;
  for (const f of Object.keys(m.files).filter((k) => !m.files[k]!.slice_of) as FileName[]) {
    const a = await loader.stored(f).catch(() => false);
    if (a === null) return null;
    if (a) held += m.files[f]?.bytes ?? 0;
  }
  return held;
}
