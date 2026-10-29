/**
 * (T238) THE OFFLINE MAPS PAGE'S DATA: what each fishing region costs to keep, what this device keeps of it, and — on the
 * web — saving a region ahead (with progress, and resume after a failure) or removing it.
 *
 *   WHAT IS A REGION'S   its own slices: `regs-<r>` and `salmon-<r>` (the files the release splits by region). Every other
 *                        data file (core, water, paint, regs-sid, …) is the province's and shared: saved with the first
 *                        region (or on their own), kept while any region is, dropped by "Remove everything" or with
 *                        the last region removed. A region reads Saved when its own files are kept. The map's tiles and
 *                        basemap are ONE file each for the whole province (no per-region tile split yet: T85/T202), so
 *                        they are shown as province-wide and never as a region's.
 *   PHONE                installs the whole release (install.ts), so every region is on the phone and nothing is
 *                        offered per region; sizes come from the held set's manifest (`offlineSizes`).
 *   WEB                  the release cache (Cache Storage by immutable url) is the store: `ReleaseLoader.stored` asks it,
 *                        `prefetch` fills it (THE SAME verified path the background prefetch and a tap take — nothing
 *                        here fetches on its own), `drop` empties it. A save is one file at a time; a file that fails
 *                        stops the save with what was saved kept, so "Resume" fetches only the rest.
 */
import type { UpdateEvent } from "@app/core";
import { REGIONS, type Region } from "../pack.generated";
import type { Manifest } from "../manifest";
import type { FileName, ReleaseLoader } from "./loader";

/** A region's own files, as this manifest names them. */
export function regionFiles(m: Pick<Manifest, "files">, r: Region): FileName[] {
  return ([`regs-${r}`, `salmon-${r}`] as FileName[]).filter((f) => !!m.files[f]);
}

/** The province's data files: every file the manifest names that is no region's own. */
export function sharedFiles(m: Pick<Manifest, "files">): FileName[] {
  const own = new Set(REGIONS.flatMap((r) => regionFiles(m, r)));
  return (Object.keys(m.files) as FileName[]).filter((f) => !own.has(f)).sort();
}

const sizeOf = (m: Pick<Manifest, "files">, fs: readonly FileName[]) => fs.reduce((n, f) => n + (m.files[f]?.bytes ?? 0), 0);

export interface OfflineSizes {
  /** each region the release holds a regs file for: its own files' bytes */
  readonly regions: readonly { readonly region: Region; readonly bytes: number }[];
  /** the province's data files */
  readonly shared: number;
  /** the map: tiles (+ their mask) and the basemap, one file each for the whole province */
  readonly map: { readonly tiles: number; readonly basemap: number };
}

/** What a manifest says each part costs (bytes as kept on the device: raw, as `install` writes and the cache holds). */
export function offlineSizes(m: Manifest): OfflineSizes {
  return {
    regions: REGIONS.filter((r) => !!m.files[`regs-${r}`]).map((region) => ({ region, bytes: sizeOf(m, regionFiles(m, region)) })),
    shared: sizeOf(m, sharedFiles(m)),
    map: { tiles: (m.tiles.current?.bytes ?? 0) + (m.tiles.mask?.bytes ?? 0), basemap: m.basemap?.bytes ?? 0 },
  };
}

export type RegionKept = "kept" | "partial" | "none";

export interface OfflineJob {
  /** a region, or "shared": the province-wide files alone */
  readonly region: Region | "shared";
  /** one job at a time: a save (with progress) or a remove */
  readonly kind: "save" | "remove";
  readonly phase: "running" | "failed";
  readonly bytes: number;
  readonly of: number;
  readonly error: string | null;
}

export interface OfflineSnapshot {
  /** the first scan of the cache has answered */
  readonly scanned: boolean;
  /** the loader can say what it keeps (false: a phone, or a test loader — nothing per region is offered) */
  readonly storable: boolean;
  readonly regions: readonly { readonly region: Region; readonly bytes: number; readonly held: number;
                               readonly state: RegionKept }[];
  readonly shared: { readonly bytes: number; readonly held: number };
  readonly map: OfflineSizes["map"];
  readonly job: OfflineJob | null;
}

export interface OfflineStore {
  /** Ask the cache again what it keeps (the page opening; after the background prefetch moved). */
  scan(): Promise<void>;
  /** Save a region (its own files and whatever shared file is not kept yet), or "shared" (those alone). Resolves when
   *  done or failed. */
  save(r: Region | "shared"): Promise<void>;
  /** Drop a region's own files; the shared files go with the last kept region. */
  remove(r: Region): Promise<void>;
  /** Drop every file of the release this device keeps. */
  clear(): Promise<void>;
  snapshot(): OfflineSnapshot;
  subscribe(on: () => void): () => void;
}

export function createOfflineStore(loader: ReleaseLoader, o: { onEvent?: (e: UpdateEvent) => void; now?: () => number } = {}): OfflineStore {
  const m = loader.manifest;
  const sizes = offlineSizes(m);
  const shared = sharedFiles(m);
  const now = o.now ?? (() => Date.now());
  const kept = new Set<FileName>();
  let scanned = false, storable = !!loader.stored, job: OfflineJob | null = null;
  const listeners = new Set<() => void>();
  let snap: OfflineSnapshot | null = null;
  const changed = () => { snap = null; for (const l of [...listeners]) l(); };
  const say = (e: Omit<UpdateEvent, "at" | "source">) => {
    try { o.onEvent?.({ at: now(), source: "prefetch", ...e }); } catch { /* the log only */ }
  };

  const scan = async () => {
    if (!loader.stored) { scanned = true; storable = false; changed(); return; }
    const all = Object.keys(m.files) as FileName[];
    const answers = await Promise.all(all.map(async (f) => [f, await loader.stored!(f).catch(() => false)] as const));
    if (answers.some(([, a]) => a === null)) storable = false;
    kept.clear();
    for (const [f, a] of answers) if (a) kept.add(f);
    scanned = true;
    changed();
  };

  const held = (fs: readonly FileName[]) => fs.reduce((n, f) => n + (kept.has(f) ? m.files[f]?.bytes ?? 0 : 0), 0);
  const keptRegions = () => sizes.regions.filter((x) => regionFiles(m, x.region).some((f) => kept.has(f))).map((x) => x.region);

  const drop = async (fs: readonly FileName[]) => {
    for (const f of fs) { await loader.drop?.(f); kept.delete(f); }
  };

  return {
    scan,
    async save(r) {
      if (job?.phase === "running" || !storable) return;
      const want = [...(r === "shared" ? [] : regionFiles(m, r)), ...shared].filter((f) => !kept.has(f));
      const of = want.reduce((n, f) => n + (m.files[f]?.bytes ?? 0), 0);
      let bytes = 0;
      job = { region: r, kind: "save", phase: "running", bytes, of, error: null };
      changed();
      for (const f of want) {
        try {
          await loader.prefetch(f);
        } catch (e) {
          const error = e instanceof Error ? e.message : String(e);
          job = { region: r, kind: "save", phase: "failed", bytes, of, error };
          say({ kind: "failed", note: f, error, errorClass: null });
          changed();
          return;
        }
        // kept only once the cache says so (a browser with no Cache Storage holds it for the session alone)
        if (await loader.stored!(f).catch(() => false)) kept.add(f);
        bytes += m.files[f]?.bytes ?? 0;
        job = { region: r, kind: "save", phase: "running", bytes, of, error: null };
        changed();
      }
      job = null;
      say({ kind: "prefetched", note: r === "shared" ? "Province-wide data saved for offline" : `Region ${r.toUpperCase()} saved for offline` });
      changed();
    },
    async remove(r) {
      if (job?.phase === "running") return;
      job = { region: r, kind: "remove", phase: "running", bytes: 0, of: 0, error: null };
      changed();
      await drop(regionFiles(m, r));
      // the province's files stay while any region is kept: they are every region's
      if (!keptRegions().length) await drop(shared.filter((f) => kept.has(f)));
      job = null;
      changed();
    },
    async clear() {
      if (job?.phase === "running") return;
      await drop((Object.keys(m.files) as FileName[]).filter((f) => kept.has(f)));
      job = null;
      changed();
    },
    snapshot() {
      if (!snap) {
        const sharedHeld = held(shared);
        snap = {
          scanned, storable, job, map: sizes.map,
          shared: { bytes: sizes.shared, held: sharedHeld },
          regions: sizes.regions.map(({ region, bytes }) => {
            const h = held(regionFiles(m, region));
            // a region's own files (the province's are their own row: the background prefetch keeps a region's slices
            // and only the shared files an unnamed tap reads, so a region would otherwise never read as saved)
            const state: RegionKept = h >= bytes ? "kept" : h > 0 ? "partial" : "none";
            return { region, bytes, held: h, state };
          }),
        };
      }
      return snap;
    },
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}
