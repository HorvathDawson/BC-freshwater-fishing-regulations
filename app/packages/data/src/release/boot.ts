/**
 * START-UP (P11): manifest → (native: install / update) → paint → ready. What App.tsx runs before it draws the Shell,
 * platform-free: the platform half (`@app/data/release/platform`) is passed in, so a test runs the same function the
 * app does over fakes.
 *
 *   web      manifest (network) → the loader over Cache Storage + sql.js → paint decoded. Nothing else is fetched
 *            before the first coloured map; every other file waits for the first question that needs it.
 *   native   the HELD set first (`release/current`): a launch needs no network. First run: the manifest is required
 *            and `install` downloads every file (progress). Every launch with a network: the manifest (5 s) is diffed
 *            against the held set and only changed files are downloaded, in the background, into the next release's
 *            folder; the pointer swaps when they are all verified and the swap is USED at the next launch, when the
 *            set it replaced is pruned — never before the new one has opened and decoded its paint.
 */
import type { Paint } from "@app/core";
import { loadManifest, type Held, type Manifest } from "../manifest";
import type { RegsSource } from "../index";
import type { BundleSourceOptions } from "../bundle/source";
import { heldFrom, heldManifest, heldPath, install, prune, type Progress, type ReleaseFs } from "./install";
import { releaseLoader, type DbName, type Opener, type ReleaseLoader, type ReleaseSource } from "./loader";
import { makeReleaseSource } from "./source";

/** What `@app/data/release/platform` exports (structurally; a test passes fakes). */
export interface ReleasePlatform {
  releaseOpener(at: { wasmBase: string; installed: (name: DbName) => { dir: string; file: string } | null }): Opener;
  releaseCache(name?: string): Promise<{ readonly kind: string; get(url: string): Promise<Uint8Array | null>;
                                         put(url: string, bytes: Uint8Array): Promise<void> }>;
  releaseSource(m: Manifest, at: { base: string; cache: Awaited<ReturnType<ReleasePlatform["releaseCache"]>>;
                                   held: Held | null; fetch?: typeof fetch }): ReleaseSource;
  device(fetchImpl?: typeof fetch): {
    fs: ReleaseFs;
    download: (url: string, expect: { bytes: number; sha256: string }) => Promise<Uint8Array>;
    toDisk: (url: string, path: string, expect: { bytes: number; sha256: string },
             onBytes?: (n: number) => void) => Promise<void>;
    dirUri(path: string): string;
    fileUri(path: string): string;
  } | null;
}

export type BootStage = "manifest" | "install" | "paint";

export interface BootOptions {
  /** `ENDPOINTS.manifest`. */
  manifestUrl: string;
  /** `ENDPOINTS.release`: every manifest url is relative to it. */
  releaseBase: string;
  /** Where sql.js finds its wasm (web). */
  wasmBase: string;
  platform: ReleasePlatform;
  feed?: BundleSourceOptions["feed"];
  fetch?: typeof fetch;
  onStage?: (s: BootStage) => void;
  onProgress?: (p: Progress) => void;
  /** How long a launch waits for the manifest before going on with the held set (native). */
  manifestTimeoutMs?: number;
}

export interface Booted {
  manifest: Manifest;
  /** The installed set (native) or what the web holds (the manifest's tiles, nothing on disk). */
  held: Held;
  loader: ReleaseLoader;
  paint: Paint;
  source: RegsSource;
  /** Where the map reads a release-relative path from: a local file on native, the release root on the web. */
  locate(path: string): string;
  /** Native: the background update, resolving to the set installed for the NEXT launch (null: nothing to do). */
  update: Promise<Held | null>;
}

export class BootError extends Error {
  constructor(message: string, readonly firstRunOffline = false) { super(message); }
}

const PREVIOUS = "release/previous";

function timeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((ok, fail) => {
    const t = setTimeout(() => fail(new Error(`manifest: no answer in ${ms} ms`)), ms);
    p.then((v) => { clearTimeout(t); ok(v); }, (e: unknown) => { clearTimeout(t); fail(e); });
  });
}

export async function bootRelease(o: BootOptions): Promise<Booted> {
  const f = o.fetch ?? fetch;
  const dev = o.platform.device(f);
  o.onStage?.("manifest");

  if (!dev) {
    // THE WEB: online by definition; the manifest is required, and only paint is fetched before the map
    const m = await loadManifest(o.manifestUrl, undefined, f);
    const cache = await o.platform.releaseCache();
    const loader = releaseLoader(m, o.platform.releaseSource(m, { base: o.releaseBase, cache, held: null, fetch: f }),
                                 o.platform.releaseOpener({ wasmBase: o.wasmBase, installed: () => null }));
    o.onStage?.("paint");
    const paint = await loader.paint();
    const held: Held = { tiles: m.tiles.current?.id ?? null, basemap: m.basemap?.id ?? null,
                         assets: m.tiles.assets?.id ?? null, release: m.data_release, files: {} };
    return { manifest: m, held, loader, paint, source: makeReleaseSource(loader, { feed: o.feed }),
             locate: (path) => `${o.releaseBase}${path}`, update: Promise.resolve(null) };
  }

  // THE PHONE: the held set first; the network only to install it once and to look for a newer one
  let held = await heldFrom(dev.fs);
  const latest = await timeout(loadManifest(o.manifestUrl, undefined, f), o.manifestTimeoutMs ?? 5000)
    .catch(() => null);
  if (!held) {
    if (!latest) throw new BootError("first run: the manifest could not be read", true);
    o.onStage?.("install");
    held = await install(latest, dev.fs, dev.download, o.releaseBase, null, o.onProgress, dev.toDisk);
  }
  const m = await heldManifest(dev.fs, held);
  const dir = `release/${held.release}`;
  const loader = releaseLoader(m,
    o.platform.releaseSource(m, { base: o.releaseBase, cache: await o.platform.releaseCache(), held, fetch: f }),
    o.platform.releaseOpener({
      wasmBase: o.wasmBase,
      installed: (name) => {
        const spec = m.files[name];
        return spec ? { dir: dev.dirUri(dir), file: heldPath(held!, spec.url).split("/").pop()! } : null;
      },
    }));
  o.onStage?.("paint");
  const paint = await loader.paint();

  // the set this one replaced is deleted only now: the new one has opened and its paint decoded against its handles
  if (await dev.fs.exists(PREVIOUS)) {
    const prev = await heldFrom({ ...dev.fs, exists: async () => true, read: () => dev.fs.read(PREVIOUS) });
    if (prev) await prune(dev.fs, prev, held);
    await dev.fs.remove(PREVIOUS);
  }

  const now = held;
  const update = latest && latest.data_release !== now.release
    ? install(latest, dev.fs, dev.download, o.releaseBase, now, () => {}, dev.toDisk).then(async (next) => {
        await dev.fs.write(PREVIOUS, new TextEncoder().encode(JSON.stringify(now)));
        return next;
      })
    : Promise.resolve(null);
  update.catch(() => {});        // a failed update leaves the held set in force (install never swapped)

  return { manifest: m, held: now, loader, paint, source: makeReleaseSource(loader, { feed: o.feed }),
           locate: (path) => dev.fileUri(`release/${path}`), update };
}

/**
 * The map's three files from the release when the manifest names them, else the development endpoints (`ENDPOINTS.tiles`)
 * — until the human-run tile rebuild writes a tile version folder (P18), the release names no tiles and no mask.
 */
export function releaseTiles(b: Pick<Booted, "manifest" | "locate">,
                             dev: { atlas: string; basemap: string; outside: string }):
    { atlas: string; basemap: string; outside: string } {
  const t = b.manifest.tiles;
  return {
    atlas: t.current ? b.locate(t.current.url) : dev.atlas,
    basemap: b.manifest.basemap ? b.locate(b.manifest.basemap.url) : dev.basemap,
    outside: t.mask ? b.locate(t.mask.url) : dev.outside,
  };
}
