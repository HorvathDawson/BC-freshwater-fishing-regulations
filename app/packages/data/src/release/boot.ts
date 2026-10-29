/**
 * START-UP (P11): manifest → (native: install / update) → paint → ready. What App.tsx runs before it draws the Shell,
 * platform-free: the platform half (`@app/data/release/platform`) is passed in, so a test runs the same function the
 * app does over fakes.
 *
 *   web      manifest (network) → the loader over Cache Storage + sql.js → paint decoded. Nothing else is fetched
 *            before the first coloured map; every other file waits for the first question that needs it.
 *   native   the HELD set first (`release/current`): a launch needs no network. First run: the manifest is required
 *            and `install` downloads every file (progress). Every launch with a network: the manifest (5 s) is compared
 *            with the held set — its data files, tiles, mask, basemap and map assets — and only what changed is
 *            downloaded, in the background; the pointer swaps when it is all verified and the swap is USED at the next
 *            launch, when the set it replaced is pruned — never before the new one has opened, decoded its paint and
 *            paired. A new set that will not open ROLLS BACK to the set it replaced (still on disk: nothing is pruned
 *            before success), and the reader is told (`Booted.rolledBack`). Only a VERIFIED defect — bytes that fail
 *            their sha, a format or handles mismatch, a decode failure of verified bytes (`provenDefect`) — refuses
 *            it for good: the pointer goes back, the set is deleted and not installed again until the manifest names
 *            another. Anything else (an IO or opener failure) is rolled back for THIS launch only: the pointer, the
 *            set and the set it replaced all stay, and the next launch tries it again.
 */
import { PaintError, StatusIndexError, type Paint } from "@app/core";
import { VerifyError } from "../fetchVerified";
import { compatible, loadManifest, ManifestError, type Held, type Manifest } from "../manifest";
import { sha256Hex } from "../sha256";
import type { WaterSource } from "../index";
import type { SourceOptions } from "./source";
import { heldFrom, heldManifest, heldPath, install, pointerFor, POINTER as POINTER_PATH, prune, readHeld, sameSet,
         writeHeld, type Progress, type ReleaseFs } from "./install";
import { rawFetch } from "./assets";
import { withLiveSalmon, type SalmonFeedSource } from "../feed/salmon";
import { releaseLoader, ReleaseError, type DbName, type Opener, type ReleaseLoader,
         type ReleaseSource } from "./loader";
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
  feed?: SourceOptions["feed"];
  /** (T211) the live DFO salmon feed (last capture kept): laid over the release's salmon file (`withLiveSalmon`) */
  salmonFeed?: SalmonFeedSource;
  fetch?: typeof fetch;
  onStage?: (s: BootStage) => void;
  onProgress?: (p: Progress) => void;
  /** How long a launch waits for the manifest before going on with the held set (native). */
  manifestTimeoutMs?: number;
  /**
   * (T231) Leave the background update to the update service (`Booted.stage`, called by `@app/data` `createUpdateService`
   * when its policy allows: Wi-Fi for a large set): the launch starts no install of its own. Absent/false: the launch
   * installs a newer set in the background, as before.
   */
  deferUpdate?: boolean;
}

export interface Booted {
  manifest: Manifest;
  /** The installed set (native) or what the web holds (the manifest's tiles, nothing on disk). */
  held: Held;
  loader: ReleaseLoader;
  paint: Paint;
  source: WaterSource;
  /** Where the map reads a release-relative path from: a local file on native, the release root on the web. */
  locate(path: string): string;
  /** Native: the background update, resolving to the set installed for the NEXT launch (null: nothing to do). */
  update: Promise<Held | null>;
  /** Native: the new set would not open, so this launch went back to the one it replaced (null: it did not). */
  rolledBack: RolledBack | null;
  /**
   * (T231) Fetch what a newer manifest needs BESIDE the set in use, verified, and never under it: on a phone the whole
   * set is installed and the pointer swapped (`install`; the set in use stays on disk and open until the next boot, which
   * opens the new one and prunes the old); on the web the changed files this session has opened are fetched into the
   * release cache (immutable urls), so the boot that applies it reads them from there. Resolves when the next boot can
   * use it (null: nothing to fetch — the same set, one refused, or one already installed and waiting to be applied).
   */
  stage(m: Manifest, onProgress?: (p: Progress) => void): Promise<Held | null>;
  /** (T231) The manifest this launch read from the server (the web's is `manifest`), or null (offline): the update
   *  service's first check (`seedRelease`), so a launch asks for the manifest once. */
  latest: Manifest | null;
}

/**
 * Which set was refused, which one is used instead, and why (the error the new set opened with). `retry`: the failure
 * proved nothing wrong with the set (IO, the opener), so it stays the pointer's and the next launch tries it again;
 * false: a verified defect, refused for good.
 */
export interface RolledBack { refused: string | null; using: string | null; reason: string; retry: boolean }

export class BootError extends Error {
  constructor(message: string, readonly firstRunOffline = false) { super(message); }
}

/** The set an update replaced, kept until the new one has opened and paired (written just before the swap). */
const PREVIOUS = "release/previous";
/** A set that would not open and was rolled back: not installed again until the manifest names another. */
const REFUSED = "release/refused";

function timeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((ok, fail) => {
    const t = setTimeout(() => fail(new Error(`manifest: no answer in ${ms} ms`)), ms);
    p.then((v) => { clearTimeout(t); ok(v); }, (e: unknown) => { clearTimeout(t); fail(e); });
  });
}

/**
 * Is `e`, the error a held set `h` opened with, a VERIFIED defect of the set (refuse it for good) rather than a failure
 * of this launch (IO, the opener: try it again next launch)? The checkers' own errors are (a sha, a format or handles
 * mismatch, a decode failure of verified bytes); any other error is proven only when the set's bytes on disk no longer
 * are the ones installed (a file gone, or not its sha) or its manifest no longer reads. A read that fails proves nothing.
 */
export async function provenDefect(e: unknown, fs: ReleaseFs, h: Held): Promise<boolean> {
  if (e instanceof VerifyError || e instanceof ManifestError || e instanceof ReleaseError || e instanceof PaintError
      || e instanceof StatusIndexError) return true;
  try {
    const m = await heldManifest(fs, h).catch((err: unknown) => {
      if (err instanceof SyntaxError || err instanceof ManifestError) throw new VerifyError("manifest unreadable");
      throw err;
    });
    for (const [name, sha] of Object.entries(h.files)) {
      const spec = m.files[name];
      if (!spec) return true;
      const path = heldPath(h, spec.url);
      if (!(await fs.exists(path))) return true;
      if (sha256Hex(await fs.read(path)) !== sha) return true;
    }
    return false;
  } catch (err) {
    return err instanceof VerifyError;
  }
}

export async function bootRelease(o: BootOptions): Promise<Booted> {
  const f = o.fetch ?? fetch;
  const dev = o.platform.device(f);
  o.onStage?.("manifest");

  if (!dev) {
    // THE WEB: online by definition; the manifest is required, and only paint is fetched before the map
    const m = await loadManifest(o.manifestUrl, undefined, f);
    const cache = await o.platform.releaseCache();
    const loader = withLiveSalmon(
      releaseLoader(m, o.platform.releaseSource(m, { base: o.releaseBase, cache, held: null, fetch: f }),
                    o.platform.releaseOpener({ wasmBase: o.wasmBase, installed: () => null })), o.salmonFeed);
    o.onStage?.("paint");
    const paint = await loader.paint();
    const held: Held = { tiles: m.tiles.current?.id ?? null, basemap: m.basemap?.id ?? null,
                         assets: m.tiles.assets?.id ?? null, release: m.data_release, files: {} };
    const stage = async (next: Manifest, onProgress?: (p: Progress) => void): Promise<Held | null> => {
      if (sameSet(pointerFor(next), pointerFor(m))) return null;
      // the files this session has opened that the new release changed: warmed in the release cache, verified
      const names = loader.opened().filter((n) => next.files[n] && next.files[n]!.sha256 !== m.files[n]?.sha256);
      const ahead = releaseLoader(next, o.platform.releaseSource(next, { base: o.releaseBase, cache, held: null, fetch: f }),
                                  o.platform.releaseOpener({ wasmBase: o.wasmBase, installed: () => null }));
      const of = names.reduce((n, k) => n + next.files[k]!.bytes, 0);
      let done = 0, bytes = 0;
      for (const n of names) {
        await ahead.prefetch(n);
        bytes += next.files[n]!.bytes;
        onProgress?.({ done: ++done, total: names.length, file: n, bytes, of });
      }
      return pointerFor(next);
    };
    return { manifest: m, held, loader, paint, source: makeReleaseSource(loader, { feed: o.feed }),
             locate: (path) => `${o.releaseBase}${path}`, update: Promise.resolve(null), rolledBack: null, stage,
             latest: m };
  }

  // THE PHONE: the held set first; the network only to install it once and to look for a newer one
  let held = await heldFrom(dev.fs);
  const latest = await timeout(loadManifest(o.manifestUrl, undefined, f), o.manifestTimeoutMs ?? 5000)
    .catch(() => null);
  if (!held) {
    if (!latest) throw new BootError("first run: the manifest could not be read", true);
    o.onStage?.("install");
    held = await install(latest, dev.fs, dev.download, o.releaseBase, null, o.onProgress, dev.toDisk, undefined,
                         rawFetch(f));
  }
  /** Open a held set: its manifest, paint decoded against its handles, core opened and checked, its tiles paired. */
  const cache = await o.platform.releaseCache();
  const openSet = async (h: Held) => {
    const m = await heldManifest(dev.fs, h);
    const t = m.tiles.current;
    if (t && !(t.id && compatible(m.tiles.compatible, t.id)))
      throw new ReleaseError(`release ${m.data_release}: tiles ${t.id ?? "(no id)"} are not in ${m.tiles.compatible}`);
    const dir = `release/${h.release}`;
    const loader = releaseLoader(m,
      o.platform.releaseSource(m, { base: o.releaseBase, cache, held: h, fetch: f }),
      o.platform.releaseOpener({
        wasmBase: o.wasmBase,
        installed: (name) => {
          const spec = m.files[name];
          return spec ? { dir: dev.dirUri(dir), file: heldPath(h, spec.url).split("/").pop()! } : null;
        },
      }));
    o.onStage?.("paint");
    const paint = await loader.paint();
    await loader.db("core");                      // opened in place on a phone: the first file any question needs
    return { m, loader, paint };
  };

  let set: Awaited<ReturnType<typeof openSet>>;
  let rolledBack: RolledBack | null = null;
  /** A set rolled back for this launch only (not proven defective): kept, and tried again at the next launch. */
  let retrying: { h: Held; m: Manifest | null } | null = null;
  const previous = await readHeld(dev.fs, PREVIOUS);
  try {
    set = await openSet(held);
  } catch (e) {
    // ROLLBACK: the set this one replaced is still on disk (nothing is pruned before success); open it instead
    if (!previous || sameSet(previous, held)) throw e;
    const bad = held, badM = await heldManifest(dev.fs, bad).catch(() => null);
    const defect = await provenDefect(e, dev.fs, bad);
    if (defect) {
      await writeHeld(dev.fs, REFUSED, bad);
      await writeHeld(dev.fs, POINTER_PATH, previous);
    }
    held = previous;
    set = await openSet(held);
    rolledBack = { refused: bad.release ?? null, using: held.release ?? null,
                   reason: e instanceof Error ? e.message : String(e), retry: !defect };
    if (defect) {
      await prune(dev.fs, bad, badM, held, set.m);
      await dev.fs.remove(PREVIOUS);
    } else {
      retrying = { h: bad, m: badM };
    }
  }

  // the set this one replaced is deleted only now: the new one has opened, decoded its paint and paired
  if (!rolledBack && previous) {
    if (!sameSet(previous, held))
      await prune(dev.fs, previous, await heldManifest(dev.fs, previous).catch(() => null), held, set.m);
    await dev.fs.remove(PREVIOUS);
  }

  const now = held;
  const refused = await readHeld(dev.fs, REFUSED);
  /** (T231) The set installed for the next boot by this session (one at a time: a newer one waits for that boot). */
  let staged: Held | null = null;
  let staging: Promise<Held | null> | null = null;
  // an update whenever ANY part of the set moved — data files, tiles, mask, basemap, assets — unless it was refused, or
  // is the set this launch rolled back from and will retry (already installed, still the pointer's)
  const stage = (next: Manifest, onProgress: (p: Progress) => void = () => {}): Promise<Held | null> => {
    if (staging) return staging;
    const target = pointerFor(next);
    if (staged || sameSet(target, now) || (refused && sameSet(target, refused))
        || (retrying && sameSet(target, retrying.h))) return Promise.resolve(null);
    staging = (async () => {
      // a newer set replaces the one being retried: what of it the held set does not name goes first
      if (retrying) { await prune(dev.fs, retrying.h, retrying.m, now, set.m); retrying = null; }
      const h = await install(next, dev.fs, dev.download, o.releaseBase, now, onProgress, dev.toDisk,
                              () => writeHeld(dev.fs, PREVIOUS, now), rawFetch(f));
      staged = h;
      return h;
    })();
    staging.then(() => { staging = null; }, () => { staging = null; });
    return staging;
  };
  const update = latest && !o.deferUpdate ? stage(latest) : Promise.resolve(null);
  update.catch(() => {});        // a failed update leaves the held set in force (install never swapped)

  const loader = withLiveSalmon(set.loader, o.salmonFeed);
  return { manifest: set.m, held: now, loader, paint: set.paint,
           source: makeReleaseSource(loader, { feed: o.feed }),
           locate: (path) => dev.fileUri(`release/${path}`), update, rolledBack, stage, latest };
}

/**
 * The map's three files from the release when the manifest names them, else the development endpoints (`ENDPOINTS.tiles`)
 * — until the human-run tile rebuild writes a tile version folder (P18), the release names no tiles and no mask.
 * N2: and the map ASSETS folder (glyphs + sprites) when the manifest names one, so labels come from the release — the
 * installed `file://` folder on a phone — and not from a CDN. Absent, the map keeps its own default.
 */
export function releaseTiles(b: Pick<Booted, "manifest" | "locate">,
                             dev: { atlas: string; basemap: string; outside: string }):
    { atlas: string; basemap: string; outside: string; assets?: string } {
  const t = b.manifest.tiles;
  const assets = t.assets ? b.locate(t.assets.url).replace(/\/?$/, "/") : null;
  return {
    atlas: t.current ? b.locate(t.current.url) : dev.atlas,
    basemap: b.manifest.basemap ? b.locate(b.manifest.basemap.url) : dev.basemap,
    outside: t.mask ? b.locate(t.mask.url) : dev.outside,
    // the glyphs and sprites: the installed folder on a phone, the release url on the web (the style's CDN otherwise)
    ...(assets ? { assets } : {}),
  };
}
