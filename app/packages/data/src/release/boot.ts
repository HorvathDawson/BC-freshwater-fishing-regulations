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
 *            (T403) A held set THIS app cannot read (`readsRelease`: kept from an older app, say 0.4.0's after an update
 *            to 0.5.0) is never opened: the manifest's set is installed over it before the map, like a first run, and
 *            with no manifest the start fails and says so. No set this app cannot read is installed, opened or rolled
 *            back to.
 */
import { LAUNCH_SLICE_MS, PaintError, releaseAllowed, reportFailure, SLICE_MS, spaceLine, StatusIndexError, UPDATE_WORDS, type AppVersion, type Paint,
         now as clockNow } from "@app/core";
import { VerifyError } from "../fetchVerified";
import { fetchResilient, NetError, type Net } from "../net";
import { compatible, loadManifest, ManifestError, parseManifest, readsRelease, type Held, type Manifest, type StageOutcome } from "../manifest";
import { sha256Hex } from "../sha256";
import type { WaterSource } from "../index";
import type { SourceOptions } from "./source";
import { bigFiles, dataFiles, heldFrom, heldManifest, heldPath, install, installBytes, pointerFor, POINTER as POINTER_PATH, prune, readHeld, sameSet,
         synopsisPath, writeHeld, type LookBytes, type Progress, type ReleaseFs } from "./install";
import { rawFetch } from "./assets";
import type { BackgroundNative, BgItem, BgJob } from "./bgDownload";
import { withLiveSalmon, type SalmonFeedSource } from "../feed/salmon";
import { releaseLoader, ReleaseError, type DbName, type Opener, type ReleaseLoader,
         type ReleaseSource } from "./loader";
import { makeReleaseSource } from "./source";
import type { Region } from "../pack.generated";

/** What `@app/data/release/platform` exports (structurally; a test passes fakes). */
export interface ReleasePlatform {
  releaseOpener(at: { wasmBase: string; installed: (name: DbName) => { dir: string; file: string } | null; net?: Net | null }): Opener;
  releaseCache(name?: string): Promise<{ readonly kind: string; get(url: string): Promise<Uint8Array | null>;
                                         put(url: string, bytes: Uint8Array): Promise<void> }>;
  releaseSource(m: Manifest, at: { base: string; cache: Awaited<ReturnType<ReleasePlatform["releaseCache"]>>;
                                   held: Held | null; fetch?: typeof fetch; net?: Net | null }): ReleaseSource;
  device(fetchImpl?: typeof fetch, bg?: BackgroundNative | null): {
    fs: ReleaseFs;
    download: (url: string, expect: { bytes: number; sha256: string }) => Promise<Uint8Array>;
    toDisk: (url: string, path: string, expect: { bytes: number; sha256: string },
             onBytes?: (n: number) => void) => Promise<void>;
    dirUri(path: string): string;
    fileUri(path: string): string;
    background?: { queue(items: BgItem[], metered: boolean): Promise<void> };
    /** (T738) what the device holds of each big file now: the bar counts them from the first moment (`tally.ts`) */
    look?: LookBytes;
    space?: DeviceSpace;
  } | null;
}

/** (T722 F9) The phone's free bytes (NaN: it would not say) and the bytes a file under the install root holds (0: none). */
export interface DeviceSpace { free(): Promise<number>; size(path: string): Promise<number> }

/** (T298) "wifi": a first install held for Wi-Fi by the reader's mobile-data choice. */
/** (T746) "apply": the set opening is one this device has not opened before (downloaded earlier, put in use now). */
export type BootStage = "manifest" | "wifi" | "install" | "paint" | "apply";

/**
 * (T298) THE FIRST INSTALL'S MOBILE-DATA GATE, the app's (it asks the reader and keeps the answer as the setting).
 */
export interface MobileDataGate {
  /** May a download of `bytes` use mobile data: "go", or "wifi" (it waits for Wi-Fi). Asks when the setting says Ask. */
  decide(bytes: number): Promise<"go" | "wifi">;
  /** After "wifi": resolves "wifi" once an unmetered line is up (at once if it is), or "now" if the reader chose to go. */
  untilAllowed(): Promise<"wifi" | "now">;
}

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
  /** (T254) the app's bad-connection kit: the web source's fetches take their turn, survive cuts, say how far they got */
  net?: Net | null;
  /** `release`: (T746) with "apply", the id of the release being put in use (its line is picked by it, core `applyLine`) */
  onStage?: (s: BootStage, release?: string | null) => void;
  onProgress?: (p: Progress) => void;
  /** (T298) a phone's system downloader (Android's DownloadManager): the first install's big files go on with the app closed */
  background?: BackgroundNative | null;
  /**
   * (T298) Before the first install's big files go to the system's downloader: the app asks to notify where the system
   * needs it (Android 13+: its download notification shows only then). Never throws; absent: nothing is asked.
   */
  beforeBackground?: () => Promise<void>;
  /** (T298) a phone's first install asks before a large download on mobile data (absent: it just goes) */
  mobileData?: MobileDataGate | null;
  /**
   * (T256) The web waits for paint before the Shell draws (default). False: a start that opens a Regulations page first
   * (`?regs=`, a water's own page) draws it without paint — the map's status asks for paint when it is shown. Native
   * ignores it (paint is on disk).
   */
  paintFirst?: boolean;
  /**
   * (T304) The region the start is about (a water's own page names its region, `cift:region`): the web loader is
   * focused on it BEFORE anything reads core or water, so a landing reads `core-<r>` / `water-<r>` (T256) instead of the
   * whole files — what a tap in that region of the map reads. A slice answers only what it covers (`focus.ts`), so a
   * wrong region costs bytes, never a different answer. Native ignores it (a phone holds no slices).
   */
  focus?: Region | null;
  /** How long a launch waits for the manifest before going on with the held set (native). */
  manifestTimeoutMs?: number;
  /**
   * (T231) Leave the background update to the update service (`Booted.stage`, called by `@app/data` `createUpdateService`
   * when its policy allows: Wi-Fi for a large set): the launch starts no install of its own. Absent/false: the launch
   * installs a newer set in the background, as before.
   */
  deferUpdate?: boolean;
  /**
   * (T418) AN UPDATE APPLIED WHILE THE SET IT REPLACES IS ON SCREEN (the app's in-session apply, `useReleaseBoot`'s
   * generation > 0): the replaced set is NOT pruned by this boot. Its tile folder — the atlas, the outside mask, the map
   * assets (glyphs, sprites) — is what the map on screen still reads until the new set is drawn; pruned under it, every
   * one of them failed to load (0.5.0, 2026-11-04 17:07: r-4e70 → r-5a59 changed the tile folder). It stays the
   * PREVIOUS pointer and is pruned by the next launch, or by the next `stage` (the new set is on screen by then).
   */
  keepReplaced?: boolean;
  /**
   * (T251) THE RELEASE GATE on a phone: a manifest whose `minAppVersion` is newer than this app is never installed — a
   * first run says "Update the app to get new regulations" (there is nothing older to keep); a later launch keeps the
   * held set. The web passes none: its app code is deployed with its data.
   */
  appVersion?: AppVersion | null;
  /**
   * (T722 F6) Mark a file or folder (its file:// uri) as not for the device's backup: iOS (`isExcludedFromBackup`, the
   * iOS half of `modules/bg-download`). Absent (Android, the web): nothing is marked.
   */
  noBackup?: ((uri: string) => Promise<void>) | null;
}

export interface Booted {
  manifest: Manifest;
  /** The installed set (native) or what the web holds (the manifest's tiles, nothing on disk). */
  held: Held;
  loader: ReleaseLoader;
  /** Paint, decoded — null on a web start that did not wait for it (`paintFirst: false`; `loader.paint()` has it). */
  paint: Paint | null;
  source: WaterSource;
  /** Where the map reads a release-relative path from: a local file on native, the release root on the web. */
  locate(path: string): string;
  /** Native: the background update, resolving to the set installed for the NEXT launch (null: nothing to do). */
  update: Promise<Held | null>;
  /** Native: the new set would not open, so this launch went back to the one it replaced (null: it did not). */
  rolledBack: RolledBack | null;
  /**
   * (T585 F12) Native: THIS BOOT IS THE FIRST TO OPEN THIS SET, which replaced the set `from` — a set staged earlier
   * (this session, or one that ended before it was applied) is APPLIED by the boot that opens it, a cold start as much
   * as the app's in-session apply. Null: the set opened is the one last opened (nothing was applied), a first install,
   * or a rollback. The app tells the update service and the reader the same way whichever boot it was
   * (`bootSaid`): a cold start that moved the phone to a newer release no longer does it without a word. Null on the web
   * (it holds no set between visits; its in-session apply is known by the app, which held the set before).
   */
  applied: { from: string | null } | null;
  /**
   * (T231) Fetch what a newer manifest needs BESIDE the set in use, verified, and never under it: on a phone the whole
   * set is installed and the pointer swapped (`install`; the set in use stays on disk and open until the next boot, which
   * opens the new one and prunes the old); on the web the changed files this session has opened are fetched into the
   * release cache (immutable urls), so the boot that applies it reads them from there. Resolves when the next boot can
   * use it, as an outcome (`StageOutcome`: staged, already waiting, already in use, refused, or a format this app does not
   * read) — never a bare "nothing", which the update service would take for ready (T555).
   */
  stage(m: Manifest, onProgress?: (p: Progress) => void, o?: StageOptions): Promise<StageOutcome>;
  /** (T231) The manifest this launch read from the server (the web's is `manifest`), or null (offline): the update
   *  service's first check (`seedRelease`), so a launch asks for the manifest once. */
  latest: Manifest | null;
}

/** (T912) How an update is downloaded on a phone. */
export interface StageOptions {
  /**
   * May the system's downloader use mobile data for it (the update service's own rule: a small update, or the reader's
   * "Always"); false: its jobs wait for Wi-Fi, also after the app is closed. Absent: true (it was allowed to start).
   */
  metered?: boolean;
}

/**
 * (T912) EVERY FILE AN INSTALL DOWNLOADS, FOR THE SYSTEM'S DOWNLOADER — one list for the first install (`held` null: every
 * file) and an update (what changed beside `held`): the data files (first: they are small and the install reads them
 * first) and the big files. Queued before the install starts, so an app closed a second later still downloads all of it;
 * the next open claims what finished. (T298 queued the first install's big files only, and an update downloaded its data
 * files in the app and queued each big file only when the install reached it.)
 */
export function queueItems(m: Manifest, held: Held | null, base: string): BgItem[] {
  return [
    ...dataFiles(m, held).map((f) => ({ url: `${base}${f.url}`, file: "data", bytes: f.e.bytes, path: f.path, whole: true })),
    ...bigFiles(m, held).map((b) => ({ url: `${base}${b.url}`, file: b.file, bytes: b.e.bytes, path: b.path })),
  ];
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

/** (T722 F9) A release download that will not fit on the phone: its message is the reader's words (`spaceLine`). */
export class SpaceError extends BootError {}

/**
 * (T845) The manifest names a release this app cannot read (`readsRelease`) and nothing this app reads is held: the
 * start stops. Its message is the developer's (which format, which file: the log); the screen says
 * `INSTALL_WORDS.unreadable` (useReleaseBoot's `said`), not "the map data could not be loaded".
 */
export class UnreadableError extends BootError {}

/**
 * (T722 F9) WILL THE DOWNLOAD FIT? Before a release download (the first install, an update), on both platforms: what
 * installing `m` beside `held` downloads (`installBytes`, the manifest's sizes) less what of its big files is on disk
 * already (a `.part` or the finished file at its path, or on Android the bytes its system download holds), against the
 * phone's free bytes. Too little: a `SpaceError` naming both, before anything is queued or downloaded (shown by the boot
 * screen or as the update's error, reported by both). A phone that will not say its free space is reported, and the
 * download goes on (a full disk then fails the write).
 */
export async function checkRoom(m: Manifest, held: Held | null, space: DeviceSpace | undefined,
                                jobs: readonly BgJob[] = []): Promise<void> {
  if (!space) return;
  let have = 0;
  for (const b of bigFiles(m, held)) {
    const job = jobs.find((j) => j.url.endsWith(`/${b.url}`));
    const on = Math.max(await space.size(b.path), await space.size(`${b.path}.part`), job?.bytes ?? 0);
    have += Math.min(b.e.bytes, Math.max(0, on));
  }
  const need = installBytes(m, held) - have;
  const free = await space.free();
  if (typeof free !== "number" || !Number.isFinite(free) || free < 0) {
    reportFailure(new Error(`[space] the phone did not say its free space (${String(free)}); ${need} bytes to download`));
    return;
  }
  console.info(`[space] ${need} bytes to download, ${free} free`);
  if (free < need) throw new SpaceError(spaceLine(need, free));
}

/** (T722 F9) The system downloader's jobs (Android, iOS), for what of a big file it holds already; none without one. */
const jobsOf = async (bg: BackgroundNative | null | undefined): Promise<BgJob[]> => (bg ? bg.jobs() : []);

/**
 * (T722 F6) THE RELEASE OUT OF THE DEVICE'S BACKUP (iOS `isExcludedFromBackup`, a flag per item, so it is set again after
 * each install and at each boot: a folder made again loses it). `release/` holds every set, tile folder and pointer; the
 * rule book sits beside it at the document root (`synopsisPath`). Saved spots and their photos (`spots/`), the settings
 * (`settings/`) and the run-timing cache (`feeds/`) are outside both and stay in the backup. A failure is reported and
 * the boot goes on.
 */
async function outOfBackup(noBackup: BootOptions["noBackup"], dev: NonNullable<ReturnType<ReleasePlatform["device"]>>,
                           m: Manifest): Promise<void> {
  if (!noBackup) return;
  const book = m.synopsis?.sha256 ? synopsisPath(m.synopsis.sha256) : null;
  const uris = [dev.dirUri("release"), ...(book && await dev.fs.exists(book) ? [dev.fileUri(book)] : [])];
  for (const uri of uris) await noBackup(uri).catch((e: unknown) => { reportFailure(e); });
}

/** The set an update replaced, kept until the new one has opened and paired (written just before the swap). */
const PREVIOUS = "release/previous";
/** A set that would not open and was rolled back: not installed again until the manifest names another. */
const REFUSED = "release/refused";
/** (T585 F12) The set the last successful boot opened: a boot that opens another one is the boot that APPLIED it. */
const OPENED = "release/opened";

/** (T254) The manifest through `fetchResilient` (timeouts, retries, waiting out an offline spell); an HTTP refusal stays a
 *  `ManifestError`. */
async function resilientManifest(url: string, f: typeof fetch, net: Net): Promise<Manifest> {
  const body = await fetchResilient(url, { fetchImpl: f, wireBytes: 8000, isOnline: net.online, waitOnline: net.waitOnline })
    .catch((e: unknown) => {
      if (e instanceof NetError && e.kind === "http") throw new ManifestError(`manifest: HTTP ${e.status} for ${url}`);
      throw e;
    });
  return parseManifest(JSON.parse(new TextDecoder().decode(body)));
}

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

/**
 * (T741) The slice a held set's paint is decoded in: a LAUNCH (nothing but the boot screen up) takes the long one; an
 * update applied in the session (`keepReplaced`: the replaced set's map is on screen, a finger may be on it) the short.
 */
export function paintSliceFor(o: Pick<BootOptions, "keepReplaced">): { sliceMs: number } {
  return { sliceMs: o.keepReplaced ? SLICE_MS : LAUNCH_SLICE_MS };
}

export async function bootRelease(o: BootOptions): Promise<Booted> {
  const f = o.fetch ?? fetch;
  const dev = o.platform.device(f, o.background ?? null);
  o.onStage?.("manifest");

  if (!dev) {
    // THE WEB: online by definition; the manifest is required, and only paint is fetched before the map
    // (T254) on a bad line the manifest is a resilient download: a stalled request is cut and tried again, and while
    // the device is offline the boot waits for it to come back rather than failing
    const m = o.net ? await resilientManifest(o.manifestUrl, f, o.net) : await loadManifest(o.manifestUrl, undefined, f);
    const readable = readsRelease(m, "web");
    if (!readable.ok) throw new UnreadableError(readable.reason);
    const cache = await o.platform.releaseCache();
    const loader = withLiveSalmon(
      releaseLoader(m, o.platform.releaseSource(m, { base: o.releaseBase, cache, held: null, fetch: f, net: o.net ?? null }),
                    o.platform.releaseOpener({ wasmBase: o.wasmBase, installed: () => null, net: o.net ?? null })), o.salmonFeed);
    // (T304) the start's region, before the first read of core / water (none happens before this returns)
    if (o.focus) loader.focus(o.focus);
    // (T256) a Regulations-first start draws its page before paint: the map asks for it when it is shown
    let paint: Paint | null = null;
    if (o.paintFirst !== false) {
      o.onStage?.("paint");
      paint = await loader.paint();
    }
    const held: Held = { tiles: m.tiles.current?.id ?? null, basemap: m.basemap?.id ?? null,
                         assets: m.tiles.assets?.id ?? null, release: m.data_release, files: {} };
    const stage = async (next: Manifest, onProgress?: (p: Progress) => void): Promise<StageOutcome> => {
      if (sameSet(pointerFor(next), pointerFor(m))) return { kind: "in-use" };
      const r = readsRelease(next, "web");
      if (!r.ok) return { kind: "unreadable", reason: r.reason };
      // the files this session has opened that the new release changed: warmed in the release cache, verified
      const names = loader.opened().filter((n) => next.files[n] && next.files[n]!.sha256 !== m.files[n]?.sha256);
      const ahead = releaseLoader(next, o.platform.releaseSource(next, { base: o.releaseBase, cache, held: null, fetch: f, net: o.net ?? null }),
                                  o.platform.releaseOpener({ wasmBase: o.wasmBase, installed: () => null, net: o.net ?? null }));
      const of = names.reduce((n, k) => n + next.files[k]!.bytes, 0);
      let done = 0, bytes = 0;
      for (const n of names) {
        await ahead.prefetch(n);
        bytes += next.files[n]!.bytes;
        onProgress?.({ done: ++done, total: names.length, file: n, bytes, of });
      }
      return { kind: "staged", held: pointerFor(next) };
    };
    return { manifest: m, held, loader, paint, source: makeReleaseSource(loader, { feed: o.feed }),
             locate: (path) => `${o.releaseBase}${path}`, update: Promise.resolve(null), rolledBack: null, applied: null,
             stage, latest: m };
  }

  // THE PHONE: the held set first; the network only to install it once and to look for a newer one
  let held = await heldFrom(dev.fs);
  /**
   * (T912) THE ONE WAY A PHONE'S INSTALL STARTS ITS DOWNLOADS — the first install, an older app's set installed over, an
   * update: every file it downloads (`queueItems`) handed to the system's downloader first. A queue that fails is said and
   * reported, and the install goes on in the app (a file with no job: `fetch` / `fill`).
   */
  const handOver = (m: Manifest, from: Held | null, metered: boolean) => dev.background?.queue(queueItems(m, from, o.releaseBase), metered)
    .catch((e: unknown) => { console.warn(`[download] queue failed: ${e instanceof Error ? e.message : String(e)}`); reportFailure(e); });
  /** (T272) A first run's last install progress, and its second stage's open steps (none on a later launch). */
  let last: Progress | null = null;
  let openStep: (done: number) => void = () => {};
  // no manifest is normal offline (the held set opens), but WHY is kept: a first run says it, a later one logs it
  let unread = null as string | null;
  const latest = await timeout(loadManifest(o.manifestUrl, undefined, f), o.manifestTimeoutMs ?? 5000)
    .catch((e: unknown) => { unread = e instanceof Error ? e.message : String(e); return null; });
  if (unread) console.warn(`[boot] the manifest could not be read: ${unread}`);
  const allowed = (m: Manifest) => !o.appVersion || releaseAllowed(m.minAppVersion, o.appVersion).ok;
  /** (T403) Can this app read the set `h` holds? (a manifest that will not read is openSet's to refuse) */
  const heldReads = async (h: Held) => {
    const hm = await heldManifest(dev.fs, h).catch(() => null);
    return hm ? readsRelease(hm, "phone") : { ok: true as const };
  };
  const stale = held ? await heldReads(held) : null;
  if (held && stale && !stale.ok) {
    // (T403) THE HELD SET IS AN OLDER APP'S: installed over before anything opens it (it is kept until the new one has
    // opened, then pruned, as an update's); without a manifest this app reads, the start fails and says why
    console.warn(`[boot] ${stale.reason}: the manifest's set is installed before the map opens`);
    if (!latest) throw new BootError(`${stale.reason}, and the manifest could not be read (${unread ?? "no answer"})`, true);
    if (!allowed(latest)) throw new BootError(UPDATE_WORDS.row.appTooOld);
    const fresh = readsRelease(latest, "phone");
    if (!fresh.ok) throw new UnreadableError(`${stale.reason}; and the manifest's ${fresh.reason}`);
    const old = held;
    await checkRoom(latest, old, dev.space, await jobsOf(o.background));
    let metered = true;
    if (o.mobileData && await o.mobileData.decide(installBytes(latest, old)) === "wifi") {
      o.onStage?.("wifi");
      metered = await o.mobileData.untilAllowed() === "now";   // Wi-Fi came: the jobs keep to Wi-Fi, as the reader chose
    }
    await handOver(latest, old, metered);
    o.onStage?.("install");
    held = await install(latest, dev.fs, dev.download, o.releaseBase, old, (p) => o.onProgress?.(p), dev.toDisk,
                         () => writeHeld(dev.fs, PREVIOUS, old), rawFetch(f), dev.look);
  }
  if (!held) {
    if (!latest) throw new BootError(`first run: the manifest could not be read (${unread ?? "no answer"})`, true);
    if (!allowed(latest)) throw new BootError(UPDATE_WORDS.row.appTooOld);
    const fresh = readsRelease(latest, "phone");
    if (!fresh.ok) throw new UnreadableError(fresh.reason);
    // (T722 F9) before anything is queued or downloaded: a download that will not fit says so
    await checkRoom(latest, null, dev.space, await jobsOf(o.background));
    // (T298, T912) every file handed to the system's downloader BEFORE anything downloads, so closing the app a moment
    // later still fetches all of them (the data files too, T912: one path with an update's); on mobile data the reader's
    // setting decides first (ask / go / wait for Wi-Fi)
    const queue = (metered: boolean) => handOver(latest, null, metered);
    if (dev.background) await o.beforeBackground?.().catch(() => {});
    let metered = true;
    const choice = o.mobileData ? await o.mobileData.decide(installBytes(latest, null)) : "go";
    console.info(`[download] mobile data: ${choice} (${installBytes(latest, null)} bytes)`);
    if (choice === "wifi" && o.mobileData) {
      metered = false;
      await queue(false);                      // they start on Wi-Fi by themselves, the app open or not
      o.onStage?.("wifi");
      if (await o.mobileData.untilAllowed() === "now") metered = true;
    }
    await queue(metered);
    o.onStage?.("install");
    const t0 = clockNow();
    let downAt = 0;
    held = await install(latest, dev.fs, dev.download, o.releaseBase, null, (p) => {
      if (!downAt && p.of > 0 && p.bytes >= p.of) { downAt = clockNow(); console.info(`[setup] downloaded in ${downAt - t0} ms`); }
      last = p;
      o.onProgress?.(p);
    }, dev.toDisk, undefined, rawFetch(f), dev.look);
    console.info(`[setup] installed ${clockNow() - (downAt || t0)} ms after the last byte`);
    // (T272) THE SECOND STAGE goes on through opening the set: the manifest, paint, core (the steps below)
    const p = last as Progress | null;    // set by the callback above
    if (p) openStep = (done) => o.onProgress?.({ ...p, setup: { step: "open", done, of: 3 } });
  }
  /** Open a held set: its manifest, paint decoded against its handles, core opened and checked, its tiles paired. */
  const cache = await o.platform.releaseCache();
  // (T585 F12) the set this device opened last — read BEFORE opening, so (T746) a start that applies a set downloaded
  // earlier says so while it opens ("apply"); unreadable, it is reported and the boot goes on (worst case a later launch
  // says "Updated to" again)
  const openedBefore = await readHeld(dev.fs, OPENED).catch((e: unknown) => { reportFailure(e); return null; });
  const firstOpen = (h: Held) => openedBefore !== null && !sameSet(openedBefore, h);
  const openSet = async (h: Held) => {
    const t0 = clockNow();
    openStep(0);
    const m = await heldManifest(dev.fs, h);
    const r = readsRelease(m, "phone");
    if (!r.ok) throw new ReleaseError(r.reason);              // (T403) never opened: its readers would throw later
    openStep(1);
    const t = m.tiles.current;
    if (t && !(t.id && compatible(m.tiles.compatible, t.id)))
      throw new ReleaseError(`release ${m.data_release}: tiles ${t.id ?? "(no id)"} are not in ${m.tiles.compatible}`);
    const dir = `release/${h.release}`;
    const loader = releaseLoader(m,
      o.platform.releaseSource(m, { base: o.releaseBase, cache, held: h, fetch: f, net: o.net ?? null }),
      o.platform.releaseOpener({
        wasmBase: o.wasmBase,
        installed: (name) => {
          const spec = m.files[name];
          return spec ? { dir: dev.dirUri(dir), file: heldPath(h, spec.url).split("/").pop()! } : null;
        },
      }));
    if (firstOpen(h)) o.onStage?.("apply", h.release ?? null); else o.onStage?.("paint");
    // (T741) a launch decodes paint in long slices (the boot screen is all there is); an update applied in the session
    // (`keepReplaced`: the map is on screen) keeps the short ones
    const paint = await loader.paint(paintSliceFor(o));
    openStep(2);
    const tPaint = clockNow();
    await loader.db("core");                      // opened in place on a phone: the first file any question needs
    openStep(3);
    console.info(`[setup] opened ${h.release}: manifest + paint ${tPaint - t0} ms, core ${clockNow() - tPaint} ms`);
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
    // (T403) never back to a set this app cannot read (an older app's, just installed over): the failure stands
    if (!previous || sameSet(previous, held) || !(await heldReads(previous)).ok) throw e;
    reportFailure(e);   // (T368) shown (`rolledBackLine`) and reported
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

  // the set this one replaced is deleted only now: the new one has opened, decoded its paint and paired — and (T418) only
  // when nothing still draws it: an in-session apply leaves it to the next launch or the next stage (`keepReplaced`)
  let replaced: { h: Held; m: Manifest | null } | null = null;
  if (!rolledBack && previous) {
    if (!sameSet(previous, held)) {
      const previousM = await heldManifest(dev.fs, previous).catch(() => null);
      if (o.keepReplaced) replaced = { h: previous, m: previousM };
      else await prune(dev.fs, previous, previousM, held, set.m);
    }
    if (!replaced) await dev.fs.remove(PREVIOUS);
  }

  const now = held;
  // (T722 F6) at every boot, so a set an older app installed is covered too (a first install's is this call)
  await outOfBackup(o.noBackup, dev, set.m);
  // (T585 F12) is this the boot that APPLIES the set (the first to open it)? The record (read above) is this device's own
  // small file; unwritable, it is reported and the boot goes on
  const applied = openedBefore && !sameSet(openedBefore, now) ? { from: openedBefore.release ?? null } : null;
  if (!openedBefore || applied) await writeHeld(dev.fs, OPENED, now).catch((e: unknown) => { reportFailure(e); });
  const refused = await readHeld(dev.fs, REFUSED);
  /** (T231) The set installed for the next boot by this session (one at a time: a newer one waits for that boot). */
  let staged: Held | null = null;
  let staging: Promise<Held> | null = null;
  // an update whenever ANY part of the set moved — data files, tiles, mask, basemap, assets — unless it was refused, or
  // is the set this launch rolled back from and will retry (already installed, still the pointer's)
  const stage = async (next: Manifest, onProgress: (p: Progress) => void = () => {}, so: StageOptions = {}): Promise<StageOutcome> => {
    if (staging) return { kind: "staged", held: await staging };
    const target = pointerFor(next);
    if (sameSet(target, now)) return { kind: "in-use" };
    if (refused && sameSet(target, refused)) return { kind: "refused", release: refused.release ?? null };
    // already installed: the set being retried (the pointer names it), or one this session staged (one at a time)
    if (staged || (retrying && sameSet(target, retrying.h))) return { kind: "waiting" };
    // (T403) a set this app cannot read is never installed: a named state (the app is to be updated), not a failure
    const r = readsRelease(next, "phone");
    if (!r.ok) return { kind: "unreadable", reason: r.reason };
    staging = (async () => {
      // a newer set replaces the one being retried: what of it the held set does not name goes first
      if (retrying) { await prune(dev.fs, retrying.h, retrying.m, now, set.m); retrying = null; }
      // (T418) the set an in-session apply kept for the map that was on screen: this set has been drawn since
      if (replaced) { await prune(dev.fs, replaced.h, replaced.m, now, set.m); replaced = null; }
      await checkRoom(next, now, dev.space, await jobsOf(o.background));
      // (T912) everything it downloads handed to the system's downloader first: closing the app no longer stops it
      await handOver(next, now, so.metered ?? true);
      const h = await install(next, dev.fs, dev.download, o.releaseBase, now, onProgress, dev.toDisk,
                              () => writeHeld(dev.fs, PREVIOUS, now), rawFetch(f), dev.look);
      // (T722 F6) after each install: what it made (a new tile folder, the new rule book) is marked with the rest
      await outOfBackup(o.noBackup, dev, next);
      staged = h;
      return h;
    })();
    staging.then(() => { staging = null; }, () => { staging = null; });
    return { kind: "staged", held: await staging };
  };
  /** the launch's own update: the set installed for the next launch; an unreadable release is its failure, as before */
  const heldOf = (r: StageOutcome): Held | null => {
    if (r.kind === "unreadable") throw new ReleaseError(r.reason);
    return r.kind === "staged" ? r.held : null;
  };
  const update = latest && !o.deferUpdate && allowed(latest) ? stage(latest).then(heldOf) : Promise.resolve(null);
  update.catch(() => {});        // a failed update leaves the held set in force (install never swapped)

  const loader = withLiveSalmon(set.loader, o.salmonFeed);
  return { manifest: set.m, held: now, loader, paint: set.paint,
           source: makeReleaseSource(loader, { feed: o.feed }),
           locate: (path) => dev.fileUri(`release/${path}`), update, rolledBack, applied, stage, latest };
}

/**
 * (T555 item 1, T585 F12) WHAT A FINISHED BOOT MEANS FOR THE UPDATE SERVICE AND THE READER — one rule for every boot, the
 * cold start (generation 0) and the app's in-session apply (generation > 0) alike:
 *   applied       a set that replaced another was opened for the first time: the service is told (`applied`) and the
 *                 reader is ("Updated to …"). Native: the boot's own word (`Booted.applied`). The web holds no set
 *                 between visits, so only an in-session apply counts there, known by the set held `before` it.
 *   not-applied   an in-session apply went back to the set it had (`rolledBack`): the service names the refused release.
 *   nothing       an ordinary start; an apply that failed (said by `applyError`); a cold start's rollback (the banner and
 *                 the service's own check say it); a re-boot onto the set already in use — never "Updated to".
 */
export type BootSaid =
  | { kind: "applied"; release: string; from: string | null }
  | { kind: "not-applied"; refused: string; reason: string }
  | { kind: "nothing" };
export function bootSaid(i: { generation: number; applyError?: unknown;
                              booted: Pick<Booted, "manifest" | "held" | "rolledBack" | "applied">;
                              /** the set in use when the in-session apply was asked (null: a cold start) */
                              before: Held | null }): BootSaid {
  if (i.applyError) return { kind: "nothing" };
  const b = i.booted;
  if (b.rolledBack) {
    return i.generation > 0 ? { kind: "not-applied", refused: b.rolledBack.refused ?? "unknown", reason: b.rolledBack.reason }
      : { kind: "nothing" };
  }
  if (b.applied) return { kind: "applied", release: b.manifest.data_release, from: b.applied.from };
  // the web's in-session apply: the set it boots onto is not the one it held (the install's own `sameSet`)
  if (i.generation > 0 && i.before !== null && !sameSet(i.before, b.held))
    return { kind: "applied", release: b.manifest.data_release, from: i.before.release ?? null };
  return { kind: "nothing" };
}

/**
 * The map's three files from the release when the manifest names them, else the development endpoints (`ENDPOINTS.tiles`)
 * — until the human-run tile rebuild writes a tile version folder (P18), the release names no tiles and no mask.
 * N2: and the map ASSETS folder (glyphs + sprites) when the manifest names one, so labels come from the release — the
 * installed `file://` folder on a phone — and not from a CDN. Absent, the environment's own data host (`dev.assets`) — never a CDN.
 * (T404) `dev.assets` is REQUIRED: the caller passes its environment's folder (`ENDPOINTS.tiles.assets`), so the map can
 * never fall back to some other environment's host.
 */
export function releaseTiles(b: Pick<Booted, "manifest" | "locate">,
                             dev: { atlas: string; basemap: string; outside: string; assets: string }):
    { atlas: string; basemap: string; outside: string; assets: string } {
  const t = b.manifest.tiles;
  const assets = t.assets ? b.locate(t.assets.url).replace(/\/?$/, "/") : null;
  return {
    atlas: t.current ? b.locate(t.current.url) : dev.atlas,
    basemap: b.manifest.basemap ? b.locate(b.manifest.basemap.url) : dev.basemap,
    outside: t.mask ? b.locate(t.mask.url) : dev.outside,
    // the glyphs and sprites: the installed folder on a phone, the release url on the web (the style's CDN otherwise)
    assets: assets ?? dev.assets,
  };
}
