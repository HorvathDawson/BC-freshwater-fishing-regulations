/**
 * ALWAYS OFFLINE ON NATIVE (P11, MINIMAL-PACKAGING §5): install a release, update it, swap atomically. Platform-free over
 * an injected file system (install.native.ts adapts expo-file-system), so a test can crash it anywhere.
 *
 *   layout     release/<data_release>/<file>       the data files, raw (sha-verified)
 *              release/tiles/<tile version>/…       the tiles, mask and map assets (only when the version moves)
 *              release/<data_release>/manifest-<sha12>.json   the manifest a set was installed from (one per set)
 *              release/current                      THE POINTER: {release, files: {name: sha}, tiles, assets, basemap,
 *                                                   manifest}
 *   install    every manifest file → release/<r>/, each written to `.part`, verified, then renamed; a file already there
 *              with the right sha is kept (resume at file granularity).
 *   update     diff (manifest.ts) against the held pointer: changed files are downloaded, unchanged ones COPIED from the
 *              held set (never re-downloaded); everything verified; then the pointer is replaced by ONE rename. A crash
 *              or a failed sha before that rename leaves the old pointer — the old set — in force. A release whose data
 *              is unchanged (a tile-only release) is an update too: its tiles, mask, basemap or assets moved.
 *   prune      after the new set has opened and paired, everything of the old set the new one does not name is deleted
 *              (never before): its release folder, its manifest, and its tile version folder, mask, basemap, assets.
 */
import { diff, parseManifest, type Held, type Manifest } from "../manifest";
// Hermes has no TextDecoder (regs/blob.ts); TextEncoder it has
import { utf8 } from "../regs/blob";
import { Sha256, sha256Hex } from "../sha256";
import { VerifyError } from "../fetchVerified";
import { assetsDir, installAssets, type RawFetch } from "./assets";

export interface ReleaseFs {
  exists(path: string): Promise<boolean>;
  read(path: string): Promise<Uint8Array>;
  write(path: string, bytes: Uint8Array): Promise<void>;
  /** Move `from` onto `to`, replacing it (the one atomic step). */
  rename(from: string, to: string): Promise<void>;
  remove(path: string): Promise<void>;
}
/** Fetch the RAW bytes of a release file and verify them (fetchVerified), or throw. */
export type Download = (url: string, expect: { bytes: number; sha256: string }) => Promise<Uint8Array>;
/**
 * Stream a file too big to hold (the tiles: 531 MB, the basemap 918 MB) straight to `path`: written to `${path}.part`
 * (resumed when one is there), verified off disk chunk by chunk (`Sha256`), then renamed onto `path`. Throws, leaving
 * the `.part` for the next attempt, when the bytes are not the manifest's. `install.native.ts` is the phone's.
 */
export type DownloadTo = (url: string, path: string, expect: { bytes: number; sha256: string },
                          onBytes?: (n: number) => void, onHashed?: (n: number) => void) => Promise<void>;
/** What `rangedDownload` needs of a file system: a `.part` it can grow, and read back a bounded chunk at a time. */
export interface PartFs {
  /** Bytes already in `path` (0 when it is absent). */
  size(path: string): Promise<number>;
  append(path: string, bytes: Uint8Array): Promise<void>;
  /** Feed the whole file to `each`, a bounded chunk at a time (never the whole file in memory). */
  chunks(path: string, each: (bytes: Uint8Array) => void): Promise<void>;
  /**
   * (T272) `length` bytes of `path` from `offset` (fewer at its end). With it, `partDownload` hashes the `.part` WHILE it
   * downloads; without it, the whole file is hashed once it is down.
   */
  readAt?(path: string, offset: number, length: number): Promise<Uint8Array>;
  rename(from: string, to: string): Promise<void>;
  remove(path: string): Promise<void>;
}

/**
 * Fill `part` up to `total` bytes, resuming after the `have` it already holds (bytes it already holds are kept), calling
 * `onBytes` with the length so far. A transport: `rangedFill` (JS fetch, by ranges) or the phone's native downloader.
 */
export type PartFill = (url: string, part: string, have: number, total: number,
                        onBytes?: (n: number) => void, onRewrite?: () => void) => Promise<void>;

/** (T272) How much of a growing `.part` the follower reads and hashes at a time. */
const FOLLOW_CHUNK = 1 << 20;
const nextTick = () => new Promise<void>((r) => setTimeout(r, 0));

/**
 * THE RESUMABLE DOWNLOAD (MINIMAL-PACKAGING §5): `fill` grows `${path}.part`. The `.part` IS the resume state — a crash
 * or a dropped connection leaves it, and the next attempt asks for the bytes after its length — so nothing else has to
 * be saved. When it is whole it is hashed off disk (`Sha256`, chunk by chunk) and renamed onto `path`; bytes that are not
 * the manifest's drop the `.part`.
 */
/**
 * (T272) The sha256 of a whole file off disk. With `readAt`, a megabyte at a time with a turn for the screen between
 * (its progress is drawn); without, `chunks` in one go.
 */
async function hashFile(fs: PartFs, path: string, onHashed?: (n: number) => void): Promise<string> {
  const h = new Sha256();
  if (!fs.readAt) {
    let n = 0;
    await fs.chunks(path, (b) => { h.update(b); n += b.length; onHashed?.(n); });
    return h.hex();
  }
  const size = await fs.size(path);
  for (let at = 0; at < size;) {
    const b = await fs.readAt(path, at, Math.min(FOLLOW_CHUNK, size - at));
    if (!b.length) break;
    h.update(b); at += b.length; onHashed?.(at);
    await nextTick();
  }
  return h.hex();
}

export function partDownload(fs: PartFs, fill: PartFill): DownloadTo {
  return async (url, path, e, onBytes, onHashed) => {
    const part = `${path}.part`;
    // already in place from a stage that failed after it (the file is named by its content): kept when it verifies
    if (await fs.size(path) === e.bytes) {
      // (T272) its bytes are down: the bar says so, and the re-check is the second stage's (~40 s for both map files)
      onBytes?.(e.bytes);
      if (await hashFile(fs, path, onHashed) === e.sha256) { onHashed?.(e.bytes); return; }
    }
    let have = await fs.size(part);
    if (have > e.bytes) { await fs.remove(part); have = 0; }
    // (T272) THE FOLLOWER: the `.part` is hashed while it downloads, a megabyte behind the writer, so the wait after the
    // last byte is what the hash has left, not the whole file (on the emulator 274 MB took 23 s to hash AFTER the
    // download). Only bytes on disk are hashed, in order; it is trusted only for ONE clean fill — the writer only ever
    // appended. A fill that rewrote the `.part` (a retry, a server that sent it whole) says so (`onRewrite`) and the file
    // is hashed again off disk, as before.
    const readAt = fs.readAt?.bind(fs);
    let rewritten = false, filling = true;
    const fh = new Sha256();
    let hashed = 0;
    const follow = async () => {
      if (!readAt) return;
      for (;;) {
        if (rewritten) return;
        const size = await fs.size(part);
        if (size < hashed) { rewritten = true; return; }
        if (size > hashed) {
          const b = await readAt(part, hashed, Math.min(FOLLOW_CHUNK, size - hashed));
          if (rewritten) return;
          fh.update(b); hashed += b.length; onHashed?.(hashed);
          await nextTick();                       // the download's progress and the screen get their turn
          continue;
        }
        if (!filling) return;
        await new Promise<void>((r) => setTimeout(r, 100));
      }
    };
    const following = follow().catch(() => { rewritten = true; });     // a read that failed: hashed off disk
    const t0 = Date.now();
    try {
      if (have < e.bytes) await fill(url, part, have, e.bytes, onBytes, () => { rewritten = true; });
    } finally {
      filling = false;
    }
    const tFill = Date.now();
    await following;
    const got = await fs.size(part);
    if (got !== e.bytes) {
      if (got > e.bytes) await fs.remove(part);
      throw new VerifyError(`${url}: ${got} bytes on disk, the manifest says ${e.bytes}`);
    }
    let sha = !rewritten && hashed === got ? fh.hex() : null;
    const tail = Date.now() - tFill;
    // not followed (no readAt, a rewrite), or a follower hash that does not match: the whole file, off disk, decides
    if (sha !== e.sha256) sha = await hashFile(fs, part, onHashed);
    console.info(`[download] ${url}: sha256 of ${got} bytes — ${Date.now() - tFill} ms after the download ` +
                 `(${hashed === got && !rewritten ? `followed, ${tail} ms to catch up` : "hashed off disk"}; fill ${tFill - t0} ms)`);
    if (sha !== e.sha256) {
      await fs.remove(part);
      throw new VerifyError(`${url}: sha256 ${sha.slice(0, 12)}…, the manifest says ${e.sha256.slice(0, 12)}…`);
    }
    await fs.rename(part, path);
  };
}

/**
 * The JS transport: HTTP Range requests of `chunk` bytes appended to the `.part`. A server that answers a range with
 * anything but 206 is refused: a 200 is the whole file, which a phone must not take into memory. NOT the Android one
 * (T262): each body passes through expo's fetch as a Java-heap buffer that outlives its use until a JS GC, and a
 * 500 MB first install filled the 192 MB heap — Android downloads natively (`install.native.ts`).
 */
export function rangedFill(fs: PartFs, fetchImpl: typeof fetch = fetch, chunk = 8 * 1024 * 1024): PartFill {
  return async (url, part, have, total, onBytes) => {
    while (have < total) {
      const end = Math.min(have + chunk, total) - 1;
      const res = await fetchImpl(url, { headers: { Range: `bytes=${have}-${end}` } });
      if (res.status !== 206) {
        await fs.remove(part);
        throw new VerifyError(`${url}: HTTP ${res.status} to a range request (a resumable download needs 206)`);
      }
      const b = new Uint8Array(await res.arrayBuffer());
      if (b.length !== end - have + 1) throw new VerifyError(`${url}: ${b.length} bytes for a ${end - have + 1}-byte range`);
      await fs.append(part, b);
      have += b.length;
      onBytes?.(have);
    }
  };
}

/** The resumable download by JS Range requests (tests, and any platform without a native downloader). */
export function rangedDownload(fs: PartFs, fetchImpl: typeof fetch = fetch, chunk = 8 * 1024 * 1024): DownloadTo {
  return partDownload(fs, rangedFill(fs, fetchImpl, chunk));
}

/**
 * (T272) THE SECOND STAGE of a first install, after the last byte is down: `verify` the big files' sha256 (`done`/`of` in
 * bytes hashed), `assets` the map's glyphs and sprites (files), `open` the set (steps: manifest, paint, core).
 */
export interface SetupProgress { step: "verify" | "assets" | "open"; done: number; of: number }

/**
 * `done`/`total` count files; `bytes`/`of` count what is fetched (copies from the held set cost nothing). `setup`: set
 * once everything is downloaded and what is left is setting it up on the device (T272).
 */
export interface Progress { done: number; total: number; file: string; bytes: number; of: number; setup?: SetupProgress }

export const POINTER = "release/current";

export async function heldFrom(fs: ReleaseFs): Promise<Held | null> {
  return readHeld(fs, POINTER);
}

const dirOf = (release: string) => `release/${release}`;

async function place(fs: ReleaseFs, path: string, bytes: Uint8Array): Promise<void> {
  await fs.write(`${path}.part`, bytes);
  await fs.rename(`${path}.part`, path);
}

async function fetchInto(fs: ReleaseFs, dl: Download, url: string, path: string,
                         expect: { bytes: number; sha256: string }): Promise<void> {
  if (await fs.exists(path) && sha256Hex(await fs.read(path)) === expect.sha256) return;     // resumed: already there
  await place(fs, path, await dl(url, expect));
}

/** Where a manifest is kept with its set: named by its own bytes, so two sets sharing a data folder keep two. */
const manifestPath = (m: Manifest, bytes: Uint8Array) => `${dirOf(m.data_release)}/manifest-${sha256Hex(bytes).slice(0, 12)}.json`;

/** The pointer a set installed from `m` is held by. */
export function pointerFor(m: Manifest, manifest?: string): Held {
  return { release: m.data_release, tiles: m.tiles.current?.id ?? null, basemap: m.basemap?.id ?? null,
           assets: m.tiles.assets?.id ?? null,
           files: Object.fromEntries(Object.entries(m.files).map(([k, v]) => [k, v.sha256])),
           ...(manifest ? { manifest } : {}) };
}

/** Are two pointers the same set: the same data files, tiles, basemap and map assets (where the manifest is kept aside)? */
export function sameSet(a: Held, b: Held): boolean {
  const fa = Object.keys(a.files).sort(), fb = Object.keys(b.files).sort();
  return (a.release ?? null) === (b.release ?? null) && (a.tiles ?? null) === (b.tiles ?? null)
    && (a.basemap ?? null) === (b.basemap ?? null) && (a.assets ?? null) === (b.assets ?? null)
    && fa.length === fb.length && fa.every((k, i) => k === fb[i] && a.files[k] === b.files[k]);
}

/** Read a pointer-shaped record (the pointer, `release/previous`, `release/refused`), or null when there is none. */
export async function readHeld(fs: ReleaseFs, path: string): Promise<Held | null> {
  if (!(await fs.exists(path))) return null;
  return JSON.parse(utf8(await fs.read(path))) as Held;
}

/** Write a pointer-shaped record whole (to `.part`, then one rename). */
export async function writeHeld(fs: ReleaseFs, path: string, h: Held): Promise<void> {
  await place(fs, path, new TextEncoder().encode(JSON.stringify(h)));
}

/**
 * Download what `m` needs beside `held` (everything when `held` is null) and swap the pointer. `toDisk` streams the
 * tiles and their mask (a test or the web may leave it out: they are then fetched whole through `dl`). `beforeSwap`
 * runs once everything is verified, just before THE SWAP (boot records the set being replaced there, so a launch killed
 * right after the swap still knows what to prune). `raw` fetches the map assets (`assets.ts`: held to the manifest's
 * `files_sha256`, then each file to its sha); a manifest that names assets needs it.
 */
export async function install(m: Manifest, fs: ReleaseFs, dl: Download, base: string, held: Held | null = null,
                              onProgress: (p: Progress) => void = () => {}, toDisk?: DownloadTo,
                              beforeSwap?: () => Promise<void>, raw?: RawFetch): Promise<Held> {
  const d = held ? diff(held, m) : { files: Object.keys(m.files).sort(), tiles: true, basemap: true, assets: true };
  const names = Object.keys(m.files).sort();
  const dir = dirOf(m.data_release);
  const big: { file: string; url: string; path: string; e: { bytes: number; sha256: string } }[] = [];
  const t = m.tiles.current, mask = m.tiles.mask;
  if (d.tiles && t?.sha256 && t.bytes)
    big.push({ file: "tiles", url: t.url, path: `release/${t.url}`, e: { bytes: t.bytes, sha256: t.sha256 } });
  if (d.tiles && mask?.sha256 && mask.bytes)
    big.push({ file: "mask", url: mask.url, path: `release/${mask.url}`, e: { bytes: mask.bytes, sha256: mask.sha256 } });
  const bm = m.basemap;
  if (d.basemap && bm?.sha256 && bm.bytes)
    big.push({ file: "basemap", url: bm.url, path: `release/${bm.url}`, e: { bytes: bm.bytes, sha256: bm.sha256 } });
  const of = d.files.reduce((n, k) => n + (m.files[k]?.bytes ?? 0), 0) + big.reduce((n, b) => n + b.e.bytes, 0);
  let done = 0, bytes = 0;
  const total = names.length + big.length;
  for (const name of names) {
    const e = m.files[name]!, path = `${dir}/${e.url.split("/").pop()!.replace(/\.gz$/, "")}`;
    const from = !d.files.includes(name) && held?.release ? `${dirOf(held.release)}/${path.split("/").pop()}` : null;
    // unchanged: copied from the held set, never downloaded — unless the held copy is no longer the manifest's bytes
    let copied = from !== null && await fs.exists(path) && sha256Hex(await fs.read(path)) === e.sha256;
    if (!copied && from !== null && await fs.exists(from)) {
      const b = await fs.read(from);
      if (sha256Hex(b) === e.sha256) { await place(fs, path, b); copied = true; }
    }
    if (!copied) { await fetchInto(fs, dl, `${base}${e.url}`, path, e); bytes += e.bytes; }
    onProgress({ done: ++done, total, file: name, bytes, of });
  }
  // every file of the new set, re-verified on disk before anything points at it
  for (const name of names) {
    const e = m.files[name]!, path = `${dir}/${e.url.split("/").pop()!.replace(/\.gz$/, "")}`;
    if (sha256Hex(await fs.read(path)) !== e.sha256) throw new Error(`release: ${path} does not match the manifest`);
  }
  // THE MAP ASSETS (glyphs + sprites): always made whole — their folder lives under the tile version, so new tiles move
  // it even when its id does not. Files already there are kept, the held folder's are copied; the rest fetched.
  // (T272) Started NOW, beside the big downloads: ~780 small requests are paced by the data host's rate limit, not by
  // bandwidth, so they cost a first install almost nothing while the map files stream. Their bar is the second stage's
  // once the last byte of the big files is down and hashed.
  const a = m.tiles.assets;
  if (a && !raw) throw new VerifyError(`assets ${a.id ?? a.url}: no way to fetch them`);
  let bigDone = false, assetsFailed: unknown = null;
  const assetsAt: { n: number; all: number } = { n: 0, all: 0 };
  const assetsStep = () => assetsAt.all > 0 && onProgress({ done, total, file: "assets", bytes, of,
                                                    setup: { step: "assets", done: assetsAt.n, of: assetsAt.all } });
  const assetsRun = a && raw ? (async () => {
    const heldA = held ? (await heldManifest(fs, held).catch(() => null))?.tiles.assets ?? null : null;
    const tA = Date.now();
    await installAssets(fs, raw, base, a, heldA ? assetsDir(heldA) : null, (n, all) => {
      assetsAt.n = n; assetsAt.all = all;
      if (bigDone) assetsStep();
    });
    console.info(`[setup] map assets (${assetsAt.all} files) in ${Date.now() - tA} ms`);
  })().catch((e: unknown) => { assetsFailed = e ?? new Error("assets failed"); }) : null;
  // (T272) what the big files' hashes have left once the last byte is down: the second stage's bar
  const verifyOf = big.reduce((n, b) => n + b.e.bytes, 0);
  let verifiedBefore = 0;
  for (const b of big) {
    if (toDisk) {
      const at = bytes, vAt = verifiedBefore;
      let down = at;
      await toDisk(`${base}${b.url}`, b.path, b.e, (n) => { down = at + n; onProgress({ done, total, file: b.file, bytes: down, of }); },
                   (n) => {
                     if (down >= of)
                       onProgress({ done, total, file: b.file, bytes: of, of, setup: { step: "verify", done: vAt + n, of: verifyOf } });
                   });
      verifiedBefore += b.e.bytes;
    } else {
      await fetchInto(fs, dl, `${base}${b.url}`, b.path, b.e);
    }
    bytes += b.e.bytes;
    // (T272) the last byte down and its hash done: the second stage stays on screen (never back to the download line)
    onProgress({ done: ++done, total, file: b.file, bytes, of,
                 ...(toDisk && bytes >= of ? { setup: { step: "verify" as const, done: verifiedBefore, of: verifyOf } } : {}) });
  }
  bigDone = true;
  if (assetsRun) {
    assetsStep();
    await assetsRun;
    if (assetsFailed !== null) throw assetsFailed;
  }
  // the manifest the set was installed from: an offline launch opens the set with it (`heldManifest`)
  const mb = new TextEncoder().encode(JSON.stringify(m));
  const mpath = manifestPath(m, mb);
  await place(fs, mpath, mb);
  const next = pointerFor(m, mpath);
  await beforeSwap?.();
  await writeHeld(fs, POINTER, next);                                            // THE SWAP
  return next;
}

/** The big files a manifest names, as install puts them: `release/<url>` (an assets url is a folder). */
const bigPaths = (m: Manifest | null): string[] =>
  !m ? [] : [m.tiles.current?.url, m.tiles.mask?.url, m.basemap?.url, m.tiles.assets?.url]
    .filter((u): u is string => !!u).map((u) => `release/${u.replace(/\/$/, "")}`);

/**
 * After the new set has opened and paired: delete everything of the set it replaced that the new one does not name —
 * its release folder (or, when the two share one, its own manifest), and its tiles, mask, basemap and map assets with
 * the tile version folder they came in. `oldM`/`nowM` are the two sets' manifests (`oldM` null: unreadable — the tile
 * version folder is still found by its id).
 */
export async function prune(fs: ReleaseFs, old: Held | null, oldM: Manifest | null, now: Held,
                            nowM: Manifest): Promise<void> {
  if (!old) return;
  if (old.release && old.release !== now.release) await fs.remove(dirOf(old.release));
  else if (old.manifest && old.manifest !== now.manifest) await fs.remove(old.manifest);
  const keep = bigPaths(nowM);
  const kept = (p: string) => keep.some((k) => k === p || k.startsWith(`${p}/`) || p.startsWith(`${k}/`));
  for (const p of bigPaths(oldM)) if (!kept(p)) await fs.remove(p);
  if (old.tiles && old.tiles !== now.tiles) {
    const folder = `release/tiles/${old.tiles}`;
    if (!kept(folder)) await fs.remove(folder);
  }
}

/** The manifest the held set was installed from (written by `install`), so a launch with no network can open it. */
export async function heldManifest(fs: ReleaseFs, h: Held): Promise<Manifest> {
  return parseManifest(JSON.parse(utf8(await fs.read(h.manifest ?? `${dirOf(h.release!)}/manifest.json`))));
}

/** The on-disk path of a held file. */
export function heldPath(h: Held, url: string): string {
  return `${dirOf(h.release!)}/${url.split("/").pop()!.replace(/\.gz$/, "")}`;
}
