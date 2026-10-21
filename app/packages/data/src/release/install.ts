/**
 * ALWAYS OFFLINE ON NATIVE (P11, MINIMAL-PACKAGING §5): install a release, update it, swap atomically. Platform-free over
 * an injected file system (install.native.ts adapts expo-file-system), so a test can crash it anywhere.
 *
 *   layout     release/<data_release>/<file>       the data files, raw (sha-verified)
 *              release/tiles/<tile version>/…       the tiles, mask and map assets (only when the version moves)
 *              release/current                      THE POINTER: {release, files: {name: sha}, tiles, assets, basemap}
 *   install    every manifest file → release/<r>/, each written to `.part`, verified, then renamed; a file already there
 *              with the right sha is kept (resume at file granularity).
 *   update     diff (manifest.ts) against the held pointer: changed files are downloaded, unchanged ones COPIED from the
 *              held set (never re-downloaded); everything verified; then the pointer is replaced by ONE rename. A crash
 *              or a failed sha before that rename leaves the old pointer — the old set — in force.
 *   prune      after the new set has opened and paired, the old release folder is deleted (never before).
 */
import { diff, parseManifest, type Held, type Manifest } from "../manifest";
// Hermes has no TextDecoder (regs/blob.ts); TextEncoder it has
import { utf8 } from "../regs/blob";
import { Sha256, sha256Hex } from "../sha256";
import { VerifyError } from "../fetchVerified";

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
                          onBytes?: (n: number) => void) => Promise<void>;
/** What `rangedDownload` needs of a file system: a `.part` it can grow, and read back a bounded chunk at a time. */
export interface PartFs {
  /** Bytes already in `path` (0 when it is absent). */
  size(path: string): Promise<number>;
  append(path: string, bytes: Uint8Array): Promise<void>;
  /** Feed the whole file to `each`, a bounded chunk at a time (never the whole file in memory). */
  chunks(path: string, each: (bytes: Uint8Array) => void): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  remove(path: string): Promise<void>;
}

/**
 * THE RESUMABLE DOWNLOAD (MINIMAL-PACKAGING §5): HTTP Range requests of `chunk` bytes appended to `${path}.part`. The
 * `.part` IS the resume state — a crash or a dropped connection leaves it, and the next attempt asks for the bytes after
 * its length — so nothing else has to be saved. When it is whole it is hashed off disk (`Sha256`, chunk by chunk) and
 * renamed onto `path`; bytes that are not the manifest's drop the `.part`. A server that answers a range with anything
 * but 206 is refused: a 200 is the whole file, which a phone must not take into memory.
 */
export function rangedDownload(fs: PartFs, fetchImpl: typeof fetch = fetch, chunk = 8 * 1024 * 1024): DownloadTo {
  return async (url, path, e, onBytes) => {
    const part = `${path}.part`;
    let have = await fs.size(part);
    if (have > e.bytes) { await fs.remove(part); have = 0; }
    while (have < e.bytes) {
      const end = Math.min(have + chunk, e.bytes) - 1;
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
    const h = new Sha256();
    await fs.chunks(part, (b) => { h.update(b); });
    const got = h.hex();
    if (got !== e.sha256) {
      await fs.remove(part);
      throw new VerifyError(`${url}: sha256 ${got.slice(0, 12)}…, the manifest says ${e.sha256.slice(0, 12)}…`);
    }
    await fs.rename(part, path);
  };
}

/** `done`/`total` count files; `bytes`/`of` count what is fetched (copies from the held set cost nothing). */
export interface Progress { done: number; total: number; file: string; bytes: number; of: number }

export const POINTER = "release/current";

export async function heldFrom(fs: ReleaseFs): Promise<Held | null> {
  if (!(await fs.exists(POINTER))) return null;
  return JSON.parse(utf8(await fs.read(POINTER))) as Held;
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

function pointerFor(m: Manifest): Held {
  return { release: m.data_release, tiles: m.tiles.current?.id ?? null, basemap: m.basemap?.id ?? null,
           assets: m.tiles.assets?.id ?? null,
           files: Object.fromEntries(Object.entries(m.files).map(([k, v]) => [k, v.sha256])) };
}

/**
 * Download what `m` needs beside `held` (everything when `held` is null) and swap the pointer. `toDisk` streams the
 * tiles and their mask (a test or the web may leave it out: they are then fetched whole through `dl`).
 */
export async function install(m: Manifest, fs: ReleaseFs, dl: Download, base: string, held: Held | null = null,
                              onProgress: (p: Progress) => void = () => {}, toDisk?: DownloadTo): Promise<Held> {
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
  for (const b of big) {
    if (toDisk) {
      const at = bytes;
      await toDisk(`${base}${b.url}`, b.path, b.e, (n) => onProgress({ done, total, file: b.file, bytes: at + n, of }));
    } else {
      await fetchInto(fs, dl, `${base}${b.url}`, b.path, b.e);
    }
    bytes += b.e.bytes;
    onProgress({ done: ++done, total, file: b.file, bytes, of });
  }
  // the manifest the set was installed from: an offline launch opens the set with it (`heldManifest`)
  await place(fs, `${dir}/manifest.json`, new TextEncoder().encode(JSON.stringify(m)));
  const next = pointerFor(m);
  await place(fs, POINTER, new TextEncoder().encode(JSON.stringify(next)));      // THE SWAP
  return next;
}

/** After the new set has opened and paired: delete the release folder it replaced. */
export async function prune(fs: ReleaseFs, old: Held | null, now: Held): Promise<void> {
  if (old?.release && old.release !== now.release) await fs.remove(dirOf(old.release));
}

/** The manifest the held set was installed from (written by `install`), so a launch with no network can open it. */
export async function heldManifest(fs: ReleaseFs, h: Held): Promise<Manifest> {
  return parseManifest(JSON.parse(utf8(await fs.read(`${dirOf(h.release!)}/manifest.json`))));
}

/** The on-disk path of a held file. */
export function heldPath(h: Held, url: string): string {
  return `${dirOf(h.release!)}/${url.split("/").pop()!.replace(/\.gz$/, "")}`;
}
