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
import { diff, type Held, type Manifest } from "../manifest";
import { sha256Hex } from "../sha256";

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
export interface Progress { done: number; total: number; file: string }

export const POINTER = "release/current";

export async function heldFrom(fs: ReleaseFs): Promise<Held | null> {
  if (!(await fs.exists(POINTER))) return null;
  return JSON.parse(new TextDecoder().decode(await fs.read(POINTER))) as Held;
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

/** Download what `m` needs beside `held` (everything when `held` is null) and swap the pointer. */
export async function install(m: Manifest, fs: ReleaseFs, dl: Download, base: string, held: Held | null = null,
                              onProgress: (p: Progress) => void = () => {}): Promise<Held> {
  const d = held ? diff(held, m) : { files: Object.keys(m.files).sort(), tiles: true, basemap: true, assets: true };
  const names = Object.keys(m.files).sort();
  const dir = dirOf(m.data_release);
  let done = 0;
  for (const name of names) {
    const e = m.files[name]!, path = `${dir}/${e.url.split("/").pop()!.replace(/\.gz$/, "")}`;
    if (!d.files.includes(name) && held?.release) {
      // unchanged: copy from the held set (never a download)
      const from = `${dirOf(held.release)}/${path.split("/").pop()}`;
      if (!(await fs.exists(path))) await place(fs, path, await fs.read(from));
    } else {
      await fetchInto(fs, dl, `${base}${e.url}`, path, e);
    }
    onProgress({ done: ++done, total: names.length, file: name });
  }
  // every file of the new set, re-verified on disk before anything points at it
  for (const name of names) {
    const e = m.files[name]!, path = `${dir}/${e.url.split("/").pop()!.replace(/\.gz$/, "")}`;
    if (sha256Hex(await fs.read(path)) !== e.sha256) throw new Error(`release: ${path} does not match the manifest`);
  }
  if (d.tiles && m.tiles.current && m.tiles.current.sha256 && m.tiles.current.bytes) {
    const t = m.tiles.current;
    await fetchInto(fs, dl, `${base}${t.url}`, `release/${t.url}`, { bytes: t.bytes!, sha256: t.sha256! });
  }
  const next = pointerFor(m);
  await place(fs, POINTER, new TextEncoder().encode(JSON.stringify(next)));      // THE SWAP
  return next;
}

/** After the new set has opened and paired: delete the release folder it replaced. */
export async function prune(fs: ReleaseFs, old: Held | null, now: Held): Promise<void> {
  if (old?.release && old.release !== now.release) await fs.remove(dirOf(old.release));
}

/** The on-disk path of a held file. */
export function heldPath(h: Held, url: string): string {
  return `${dirOf(h.release!)}/${url.split("/").pop()!.replace(/\.gz$/, "")}`;
}
