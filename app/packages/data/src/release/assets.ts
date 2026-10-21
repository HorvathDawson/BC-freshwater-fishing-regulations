/**
 * THE MAP ASSETS ON A PHONE (P11, P18 owner Q18): the glyphs and sprites the style names, installed beside the tiles so
 * the always-offline app draws its labels and icons with no network. Written by `pipeline/deliver/tiles/assets.py`:
 *
 *   release/tiles/<tile version>/map-assets-<n>/
 *       fonts/<fontstack>/<start>-<end>.pbf, sprites/v4/<light|dark>[@2x].{json,png}
 *       assets.json      {id, pin, files: {path: sha256}}
 *
 * The manifest names the folder (`tiles.assets.url`) and ONE digest over its file list (`files_sha256`: Python's
 * `sha256(json.dumps(files, sort_keys=True))`, `assetsDigest`). So `assets.json` is held to that digest, every file to
 * its own sha256 in it, and nothing unverified is placed. A file already there with the right sha is kept (resume), and
 * one the held folder has with the right sha is copied (a tile release whose assets did not change fetches nothing).
 */
import { ungzip, VerifyError } from "../fetchVerified";
import { utf8 } from "../regs/blob";
import { sha256Hex } from "../sha256";
import type { Manifest } from "../manifest";
import type { ReleaseFs } from "./install";

/** The raw bytes at a url (gzip decoded), or throw: nothing is verified here — the caller holds them to a digest. */
export type RawFetch = (url: string) => Promise<Uint8Array>;

export type AssetsBlock = NonNullable<Manifest["tiles"]["assets"]>;

/** `fetch` as a `RawFetch`: a body that is not 2xx throws. */
export function rawFetch(fetchImpl: typeof fetch = fetch): RawFetch {
  return async (url) => {
    const res = await fetchImpl(url);
    if (!res.ok) throw new VerifyError(`${url}: HTTP ${res.status}`);
    return ungzip(new Uint8Array(await res.arrayBuffer()));
  };
}

/** Python's `json.dumps(s)` with ensure_ascii: JSON.stringify, then every code unit outside printable ASCII escaped. */
const pyString = (s: string) =>
  JSON.stringify(s).replace(/[^\x20-\x7e]/g, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);

/** The manifest's `tiles.assets.files_sha256` of a file list (`assets.py` `files_sha256`), byte for byte. */
export function assetsDigest(files: Readonly<Record<string, string>>): string {
  const keys = Object.keys(files).sort();
  const json = `{${keys.map((k) => `${pyString(k)}: ${pyString(files[k]!)}`).join(", ")}}`;
  return sha256Hex(new TextEncoder().encode(json));
}

/** The on-disk folder of an assets block (`release/<url>`, ending in "/"). */
export const assetsDir = (a: Pick<AssetsBlock, "url">) => `release/${a.url.replace(/\/?$/, "/")}`;

const encodePath = (rel: string) => rel.split("/").map(encodeURIComponent).join("/");

async function place(fs: ReleaseFs, path: string, bytes: Uint8Array): Promise<void> {
  await fs.write(`${path}.part`, bytes);
  await fs.rename(`${path}.part`, path);
}

const hasSha = async (fs: ReleaseFs, path: string, sha: string) =>
  await fs.exists(path) && sha256Hex(await fs.read(path)) === sha;

/**
 * Install the assets folder `a` names into `assetsDir(a)`, every byte verified. `from`: the held set's assets folder
 * (files with the same sha are copied from it). Throws (a VerifyError naming the file) on any bytes that are not the
 * manifest's; what was placed before stays, verified, for the next attempt.
 */
export async function installAssets(fs: ReleaseFs, raw: RawFetch, base: string, a: AssetsBlock,
                                    from: string | null = null): Promise<void> {
  if (!a.files_sha256) throw new VerifyError(`assets ${a.id ?? a.url}: the manifest names no files_sha256`);
  const dir = assetsDir(a), src = `${base}${a.url.replace(/\/?$/, "/")}`;
  const listPath = `${dir}assets.json`;
  const filesOf = (b: Uint8Array) => (JSON.parse(utf8(b)) as { files?: Record<string, string> }).files ?? {};
  const holds = (b: Uint8Array | null): b is Uint8Array => {
    try { return b !== null && assetsDigest(filesOf(b)) === a.files_sha256; } catch { return false; }
  };
  const onDisk = await fs.exists(listPath) ? await fs.read(listPath) : null;
  let listBytes: Uint8Array;
  if (holds(onDisk)) listBytes = onDisk;
  else {
    listBytes = await raw(`${src}assets.json`);
    let got: string;
    try { got = assetsDigest(filesOf(listBytes)); } catch { got = "(unreadable)"; }
    if (got !== a.files_sha256)
      throw new VerifyError(`${src}assets.json: files ${got.slice(0, 12)}…, the manifest says ${a.files_sha256.slice(0, 12)}…`);
  }
  const files = filesOf(listBytes);
  for (const rel of Object.keys(files).sort()) {
    const sha = files[rel]!, path = `${dir}${rel}`;
    if (await hasSha(fs, path, sha)) continue;                                  // resumed: already there
    const held = from ? `${from.replace(/\/?$/, "/")}${rel}` : null;
    if (held && held !== path && await hasSha(fs, held, sha)) { await place(fs, path, await fs.read(held)); continue; }
    const url = `${src}${encodePath(rel)}`, b = await raw(url), got = sha256Hex(b);
    if (got !== sha) throw new VerifyError(`${url}: sha256 ${got.slice(0, 12)}…, assets.json says ${sha.slice(0, 12)}…`);
    await place(fs, path, b);
  }
  // the list last: a folder with its assets.json is whole
  await place(fs, listPath, listBytes);
}
