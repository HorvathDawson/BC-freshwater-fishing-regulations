/**
 * THE PHONE'S INSTALL ROOT (P11, MINIMAL-PACKAGING §5): expo-file-system under `documentDirectory`, adapted to the
 * platform-free `ReleaseFs` / `PartFs` that `install` and `rangedDownload` are written (and tested) against. See
 * `install.d.ts`.
 *
 * Every path is relative to the document directory: `release/<data_release>/<file>`, `release/tiles/<version>/…`,
 * `release/current` (the pointer). Writes go to `<path>.part` and are renamed into place by the caller.
 *
 * (T262) Verified on the Android emulator by the T255 test bed: `install` (a clean first run, and one killed mid-download
 * and resumed) and `big-update`.
 */
import { Directory, DownloadTask, File, FileMode, Paths } from "expo-file-system";
import { VerifyError } from "../../fetchVerified";
import { fetchVerifiedResilient } from "../../net/verified";
import { partDownload, type PartFill, type PartFs, type ReleaseFs } from "../install";
import { background, type BackgroundNative } from "../bgDownload";
import type { Device } from "./install";

export type { Device } from "./install";

/** Read a file back a megabyte at a time: the tiles are verified off disk, never held whole. */
const CHUNK = 1 << 20;

const fileAt = (path: string) => new File(Paths.document, path);
const dirAt = (path: string) => new Directory(Paths.document, path);
const parentOf = (path: string) => path.split("/").slice(0, -1).join("/");
const ensureParent = (path: string) => {
  const parent = parentOf(path);
  if (parent) {
    const d = dirAt(parent);
    if (!d.exists) d.create({ intermediates: true });
  }
};

const fs: ReleaseFs & PartFs = {
  exists: async (path) => fileAt(path).exists || dirAt(path).exists,
  read: async (path) => fileAt(path).bytes(),
  write: async (path, bytes) => {
    ensureParent(path);
    const f = fileAt(path);
    if (f.exists) f.delete();
    f.create();
    f.write(bytes);
  },
  // expo-file-system's move refuses an existing destination: replace it explicitly (the pointer swap is this rename)
  rename: async (from, to) => {
    ensureParent(to);
    const dest = fileAt(to);
    if (dest.exists) dest.delete();
    fileAt(from).move(dest);
  },
  remove: async (path) => {
    const f = fileAt(path);
    if (f.exists) { f.delete(); return; }
    const d = dirAt(path);
    if (d.exists) d.delete();
  },
  size: async (path) => { const f = fileAt(path); return f.exists ? f.size : 0; },
  append: async (path, bytes) => {
    ensureParent(path);
    const f = fileAt(path);
    if (!f.exists) f.create();
    const h = f.open(FileMode.ReadWrite);
    try {
      h.offset = h.size ?? 0;
      h.writeBytes(bytes);
    } finally {
      h.close();
    }
  },
  chunks: async (path, each) => {
    const h = fileAt(path).open(FileMode.ReadOnly);
    try {
      const size = h.size ?? 0;
      for (let at = 0; at < size; at += CHUNK) each(h.readBytes(Math.min(CHUNK, size - at)));
    } finally {
      h.close();
    }
  },
  // (T272) the follower's read: the bytes the native download has put on disk so far, a megabyte at a time
  readAt: async (path, offset, length) => {
    const h = fileAt(path).open(FileMode.ReadOnly);
    try {
      const size = h.size ?? 0;
      h.offset = offset;
      return h.readBytes(Math.max(0, Math.min(length, size - offset)));
    } finally {
      h.close();
    }
  },
};

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Append `from` to `to` a chunk at a time, then delete `from` (a resumed download's tail onto its `.part`). */
function appendFile(from: string, to: string): void {
  const src = fileAt(from).open(FileMode.ReadOnly), dst = fileAt(to).open(FileMode.ReadWrite);
  try {
    const size = src.size ?? 0;
    dst.offset = dst.size ?? 0;
    for (let at = 0; at < size; at += CHUNK) dst.writeBytes(src.readBytes(Math.min(CHUNK, size - at)));
  } finally {
    src.close(); dst.close();
  }
  fileAt(from).delete();
}

/**
 * (T262) THE PHONE'S TRANSPORT: the `.part` is filled by expo-file-system's native DownloadTask (OkHttp / URLSession
 * straight to disk), so no body passes through JS — expo's fetch held each 8 MB range body on the Java heap until a JS
 * GC, and a first install filled Android's 192 MB heap and crash-looped. A `.part` already there is resumed: the rest
 * (`Range: bytes=<have>-`) downloads natively to `<part>.tail` and is appended a megabyte at a time (a server that sent
 * the whole file instead replaces the `.part`). A dropped line or a DNS miss is tried again (from what is on disk)
 * before it is thrown; the `.part` stays for the next launch either way.
 */
const nativeFill = (tries = 4): PartFill => async (url, part, _have, total, onBytes, onRewrite) => {
  let last: unknown = null;
  for (let i = 0; i < tries; i++) {
    // (T272) a retry: the hash that followed the `.part` is not trusted (it is hashed off disk once it is whole)
    if (i > 0) { onRewrite?.(); await wait(1500 * 2 ** (i - 1)); }
    ensureParent(part);
    const f = fileAt(part);
    const have = f.exists ? f.size : 0;
    if (have >= total) return;
    console.info(`[download] ${url}: attempt ${i + 1}, ${have} of ${total} bytes on disk`);
    const tail = fileAt(`${part}.tail`);
    if (tail.exists) tail.delete();
    const to = have > 0 ? tail : f;
    // a Range header on a fresh start too: it keeps the client from asking for (and inflating) a gzip body
    const task = new DownloadTask(url, to, { headers: { Range: `bytes=${have}-` },
                                             onProgress: (p) => onBytes?.(have + p.bytesWritten) });
    try {
      const done = await task.downloadAsync();
      if (!done) throw new Error(`${url}: the download stopped`);
      if (have > 0) {
        const got = tail.size;
        if (got === total) { onRewrite?.(); f.delete(); tail.move(f); }        // the server sent it whole
        else if (got === total - have) appendFile(`${part}.tail`, part);
        else { tail.delete(); throw new Error(`${url}: ${got} bytes for the ${total - have} after ${have}`); }
      }
      console.info(`[download] ${url}: done, ${fileAt(part).size} bytes`);
      return;
    } catch (err) {
      if (err instanceof VerifyError) throw err;
      console.warn(`[download] ${url}: attempt ${i + 1} failed: ${err instanceof Error ? err.message : String(err)}`);
      last = err;
    } finally {
      try { task.release(); } catch { /* already released */ }
    }
  }
  throw last instanceof Error ? last : new Error(String(last));
};

/**
 * `bg`: (T298) the system's downloader (Android's DownloadManager, `apps/mobile/modules/bg-download`) — the big files
 * download in its process (on with the app closed, its own notification) and are hashed natively. Absent (iOS, a client
 * built before the module): `nativeFill` as before, which stops when the app does and resumes from the `.part`.
 */
export function device(fetchImpl: typeof fetch = fetch, bg?: BackgroundNative | null): Device | null {
  const sys = bg ? background(bg, { ...fs, uri: (p) => fileAt(p).uri }, nativeFill()) : null;
  const partFs: PartFs = bg ? { ...fs, hash: (p) => bg.sha256(fileAt(p).uri) } : fs;
  return {
    fs,
    // (T256 item 4) the data files of a first install over a poor line: timeouts by size, retries, resume (the big
    // files are the native transport's, `nativeFill`, which already retries and resumes)
    download: (url, expect) => fetchVerifiedResilient(url, expect, { fetchImpl }),
    toDisk: partDownload(partFs, sys ? sys.fill : nativeFill()),
    ...(sys ? { background: { queue: sys.queue } } : {}),
    dirUri: (path) => dirAt(path).uri,
    fileUri: (path) => fileAt(path).uri,
  };
}
