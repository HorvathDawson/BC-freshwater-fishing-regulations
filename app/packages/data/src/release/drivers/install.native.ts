/**
 * THE PHONE'S INSTALL ROOT (P11, MINIMAL-PACKAGING §5): expo-file-system under `documentDirectory`, adapted to the
 * platform-free `ReleaseFs` / `PartFs` that `install` and `rangedDownload` are written (and tested) against. See
 * `install.d.ts`.
 *
 * Every path is relative to the document directory: `release/<data_release>/<file>`, `release/tiles/<version>/…`,
 * `release/current` (the pointer). Writes go to `<path>.part` and are renamed into place by the caller.
 *
 * NOT VERIFIED ON A DEVICE (N1/N2 pending): the expo-file-system calls below are typed against SDK 57 and unexercised.
 */
import { Directory, File, FileMode, Paths } from "expo-file-system";
import { fetchVerified } from "../../fetchVerified";
import { rangedDownload, type PartFs, type ReleaseFs } from "../install";
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
};

export function device(fetchImpl: typeof fetch = fetch): Device | null {
  return {
    fs,
    download: (url, expect) => fetchVerified(url, expect, undefined, fetchImpl),
    toDisk: rangedDownload(fs, fetchImpl),
    dirUri: (path) => dirAt(path).uri,
  };
}
