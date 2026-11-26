/**
 * A spot's photographs in a browser: blobs in IndexedDB, drawn through object URLs (S1).
 *
 * IndexedDB because localStorage holds strings of a few megabytes in all, and one photograph is that. A blob is kept
 * under the same relative reference a phone uses ("photos/<spot id>-<n>.<ext>"), so a spot reads the same on both.
 *
 * DOWNSCALED BEFORE IT IS KEPT, to `PHOTO_MAX_EDGE` on the long side, where the browser can do it cheaply
 * (`createImageBitmap` + a canvas). Where it cannot, or anything fails, the original is kept as it is: a big photo is
 * better than a lost one. The rules are `./photos`'s.
 */
import { clearedOr, done, keptConnection } from "./idbCleared";
import { openPhotosWith, PHOTO_MAX_EDGE, type PhotoFiles, type PhotoStore, type PickedPhoto } from "./photos";

const DB = "canifishthis.photos", STORE = "photos";

const URLS = new WeakMap<IDBFactory, Map<string, string>>();
const urlsOf = (idb: IDBFactory): Map<string, string> => {
  let m = URLS.get(idb);
  if (!m) URLS.set(idb, (m = new Map()));
  return m;
};

/** Bytes by reference in one IndexedDB object store. A browser with no IndexedDB says so when it is used. */
export function photoFiles(idb: IDBFactory | undefined = globalThis.indexedDB): PhotoFiles {
  // One object URL per photo for as long as the page is open; revoked when the photo is deleted. Shared by every
  // instance over the same IndexedDB: the app draws through one and the spots store deletes through another, and a
  // delete there must revoke the URL drawn here and stop handing it out (T80 1).
  const urls = idb ? urlsOf(idb) : new Map<string, string>();
  // (T1092) the browser cleared its data: no picture drawn from a cleared photo is kept as if saved
  const conn = keptConnection({ idb, name: DB, none: () => new Error("This browser is not letting the app store photos."),
    setup: (d) => { if (!d.objectStoreNames.contains(STORE)) d.createObjectStore(STORE); },
    onCleared: () => { for (const u of urls.values()) URL.revokeObjectURL(u); urls.clear(); } });
  const write = <T>(job: (s: IDBObjectStore) => IDBRequest<T>): Promise<void> => conn.run(async (d) => {
    const tx = d.transaction(STORE, "readwrite");
    const committed = new Promise<void>((res, rej) => {
      tx.oncomplete = () => res();
      tx.onerror = () => rej(clearedOr(tx.error ?? new Error("IndexedDB refused the write")));
      tx.onabort = () => rej(clearedOr(tx.error ?? new Error("IndexedDB aborted the write")));
    });
    await Promise.all([done(job(tx.objectStore(STORE))), committed]);
  });

  return {
    async keep(ref, picked) {
      const blob = await bytesOf(picked);
      await write((s) => s.put(blob, ref));
      const old = urls.get(ref);
      if (old) { URL.revokeObjectURL(old); urls.delete(ref); }
    },
    async drop(ref) {
      await write((s) => s.delete(ref));
      const old = urls.get(ref);
      if (old) { URL.revokeObjectURL(old); urls.delete(ref); }
    },
    async uri(ref) {
      const had = urls.get(ref);
      if (had) return had;
      const blob = await conn.run((d) => done(d.transaction(STORE, "readonly").objectStore(STORE).get(ref))) as Blob | undefined;
      if (!blob) return null;
      const url = URL.createObjectURL(blob);
      urls.set(ref, url);
      return url;
    },
  };
}

async function bytesOf(picked: PickedPhoto): Promise<Blob> {
  if (picked.blob) return downscale(picked.blob);
  if (picked.uri) return downscale(await (await fetch(picked.uri)).blob());
  throw new Error("the picker returned no file");
}

/**
 * The image at most `max` px on its long edge, in its own type (so the reference's extension stays true); the original
 * wherever that cannot be done cheaply. A canvas that cannot write the type (HEIC) hands back null, and that is the
 * original too.
 */
async function downscale(blob: Blob, max = PHOTO_MAX_EDGE): Promise<Blob> {
  try {
    if (typeof createImageBitmap !== "function" || typeof document === "undefined") return blob;
    const bmp = await createImageBitmap(blob);
    const long = Math.max(bmp.width, bmp.height);
    if (long <= max) { bmp.close(); return blob; }
    const k = max / long;
    const canvas = document.createElement("canvas");
    canvas.width = Math.round(bmp.width * k);
    canvas.height = Math.round(bmp.height * k);
    const ctx = canvas.getContext("2d");
    if (!ctx) { bmp.close(); return blob; }
    ctx.drawImage(bmp, 0, 0, canvas.width, canvas.height);
    bmp.close();
    const type = blob.type || "image/jpeg";
    return await new Promise<Blob>((res) =>
      canvas.toBlob((b) => res(b && b.type === type ? b : blob), type, 0.85));
  } catch {
    return blob;
  }
}

/** `stamp` is for a test that pins exact references; the app leaves it to the clock. */
export function openPhotos(stamp?: () => string): PhotoStore {
  return openPhotosWith(photoFiles(), stamp);
}
