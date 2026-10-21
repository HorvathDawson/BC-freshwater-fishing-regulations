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
import { openPhotosWith, PHOTO_MAX_EDGE, type PhotoFiles, type PhotoStore, type PickedPhoto } from "./photos";

const DB = "canifishthis.photos", STORE = "photos";

const done = <T>(r: IDBRequest<T>): Promise<T> => new Promise((res, rej) => {
  r.onsuccess = () => res(r.result);
  r.onerror = () => rej(r.error ?? new Error("IndexedDB refused the request"));
});

/** Bytes by reference in one IndexedDB object store. A browser with no IndexedDB says so when it is used. */
export function photoFiles(idb: IDBFactory | undefined = globalThis.indexedDB): PhotoFiles {
  let db: Promise<IDBDatabase> | null = null;
  const open = (): Promise<IDBDatabase> => (db ??= (() => {
    if (!idb) return Promise.reject(new Error("This browser is not letting the app store photos."));
    const r = idb.open(DB, 1);
    r.onupgradeneeded = () => {
      if (!r.result.objectStoreNames.contains(STORE)) r.result.createObjectStore(STORE);
    };
    return done(r);
  })().catch((e) => { db = null; throw e; }));
  const write = async <T>(job: (s: IDBObjectStore) => IDBRequest<T>): Promise<void> => {
    const tx = (await open()).transaction(STORE, "readwrite");
    const committed = new Promise<void>((res, rej) => {
      tx.oncomplete = () => res();
      tx.onerror = () => rej(tx.error ?? new Error("IndexedDB refused the write"));
      tx.onabort = () => rej(tx.error ?? new Error("IndexedDB aborted the write"));
    });
    await done(job(tx.objectStore(STORE)));
    await committed;
  };
  // One object URL per photo for as long as the page is open; revoked when the photo is deleted.
  const urls = new Map<string, string>();

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
      const tx = (await open()).transaction(STORE, "readonly");
      const blob = await done(tx.objectStore(STORE).get(ref)) as Blob | undefined;
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
