/**
 * S1: a spot's photographs. Added, removed, and deleted with their spot — on both stores: the phone's files (over a
 * fake expo-file-system) and the browser's IndexedDB (over a fake IndexedDB). And what is persisted on the spot is a
 * RELATIVE reference, with no section id anywhere.
 */
import { describe, expect, it, vi } from "vitest";
import * as efs from "expo-file-system";
import { nextPhotoRef, openPhotosWith, photoExt, photosDropped, type PhotoFiles } from "./photos";
import { photoFiles as nativePhotoFiles, openPhotos as openNativePhotos } from "./photoStore.native";
import { photoFiles as idbPhotoFiles, openPhotos as openWebPhotos } from "./photoStore.web";

/** No clock stamp, so a reference can be pinned exactly. */
const PINNED = () => "";
import { openSpotsWith, type SpotsFs } from "./core";
import type { Spot } from "./model";

// ---- a fake expo-file-system: a map of file URI -> contents ------------------------------------------------------
vi.mock("expo-file-system", () => {
  const disk = new Map<string, string>();
  const dirs = new Set<string>();
  const join = (parts: unknown[]) => parts
    .map((p) => (typeof p === "string" ? p : (p as { uri: string }).uri))
    .map((s, i) => (i === 0 ? s.replace(/\/+$/, "") : s.replace(/^\/+|\/+$/g, "")))
    .join("/");
  class Directory {
    uri: string;
    constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return dirs.has(this.uri); }
    create() { dirs.add(this.uri); }
  }
  class File {
    uri: string;
    constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return disk.has(this.uri); }
    delete() { if (!disk.delete(this.uri)) throw new Error(`no ${this.uri}`); }
    async copy(to: File) {
      const v = disk.get(this.uri);
      if (v === undefined) throw new Error(`no ${this.uri}`);
      if (disk.has(to.uri)) throw new Error("DestinationAlreadyExists");
      disk.set(to.uri, v);
    }
  }
  return { Directory, File, Paths: { document: new Directory("file:///doc") }, __disk: disk };
});
const disk = (efs as unknown as { __disk: Map<string, string> }).__disk;

// ---- a fake IndexedDB: just the calls idbPhotoFiles makes ----------------------------------------------------------
function fakeIdb() {
  const data = new Map<string, Map<IDBValidKey, unknown>>();
  const later = (f: () => void) => queueMicrotask(f);
  const request = <T>(fn: () => T, after?: () => void) => {
    const r = {} as { result?: T; onsuccess?: () => void; onerror?: () => void; error?: unknown };
    later(() => { r.result = fn(); r.onsuccess?.(); after?.(); });
    return r;
  };
  const db = {
    objectStoreNames: { contains: (s: string) => data.has(s) },
    createObjectStore: (s: string) => { data.set(s, new Map()); },
    transaction: (name: string) => {
      const tx = {} as { oncomplete?: () => void; objectStore: (n: string) => unknown };
      const done = () => later(() => tx.oncomplete?.());
      tx.objectStore = (n: string) => ({
        get: (k: IDBValidKey) => request(() => data.get(n)!.get(k)),
        put: (v: unknown, k: IDBValidKey) => request(() => { data.get(n)!.set(k, v); return k; }, done),
        delete: (k: IDBValidKey) => request(() => { data.get(n)!.delete(k); }, done),
      });
      void name;
      return tx;
    },
  };
  const factory = {
    open: () => {
      const r = {} as { result?: unknown; onupgradeneeded?: () => void; onsuccess?: () => void };
      later(() => { r.result = db; r.onupgradeneeded?.(); r.onsuccess?.(); });
      return r;
    },
  };
  return { factory: factory as unknown as IDBFactory, data };
}

// ---- the store core, with an in-memory spots file ------------------------------------------------------------------
function memFs(): SpotsFs & { files: Map<string, string> } {
  const files = new Map<string, string>();
  return {
    files,
    exists: (n) => files.has(n),
    read: (n) => { const t = files.get(n); if (t === undefined) throw new Error(`no ${n}`); return t; },
    write: (n, t) => { files.set(n, t); },
    rename: (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: (n) => { files.delete(n); },
  };
}

const spot = (id: string, photos: string[] = []): Spot => ({
  id, createdAt: 1, updatedAt: 1, visitedAt: 1, lat: 49.0974, lon: -121.9675, item: "it_42" as never,
  waterName: "Chilliwack River", title: "", notes: "", photos,
  reading: null, weather: null, trace: null, panel: null,
});

describe("photo references", () => {
  it("names a photo after its spot, one past the highest number it already has", () => {
    expect(nextPhotoRef("spot_k1_ab", [], "jpg")).toBe("photos/spot_k1_ab-1.jpg");
    expect(nextPhotoRef("spot_k1_ab", ["photos/spot_k1_ab-1.jpg", "photos/spot_k1_ab-3.png"], "jpg"))
      .toBe("photos/spot_k1_ab-4.jpg");
  });

  it("never reuses a removed photo's number: its file may still be held by a discarded edit", () => {
    expect(nextPhotoRef("s", ["photos/s-2.jpg"], "jpg")).toBe("photos/s-3.jpg");
  });

  it("stamps every new photo, so a number given back by a delete is never the same URI twice (image caches)", () => {
    expect(nextPhotoRef("s", ["photos/s-1-a1.jpg"], "jpg", "b2")).toBe("photos/s-2-b2.jpg");
    expect(nextPhotoRef("s", ["photos/s-1-a1.jpg"], "jpg", "c3")).not.toBe(nextPhotoRef("s", ["photos/s-1-a1.jpg"], "jpg", "b2"));
  });

  it("keeps a photo under its own type, then its name, then jpg", () => {
    expect(photoExt({ mimeType: "image/png" })).toBe("png");
    expect(photoExt({ fileName: "IMG_0001.HEIC" })).toBe("heic");
    expect(photoExt({ uri: "file:///cache/ImagePicker/x.jpeg?x=1" })).toBe("jpg");
    expect(photoExt({ uri: "content://media/42" })).toBe("jpg");
  });

  it("drops only what a save no longer has, and only its own references", () => {
    expect(photosDropped(["photos/s-1.jpg", "photos/s-2.jpg", "file:///old.jpg"], ["photos/s-2.jpg"]))
      .toEqual(["photos/s-1.jpg"]);
  });
});

describe("the photo store over a platform seam", () => {
  const files = (): PhotoFiles & { kept: Map<string, string> } => {
    const kept = new Map<string, string>();
    return {
      kept,
      keep: async (ref, p) => { kept.set(ref, p.uri ?? "blob"); },
      drop: async (ref) => { kept.delete(ref); },
      uri: async (ref) => (kept.has(ref) ? `file:///doc/spots/${ref}` : null),
    };
  };

  it("adds, draws and removes a photo", async () => {
    const f = files(), store = openPhotosWith(f, PINNED);
    const ref = await store.add("s1", { uri: "file:///cache/a.jpg", mimeType: "image/jpeg" }, []);
    expect(ref).toBe("photos/s1-1.jpg");
    expect(await store.uri(ref)).toBe("file:///doc/spots/photos/s1-1.jpg");
    await store.remove(ref);
    expect(await store.uri(ref)).toBeNull();
    expect([...f.kept.keys()]).toEqual([]);
  });
});

describe("on a phone: files in documentDirectory/spots/photos (fake expo-file-system)", () => {
  it("copies the picked file out of the picker's cache, so clearing the cache keeps the photo", async () => {
    disk.clear();
    disk.set("file:///cache/ImagePicker/abc.jpg", "JPEG BYTES");
    const store = openNativePhotos(PINNED);
    const ref = await store.add("s1", { uri: "file:///cache/ImagePicker/abc.jpg", mimeType: "image/jpeg" }, []);
    disk.delete("file:///cache/ImagePicker/abc.jpg");               // Android clears the picker's cache
    expect(ref).toBe("photos/s1-1.jpg");
    expect([...disk.entries()]).toEqual([["file:///doc/spots/photos/s1-1.jpg", "JPEG BYTES"]]);
    expect(await store.uri(ref)).toBe("file:///doc/spots/photos/s1-1.jpg");
  });

  it("removes a photo, and a photo already gone is not an error", async () => {
    disk.clear();
    disk.set("file:///cache/b.png", "PNG");
    const files = nativePhotoFiles();
    await files.keep("photos/s1-2.png", { uri: "file:///cache/b.png" });
    await files.drop("photos/s1-2.png");
    await files.drop("photos/s1-2.png");
    expect(disk.has("file:///doc/spots/photos/s1-2.png")).toBe(false);
    expect(await files.uri("photos/s1-2.png")).toBeNull();
  });

  it("deletes a spot's photos with the spot", async () => {
    disk.clear();
    disk.set("file:///cache/c.jpg", "C");
    const photos = openNativePhotos(PINNED);
    const store = openSpotsWith(memFs(), "s.json", Date.now, photos);
    const ref = await photos.add("s1", { uri: "file:///cache/c.jpg" }, []);
    await store.put(spot("s1", [ref]));
    await store.remove("s1");
    expect(disk.has("file:///doc/spots/photos/s1-1.jpg")).toBe(false);
  });
});

describe("in a browser: blobs in IndexedDB, drawn through object URLs (fake IndexedDB)", () => {
  it("keeps the chosen file's bytes under the reference and draws them from an object URL", async () => {
    const { factory, data } = fakeIdb();
    const store = openPhotosWith(idbPhotoFiles(factory), PINNED);
    const blob = new Blob(["PNG BYTES"], { type: "image/png" });
    const ref = await store.add("s1", { blob, mimeType: "image/png", fileName: "river.png" }, []);
    expect(ref).toBe("photos/s1-1.png");
    expect(await (data.get("photos")!.get(ref) as Blob).text()).toBe("PNG BYTES");
    expect(await store.uri(ref)).toMatch(/^blob:/);
  });

  it("removes a photo: gone from IndexedDB, and nothing to draw", async () => {
    const { factory, data } = fakeIdb();
    const store = openPhotosWith(idbPhotoFiles(factory), PINNED);
    const ref = await store.add("s1", { blob: new Blob(["x"], { type: "image/jpeg" }) }, []);
    await store.remove(ref);
    expect([...data.get("photos")!.keys()]).toEqual([]);
    expect(await store.uri(ref)).toBeNull();
  });

  it("deletes a spot's photos with the spot", async () => {
    const { factory, data } = fakeIdb();
    const photos = openPhotosWith(idbPhotoFiles(factory), PINNED);
    const store = openSpotsWith(memFs(), "s.json", Date.now, photos);
    const a = await photos.add("s1", { blob: new Blob(["a"], { type: "image/jpeg" }) }, []);
    const b = await photos.add("s2", { blob: new Blob(["b"], { type: "image/jpeg" }) }, []);
    await store.put(spot("s1", [a]));
    await store.put(spot("s2", [b]));
    await store.remove("s1");
    expect([...data.get("photos")!.keys()]).toEqual(["photos/s2-1.jpg"]);
  });

  it("a photo deleted through one store instance is revoked and gone in another over the same browser (T80 1)", async () => {
    // the app opens one instance to draw (App.tsx) and the spots store another to delete (store.web.ts)
    const { factory } = fakeIdb();
    const drawer = openPhotosWith(idbPhotoFiles(factory), PINNED);
    const deleter = openPhotosWith(idbPhotoFiles(factory), PINNED);
    const ref = await drawer.add("s1", { blob: new Blob(["x"], { type: "image/jpeg" }) }, []);
    const url = await drawer.uri(ref);
    const revoke = vi.spyOn(URL, "revokeObjectURL");
    try {
      await deleter.remove(ref);
      expect({ revoked: revoke.mock.calls, drawn: await drawer.uri(ref) }).toEqual({ revoked: [[url]], drawn: null });
    } finally {
      revoke.mockRestore();
    }
  });

  it("says so when the browser has no IndexedDB, rather than losing the photo quietly", async () => {
    const store = openPhotosWith(idbPhotoFiles(undefined), PINNED);
    await expect(store.add("s1", { blob: new Blob(["x"]) }, []))
      .rejects.toThrow("This browser is not letting the app store photos.");
  });

  it("the platform entry opens the browser's own IndexedDB", () => {
    expect(typeof openWebPhotos(PINNED).add).toBe("function");
  });
});

describe("the spots store and its photos (S1)", () => {
  const removed = () => { const r: string[] = []; return { r, photos: { remove: async (x: string) => { r.push(x); } } }; };

  it("a save that drops a photo deletes that photo — after the save", async () => {
    const fs = memFs(), { r, photos } = removed();
    const store = openSpotsWith(fs, "s.json", Date.now, photos);
    await store.put(spot("s1", ["photos/s1-1.jpg", "photos/s1-2.jpg"]));
    await store.put(spot("s1", ["photos/s1-2.jpg"]));
    expect(r).toEqual(["photos/s1-1.jpg"]);
  });

  it("a save that fails deletes nothing", async () => {
    const fs = memFs(), { r, photos } = removed();
    const store = openSpotsWith(fs, "s.json", Date.now, photos);
    await store.put(spot("s1", ["photos/s1-1.jpg"]));
    fs.write = () => { throw new Error("disk full"); };
    await expect(store.put(spot("s1", []))).rejects.toThrow("disk full");
    expect(r).toEqual([]);
  });

  it("a photo that will not delete never fails the delete of its spot", async () => {
    const store = openSpotsWith(memFs(), "s.json", Date.now,
                                { remove: async () => { throw new Error("locked"); } });
    await store.put(spot("s1", ["photos/s1-1.jpg"]));
    await store.remove("s1");
    expect(await store.list()).toEqual([]);
  });

  it("persists RELATIVE photo references and no section id", async () => {
    const fs = memFs();
    const store = openSpotsWith(fs, "s.json");
    await store.put(spot("s1", ["photos/s1-1.jpg"]));
    const saved = fs.files.get("s.json")!;
    expect(JSON.parse(saved)[0].photos).toEqual(["photos/s1-1.jpg"]);
    expect(saved).not.toMatch(/file:\/\/|blob:|"section"/);
  });

  it("reads a spot whose photos were saved as null as having none", async () => {
    const fs = memFs();
    fs.files.set("s.json", JSON.stringify([{ ...spot("s1"), photos: null }]));
    expect((await openSpotsWith(fs, "s.json").get("s1"))!.photos).toEqual([]);
  });
});
