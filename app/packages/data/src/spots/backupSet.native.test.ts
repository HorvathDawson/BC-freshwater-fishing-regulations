/**
 * (T732) The phone's backup set over a fake expo-file-system and a fake expo-image-manipulator: the copy's ENCODING is
 * pinned as the native module receives it (1280 px on the long edge, WebP at 0.75: owner 2026-11-16 "D: WebP 75"), and
 * a round trip — copies made, the phone wiped to what the cloud backup carries, the next start — puts every photo back
 * from its WebP copy under the spot's own reference, with no copy made again. A pre-WebP copy (JPEG, named as its
 * original) restores too.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as efs from "expo-file-system";
import * as manip from "expo-image-manipulator";
import { syncBackupSet } from "./backupSet.native";
import type { Spot } from "./model";

vi.mock("expo-file-system", () => {
  const disk = new Map<string, string>();
  const join = (parts: unknown[]) => parts
    .map((p) => (typeof p === "string" ? p : (p as { uri: string }).uri))
    .map((s, i) => (i === 0 ? s.replace(/\/+$/, "") : s.replace(/^\/+|\/+$/g, "")))
    .join("/");
  class File {
    uri: string;
    constructor(...parts: unknown[]) { this.uri = join(parts); }
    get name() { return this.uri.slice(this.uri.lastIndexOf("/") + 1); }
    get exists() { return disk.has(this.uri); }
    get size() { const v = disk.get(this.uri); if (v === undefined) throw new Error(`no ${this.uri}`); return v.length; }
    delete() { if (!disk.delete(this.uri)) throw new Error(`no ${this.uri}`); }
    moveSync(to: File) { this.copySync(to); disk.delete(this.uri); this.uri = to.uri; }
    copySync(to: File) {
      const v = disk.get(this.uri);
      if (v === undefined) throw new Error(`no ${this.uri}`);
      if (disk.has(to.uri)) throw new Error("DestinationAlreadyExists");
      disk.set(to.uri, v);
    }
  }
  class Directory {
    uri: string;
    constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return [...disk.keys()].some((k) => k.startsWith(`${this.uri}/`)) || made.has(this.uri); }
    create() { made.add(this.uri); }
    list() {
      return [...disk.keys()].filter((k) => k.startsWith(`${this.uri}/`) && !k.slice(this.uri.length + 1).includes("/"))
        .map((k) => new File(k));
    }
  }
  const made = new Set<string>();
  return { Directory, File, Paths: { document: new Directory("file:///doc"), cache: new Directory("file:///cache") },
           __disk: disk, __made: made };
});

vi.mock("expo-image-manipulator", () => {
  const calls: unknown[][] = [];
  const sizes = new Map<string, { width: number; height: number }>();
  let n = 0;
  const image = (src: string, width: number, height: number) => ({
    width, height, release: () => {},
    async saveAsync(opts: { format: string; compress: number }) {
      calls.push(["save", opts]);
      const disk = (await import("expo-file-system") as unknown as { __disk: Map<string, string> }).__disk;
      const uri = `file:///cache/ImageManipulator/${++n}.${opts.format}`;
      disk.set(uri, `${opts.format.toUpperCase()} ${width}x${height} of ${src}`);
      return { uri, width, height };
    },
  });
  const ImageManipulator = {
    manipulate(src: string) {
      calls.push(["manipulate", src]);
      const whole = sizes.get(src) ?? { width: 4032, height: 3024 };
      let resize: { width?: number; height?: number } | null = null;
      const ctx = {
        reset() { return ctx; },
        resize(r: { width?: number; height?: number }) { calls.push(["resize", r]); resize = r; return ctx; },
        async renderAsync() {
          if (!resize) return image(src, whole.width, whole.height);
          const k = (resize.width ?? whole.width * (resize.height! / whole.height)) / whole.width;
          return image(src, Math.round(whole.width * k), Math.round(whole.height * k));
        },
        release: () => {},
      };
      return ctx;
    },
  };
  // the real enum's values (expo-image-manipulator 58 `ImageManipulator.types`): what the native module is handed
  return { ImageManipulator, SaveFormat: { JPEG: "jpeg", PNG: "png", WEBP: "webp" }, __calls: calls, __sizes: sizes };
});

const disk = (efs as unknown as { __disk: Map<string, string> }).__disk;
const made = (efs as unknown as { __made: Set<string> }).__made;
const calls = (manip as unknown as { __calls: unknown[][] }).__calls;
const sizes = (manip as unknown as { __sizes: Map<string, { width: number; height: number }> }).__sizes;

const PHOTOS = "file:///doc/spots/photos", SET = "file:///doc/spots/backup-photos";
const spot = (id: string, visitedAt: number, photos: string[]) => ({ id, visitedAt, photos }) as unknown as Spot;

/** What a cloud restore leaves: the spots folder's backed-up files only (no originals, no cache). */
function wipeToTheCloudSet() {
  for (const k of [...disk.keys()]) if (!k.startsWith(`${SET}/`) && !k.startsWith("file:///doc/spots/spots.")) disk.delete(k);
  made.clear();
}

beforeEach(() => { disk.clear(); made.clear(); calls.length = 0; sizes.clear(); });

describe("syncBackupSet on a phone (T732)", () => {
  it("ENCODING: a 12 MP photo's copy is 1280 px on its long edge, WebP at quality 0.75, named <original>.webp", async () => {
    disk.set(`${PHOTOS}/s1-1-ab.jpg`, "J".repeat(4_600_000));
    disk.set("file:///doc/spots/spots.spots", "{}");
    const r = await syncBackupSet([spot("s1", 1, ["photos/s1-1-ab.jpg"])]);
    expect(calls).toEqual([
      ["manipulate", `${PHOTOS}/s1-1-ab.jpg`],
      ["resize", { width: 1280 }],
      ["save", { format: "webp", compress: 0.75 }],
    ]);
    expect(disk.get(`${SET}/s1-1-ab.jpg.webp`)).toBe(`WEBP 1280x960 of ${PHOTOS}/s1-1-ab.jpg`);
    expect([...disk.keys()].filter((k) => k.startsWith("file:///cache/"))).toEqual([]);   // nothing left staged
    expect(r!.plan.kept).toEqual(["photos/s1-1-ab.jpg"]);
  });

  it("a portrait photo is held to 1280 px on its height; one already smaller is not resized, still WebP 75", async () => {
    sizes.set(`${PHOTOS}/p-1.jpg`, { width: 3024, height: 4032 });
    sizes.set(`${PHOTOS}/q-1.png`, { width: 800, height: 300 });
    disk.set(`${PHOTOS}/p-1.jpg`, "P"); disk.set(`${PHOTOS}/q-1.png`, "Q");
    await syncBackupSet([spot("p", 2, ["photos/p-1.jpg"]), spot("q", 1, ["photos/q-1.png"])]);
    expect(calls.filter((c) => c[0] !== "manipulate")).toEqual([
      ["resize", { height: 1280 }], ["save", { format: "webp", compress: 0.75 }],
      ["save", { format: "webp", compress: 0.75 }]]);
    expect(disk.get(`${SET}/p-1.jpg.webp`)).toBe(`WEBP 960x1280 of ${PHOTOS}/p-1.jpg`);
    expect(disk.get(`${SET}/q-1.png.webp`)).toBe(`WEBP 800x300 of ${PHOTOS}/q-1.png`);
  });

  it("ROUND TRIP: backup, a wiped phone restored from the cloud, the next start: each photo back from its WebP copy", async () => {
    disk.set(`${PHOTOS}/a-1.jpg`, "A"); disk.set(`${PHOTOS}/b-1-x.heic`, "B");
    disk.set("file:///doc/spots/spots.spots", "{}");
    const spots = [spot("a", 2, ["photos/a-1.jpg"]), spot("b", 1, ["photos/b-1-x.heic"])];
    await syncBackupSet(spots);
    const set = { a: disk.get(`${SET}/a-1.jpg.webp`), b: disk.get(`${SET}/b-1-x.heic.webp`) };
    expect(set.a).toBe(`WEBP 1280x960 of ${PHOTOS}/a-1.jpg`);

    wipeToTheCloudSet();
    expect([...disk.keys()].sort()).toEqual([`${SET}/a-1.jpg.webp`, `${SET}/b-1-x.heic.webp`, "file:///doc/spots/spots.spots"]);
    calls.length = 0;
    const r = await syncBackupSet(spots);
    expect(r!.restored).toEqual(["photos/a-1.jpg", "photos/b-1-x.heic"]);
    expect(disk.get(`${PHOTOS}/a-1.jpg`)).toBe(set.a);          // the spot's own reference, the WebP copy's bytes
    expect(disk.get(`${PHOTOS}/b-1-x.heic`)).toBe(set.b);
    expect(disk.get(`${SET}/a-1.jpg.webp`)).toBe(set.a);        // the copies stay in the set
    expect(calls).toEqual([]);                                   // no copy made again
    expect(r!.line).toBe("Your phone's backup holds your 2 spots and all 2 photos, made smaller.");

    calls.length = 0;
    const again = await syncBackupSet(spots);                    // a later start: nothing to put back, nothing to make
    expect([again!.restored, calls]).toEqual([[], []]);
  });

  it("A BACKUP FROM BEFORE WEBP (JPEG copy named as its original) restores, then is replaced by a WebP copy", async () => {
    disk.set(`${SET}/a-1.jpg`, "JPEG 1600x1200 copy");
    disk.set("file:///doc/spots/spots.spots", "{}");
    const r = await syncBackupSet([spot("a", 1, ["photos/a-1.jpg"])]);
    expect(r!.restored).toEqual(["photos/a-1.jpg"]);
    expect(disk.get(`${PHOTOS}/a-1.jpg`)).toBe("JPEG 1600x1200 copy");
    expect(disk.get(`${SET}/a-1.jpg.webp`)).toBe(`WEBP 1280x960 of ${PHOTOS}/a-1.jpg`);
    expect(disk.has(`${SET}/a-1.jpg`)).toBe(false);
  });
});
