/**
 * (T596) ONE INSTALL AT A TIME: two installs of the same release (a resumed update beside a fresh one, a second boot) both
 * wrote `<file>.part`; the first one's rename took it and the second's found "NoSuchFileException … .part". `install` and
 * `prune` now run one at a time per process, so a `.part` in flight is never another task's to move or delete.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { install, prune, type ReleaseFs } from "./install";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const BODIES: Record<string, string> = { core: "core body", water: "water body" };
const manifest = (release: string): Manifest => parseManifest({
  format: "app-manifest/2", data_release: release,
  tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
  files: Object.fromEntries(Object.entries(BODIES).map(([k, b]) => [k, {
    url: `${release}/${k}-x.bin.gz`, bytes: enc(b).length, gz: 0, sha256: sha256Hex(enc(b)), format: `${k}/1`, section_handles: H }])),
  regulations: { valid_until: null, versions: null },
});
const tick = () => new Promise<void>((r) => setTimeout(r, 1));

/** Like the phone's file system: every step takes a turn, and moving or reading a file that is not there throws. */
function strictFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  const missing = (p: string) => new Error(`java.nio.file.NoSuchFileException: ${p}`);
  return {
    files,
    exists: async (p) => { await tick(); return files.has(p) || [...files.keys()].some((k) => k.startsWith(`${p}/`)); },
    read: async (p) => { await tick(); const b = files.get(p); if (!b) throw missing(p); return b; },
    write: async (p, b) => { await tick(); files.set(p, b); },
    rename: async (a, b) => { await tick(); const f = files.get(a); if (!f) throw missing(a); files.set(b, f); files.delete(a); },
    remove: async (p) => { await tick(); for (const k of [...files.keys()]) if (k === p || k.startsWith(`${p}/`)) files.delete(k); },
  };
}
const dl = async (url: string) => { await tick(); return enc(BODIES[url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "")]!); };

describe("install runs one at a time (T596)", () => {
  it("two installs of one release at once both finish, and the file is fetched once", async () => {
    const fs = strictFs(), asked: string[] = [];
    const counted = async (u: string, e: { bytes: number; sha256: string }) => { asked.push(u); return dl(u); };
    const [a, b] = await Promise.all([install(manifest("r-1"), fs, counted, "https://r2.test/"),
                                      install(manifest("r-1"), fs, counted, "https://r2.test/")]);
    expect(a.release).toBe("r-1");
    expect(b.release).toBe("r-1");
    expect(asked.length).toBe(2);                       // core + water, fetched by the first; the second kept them
    expect([...fs.files.keys()].filter((k) => k.endsWith(".part"))).toEqual([]);
  });

  it("a prune waits for the install in flight rather than deleting its folder", async () => {
    const fs = strictFs();
    const m = manifest("r-1");
    const running = install(m, fs, dl, "https://r2.test/");
    await tick();
    const pruning = prune(fs, { release: "r-1", tiles: null, files: {} } as never, m, { release: "r-2", tiles: null, files: {} } as never, manifest("r-2"));
    const held = await running;
    await pruning;
    expect(held.release).toBe("r-1");
    expect(fs.files.has("release/r-1/core-x.bin")).toBe(false);   // pruned only after the install had finished
  });
});
