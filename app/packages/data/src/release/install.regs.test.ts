/**
 * (T238, owner 2026-10-29) A PHONE KEEPS EVERY REGION'S REGULATIONS, whatever map it keeps: `install` (first run and
 * every T231 update) writes every data file the manifest names — every `regs-<r>` and `salmon-<r>` — and nothing in the
 * install or update path filters them by region. Pinned on a manifest with all nine regions' slices, first install and
 * an update that changes one region.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { diff, parseManifest, type Manifest } from "../manifest";
import { offlineSizes } from "./offline";
import { REGIONS } from "../pack.generated";
import { install, type ReleaseFs } from "./install";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
function manifest(release: string, bodies: Record<string, string>): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: release,
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(Object.entries(bodies).map(([k, b]) => [k, {
      url: `${release}/${k}-x.bin.gz`, bytes: enc(b).length, gz: 0, sha256: sha256Hex(enc(b)), format: `${k}/1`,
      section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}
const BODIES = Object.fromEntries([["core", "core"], ["water", "water"],
  ...REGIONS.flatMap((r) => [[`regs-${r}`, `regs ${r}`], [`salmon-${r}`, `salmon ${r}`]])]) as Record<string, string>;

function memFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(`${p}/`)),
    read: async (p) => files.get(p)!,
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(`${p}/`)) files.delete(k); },
  };
}

describe("install keeps every region's regulations", () => {
  it("first install and an update: every regs-<r> and salmon-<r> on disk and in the pointer", async () => {
    const fs = memFs();
    const m1 = manifest("r-1", BODIES);
    const dl1 = async (url: string) => enc(BODIES[url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "")]!);
    const held = await install(m1, fs, dl1, "https://r2.test/");
    const regional = REGIONS.flatMap((r) => [`regs-${r}`, `salmon-${r}`]);
    expect(regional.filter((f) => !held.files[f] || !fs.files.has(`release/r-1/${f}-x.bin`))).toEqual([]);

    const b2: Record<string, string> = { ...BODIES, "regs-3": "regs 3, amended" };
    const m2 = manifest("r-2", b2);
    const fetched: string[] = [];
    const dl2 = async (url: string) => { const n = url.replace(/^.*\/r-2\//, "").replace(/-x\.bin\.gz$/, ""); fetched.push(n); return enc(b2[n]!); };
    const next = await install(m2, fs, dl2, "https://r2.test/", held);
    expect(fetched).toEqual(["regs-3"]);                     // the rest copied from the held set, none dropped
    expect(regional.filter((f) => !next.files[f] || !fs.files.has(`release/r-2/${f}-x.bin`))).toEqual([]);
  });

  it("(T256) the web's answer slices are never installed: the phone holds the whole core and water", async () => {
    const fs = memFs();
    const bodies = { ...BODIES, "core-2": "core of 2", "water-2": "water of 2" };
    const base = manifest("r-1", bodies);
    const m = { ...base, files: { ...base.files, "core-2": { ...base.files["core-2"]!, slice_of: "core", region: "2" },
                                  "water-2": { ...base.files["water-2"]!, slice_of: "water", region: "2" } } };
    const asked: string[] = [];
    const dl = async (url: string) => {
      const n = url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "");
      asked.push(n);
      return enc(bodies[n as keyof typeof bodies]!);
    };
    const held = await install(m, fs, dl, "https://r2.test/");
    expect(asked.filter((n) => n.includes("-2") && !n.startsWith("regs") && !n.startsWith("salmon"))).toEqual([]);
    expect(Object.keys(held.files).sort()).toEqual(Object.keys(BODIES).sort());
    expect(asked).toContain("core");
    expect(asked).toContain("water");
  });

  it("(T369) the web-only `lines` (links are web-only) is never installed, nor counted in what a phone holds", async () => {
    const fs = memFs();
    const bodies = { ...BODIES, lines: "lines" };
    const base = manifest("r-1", bodies);
    const m = { ...base, files: { ...base.files, lines: { ...base.files.lines!, web_only: true } } };
    const asked: string[] = [];
    const dl = async (url: string) => {
      const n = url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "");
      asked.push(n);
      return enc(bodies[n as keyof typeof bodies]!);
    };
    const held = await install(m, fs, dl, "https://r2.test/");
    expect([asked.includes("lines"), "lines" in held.files, diff(held, m).files]).toEqual([false, false, []]);
    expect(offlineSizes(m).regs).toBe(offlineSizes(base).regs - base.files.lines!.bytes);
  });
});
