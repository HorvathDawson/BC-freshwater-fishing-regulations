/**
 * (T919 c) THE MAP ASSETS AS ONE PACKED DOWNLOAD: the pack `pipeline/deliver/tiles/assets.py` `write_pack` writes (one
 * committed fixture both sides read, packages/data/dev/assets-pack-parity.json, rule 40; pipeline/tests/test_tiles.py
 * holds the Python writer to it) is what `@app/data` release/assets.ts `unpackAssets` places, every byte verified, and
 * every way a pack can be wrong is refused by name (W46). Here, not beside assets.ts: a data test may not read a file.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { gzipSync, zlibSync } from "fflate";
import { sha256Hex } from "../packages/data/src/sha256";
import { assetsDigest, unpackAssets, untar, unzlibAsync } from "../packages/data/src/release/assets";
import type { ReleaseFs } from "../packages/data/src/release/install";

function memFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(p + "/")),
    read: async (p) => { const b = files.get(p); if (!b) throw new Error(`no ${p}`); return b; },
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(p + "/")) files.delete(k); },
  };
}

const enc = (s: string) => new TextEncoder().encode(s);
const URL_ = "tiles/v1/map-assets-1/";

/** (T919 c) A ustar archive as `assets.py` `write_pack` writes one (regular files; `type` lets a test write another). */
function tarOf(members: [string, Uint8Array, string?][]): Uint8Array {
  const blocks: Uint8Array[] = [];
  const field = (h: Uint8Array, at: number, n: number, v: string) => h.set(enc(v).subarray(0, n), at);
  for (const [name, b, type] of members) {
    const h = new Uint8Array(512);
    field(h, 0, 100, name);
    field(h, 100, 8, "0000644\0"); field(h, 108, 8, "0000000\0"); field(h, 116, 8, "0000000\0");
    field(h, 124, 12, `${b.length.toString(8).padStart(11, "0")}\0`); field(h, 136, 12, "00000000000\0");
    field(h, 156, 1, type ?? "0"); field(h, 257, 6, "ustar\0"); field(h, 263, 2, "00");
    field(h, 148, 8, "        ");
    const sum = h.reduce((n, x) => n + x, 0);
    field(h, 148, 8, `${sum.toString(8).padStart(6, "0")}\0 `);
    blocks.push(h, b, new Uint8Array((512 - (b.length % 512)) % 512));
  }
  blocks.push(new Uint8Array(1024));
  const out = new Uint8Array(blocks.reduce((n, x) => n + x.length, 0));
  let o = 0;
  for (const x of blocks) { out.set(x, o); o += x.length; }
  return out;
}

/** (T919 c, rule 40) The pack `assets.py` `write_pack` wrote, committed: both sides read it (`test_tiles.py` too). */
const FX = JSON.parse(readFileSync(join(__dirname, "../packages/data/dev/assets-pack-parity.json"), "utf8")) as {
  folder: Record<string, string>; assets_json: string; files_sha256: string; pack: string };
const PACK = "release/tiles/v1/map-assets-1.tar.zlib";
const fxBlock = { id: "map-assets-1", url: URL_, files_sha256: FX.files_sha256 };
const fxList = () => JSON.parse(FX.assets_json) as { files: Record<string, string> };
/** A pack of the fixture's folder with `edit` applied to its members (`[name, bytes, type?]`), in a zlib stream. */
const packWith = (edit: (m: [string, Uint8Array, string?][]) => [string, Uint8Array, string?][]) =>
  zlibSync(tarOf(edit([["assets.json", enc(FX.assets_json)],
                       ...Object.keys(FX.folder).sort().map((k): [string, Uint8Array] => [k, enc(FX.folder[k]!)])])));

describe("(T919 c) unpackAssets: the map assets from ONE packed file", () => {
  it("unpacks the pack Python wrote: every file under release/<url>, byte for byte, assets.json placed last", async () => {
    const fs = memFs(), writes: string[] = [];
    fs.files.set(PACK, Uint8Array.from(Buffer.from(FX.pack, "base64")));
    const write = fs.write;
    fs.write = async (p, b) => { writes.push(p); await write(p, b); };
    const seen: [number, number][] = [];
    await unpackAssets(fs, PACK, fxBlock, (n, all) => seen.push([n, all]));
    const names = Object.keys(FX.folder).sort();
    expect([...fs.files.keys()].filter((k) => k.startsWith(`release/${URL_}`)).sort())
      .toEqual([`release/${URL_}assets.json`, ...names.map((k) => `release/${URL_}${k}`)].sort());
    for (const k of names) expect(new TextDecoder().decode(fs.files.get(`release/${URL_}${k}`))).toBe(FX.folder[k]);
    expect(new TextDecoder().decode(fs.files.get(`release/${URL_}assets.json`))).toBe(FX.assets_json);
    expect(writes.at(-1)).toBe(`release/${URL_}assets.json.part`);
    expect(seen).toEqual([[0, names.length], [names.length, names.length]]);
    expect(assetsDigest(fxList().files)).toBe(FX.files_sha256);
  });

  it("reads back the members a tar writer wrote, in order, and refuses a non-file or a cut archive", () => {
    const t = tarOf([["a/b.pbf", enc("glyphs")], ["c.json", new Uint8Array(0)]]);
    expect(untar(t).map(([n, b]) => [n, new TextDecoder().decode(b)])).toEqual([["a/b.pbf", "glyphs"], ["c.json", ""]]);
    expect(() => untar(tarOf([["fonts", new Uint8Array(0), "5"]]))).toThrow("map assets pack: fonts is not a regular file (type 5)");
    expect(() => untar(t.subarray(0, 515))).toThrow("map assets pack: a/b.pbf is cut (3 of 6 bytes)");
    expect(() => untar(t.subarray(0, 512 + 512 + 100))).toThrow("map assets pack: it ends inside a header (byte 1024 of 1124)");
  });

  it("inflates a zlib stream a slice at a time; refuses gzip, a cut stream and other bytes, naming the file", async () => {
    const body = enc("glyphs ".repeat(5000));
    expect(await unzlibAsync(zlibSync(body), "p", 1024)).toEqual(body);
    await expect(unzlibAsync(gzipSync(body), "p")).rejects.toThrow("p: not a zlib stream");
    const z = zlibSync(body);
    await expect(unzlibAsync(z.subarray(0, z.length - 8), "p", 64)).rejects.toThrow(/^p: (the zlib stream is cut|it would not inflate)/);
    await expect(unzlibAsync(enc("plain text"), "p")).rejects.toThrow("p: not a zlib stream");
  });

  it("refuses a pack whose assets.json is not the manifest's files_sha256, and places nothing", async () => {
    const fs = memFs();
    fs.files.set(PACK, Uint8Array.from(Buffer.from(FX.pack, "base64")));
    await expect(unpackAssets(fs, PACK, { ...fxBlock, files_sha256: "0".repeat(64) })).rejects
      .toThrow(`${PACK}: assets.json files ${FX.files_sha256.slice(0, 12)}…, the manifest says 000000000000…`);
    expect([...fs.files.keys()]).toEqual([PACK]);
  });

  it("refuses a member that is not its sha256, a missing one, an extra one and a doubled one, naming it", async () => {
    const light = "sprites/v4/light.json";
    const cases: [string, (m: [string, Uint8Array, string?][]) => [string, Uint8Array, string?][]][] = [
      [`${PACK}: ${light} sha256 ${sha256Hex(enc("tampered")).slice(0, 12)}…, assets.json says ${fxList().files[light]!.slice(0, 12)}…`,
       (m) => m.map(([n, b]) => [n, n === light ? enc("tampered") : b])],
      [`${PACK}: ${light} is not in the pack`, (m) => m.filter(([n]) => n !== light)],
      [`${PACK}: 1 files assets.json does not name (stray.txt)`, (m) => [...m, ["stray.txt", enc("x")]]],
      [`${PACK}: ${light} is in the pack twice`, (m) => [...m, [light, enc("{\"a\": 1}")]]],
      [`${PACK}: no assets.json in the pack`, (m) => m.filter(([n]) => n !== "assets.json")],
      [`${PACK}: assets.json would not read (`, (m) => m.map(([n, b]) => [n, n === "assets.json" ? enc("not json") : b])],
    ];
    for (const [said, edit] of cases) {
      const fs = memFs();
      fs.files.set(PACK, packWith(edit));
      await expect(unpackAssets(fs, PACK, fxBlock)).rejects.toThrow(said);
      expect(fs.files.has(`release/${URL_}assets.json`)).toBe(false);     // never whole
    }
  });
});
