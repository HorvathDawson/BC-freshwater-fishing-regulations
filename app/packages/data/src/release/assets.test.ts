/**
 * P11: THE MAP ASSETS on a phone (glyphs + sprites, `pipeline/deliver/tiles/assets.py`): the folder's `assets.json` is
 * held to the manifest's `files_sha256`, every file to its own sha256 in it, and each lands under `release/<url>`.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { gzipSync, zlibSync } from "fflate";
import { sha256Hex } from "../sha256";
import { assetsDigest, installAssets, unpackAssets, untar, unzlibAsync, type RawFetch } from "./assets";
import type { ReleaseFs } from "./install";

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
const BASE = "https://r2.test/release/";
const URL_ = "tiles/v1/map-assets-1/";

/** A served assets folder: its files, its assets.json and the manifest block naming it. */
function folder(body: Record<string, string>, url = URL_, id = "map-assets-1") {
  const files = Object.fromEntries(Object.entries(body).map(([k, v]) => [k, sha256Hex(enc(v))]));
  const json = enc(JSON.stringify({ id, pin: "p", files }));
  const served = new Map<string, Uint8Array>([[`${BASE}${url}assets.json`, json]]);
  for (const [k, v] of Object.entries(body))
    served.set(`${BASE}${url}${k.split("/").map(encodeURIComponent).join("/")}`, enc(v));
  return { block: { id, url, files_sha256: assetsDigest(files) }, served, files };
}

function server(served: Map<string, Uint8Array>, log: string[]): RawFetch {
  return async (url) => {
    log.push(url);
    const b = served.get(url);
    if (!b) throw new Error(`${url}: HTTP 404`);
    return b;
  };
}

const BODY = { "fonts/Noto Sans Regular/0-255.pbf": "glyphs 0", "sprites/v4/light.json": "{\"a\":1}",
               "sprites/v4/light.png": "png" };

describe("assetsDigest (the manifest's files_sha256: Python's json.dumps(files, sort_keys=True))", () => {
  it("is the digest Python writes, byte for byte (pinned from assets.py's files_sha256)", () => {
    expect(assetsDigest({ "sprites/v4/light.json": "bb", "fonts/Noto Sans Regular/0-255.pbf": "aa" }))
      .toBe("770bd46af2df6f034d936602000b010fc8a0c9a1b57400ae31b134ae1deab687");
    // a key outside ASCII is escaped as Python's ensure_ascii does
    expect(assetsDigest({ "fonts/Noto Sans Regular/0-255.pbf": "aa", "sprites/v4/light.json": "bb", "é": "cc" }))
      .toBe("7a6c04232b3078d5ae9515a42f4c2228bfc07ebbc0805d71b5993b30bb666bdf");
  });
});

describe("installAssets", () => {
  it("fetches assets.json and every file (url-encoded), verifies each, and places them under release/<url>", async () => {
    const fs = memFs(), log: string[] = [], f = folder(BODY);
    await installAssets(fs, server(f.served, log), BASE, f.block);
    expect(log).toEqual([
      `${BASE}${URL_}assets.json`,
      `${BASE}${URL_}fonts/Noto%20Sans%20Regular/0-255.pbf`,
      `${BASE}${URL_}sprites/v4/light.json`,
      `${BASE}${URL_}sprites/v4/light.png`,
    ]);
    expect([...fs.files.keys()].sort()).toEqual([
      `release/${URL_}assets.json`,
      `release/${URL_}fonts/Noto Sans Regular/0-255.pbf`,
      `release/${URL_}sprites/v4/light.json`,
      `release/${URL_}sprites/v4/light.png`,
    ]);
    expect(new TextDecoder().decode(fs.files.get(`release/${URL_}sprites/v4/light.png`))).toBe("png");
  });

  it("resumes: a folder already whole on disk fetches nothing", async () => {
    const fs = memFs(), f = folder(BODY), log: string[] = [];
    await installAssets(fs, server(f.served, []), BASE, f.block);
    await installAssets(fs, server(f.served, log), BASE, f.block);
    expect(log).toEqual([]);
  });

  it("copies unchanged files from the held folder (tiles moved, assets did not) instead of fetching them", async () => {
    const fs = memFs(), f1 = folder(BODY), log: string[] = [];
    await installAssets(fs, server(f1.served, []), BASE, f1.block);
    const f2 = folder({ ...BODY, "sprites/v4/light.png": "png 2" }, "tiles/v2/map-assets-1/");
    await installAssets(fs, server(f2.served, log), BASE, f2.block, `release/${URL_}`);
    expect(log).toEqual([`${BASE}tiles/v2/map-assets-1/assets.json`, `${BASE}tiles/v2/map-assets-1/sprites/v4/light.png`]);
    expect(new TextDecoder().decode(fs.files.get("release/tiles/v2/map-assets-1/fonts/Noto Sans Regular/0-255.pbf")))
      .toBe("glyphs 0");
  });

  it("refuses a file that is not its assets.json's sha256, naming it", async () => {
    const fs = memFs(), f = folder(BODY);
    f.served.set(`${BASE}${URL_}sprites/v4/light.png`, enc("tampered"));
    await expect(installAssets(fs, server(f.served, []), BASE, f.block)).rejects
      .toThrow(`${BASE}${URL_}sprites/v4/light.png: sha256 ${sha256Hex(enc("tampered")).slice(0, 12)}…, ` +
               `assets.json says ${sha256Hex(enc("png")).slice(0, 12)}…`);
    expect(fs.files.has(`release/${URL_}sprites/v4/light.png`)).toBe(false);
  });

  it("refuses an assets.json that is not the manifest's files_sha256, and writes nothing", async () => {
    const fs = memFs(), f = folder(BODY), log: string[] = [];
    const block = { ...f.block, files_sha256: "0".repeat(64) };
    await expect(installAssets(fs, server(f.served, log), BASE, block)).rejects
      .toThrow(`${BASE}${URL_}assets.json: files ${f.block.files_sha256.slice(0, 12)}…, the manifest says 000000000000…`);
    expect(log).toEqual([`${BASE}${URL_}assets.json`]);
    expect([...fs.files.keys()]).toEqual([]);
  });

  it("refuses a manifest assets block with no files_sha256 (nothing to verify against)", async () => {
    const fs = memFs(), f = folder(BODY), log: string[] = [];
    await expect(installAssets(fs, server(f.served, log), BASE, { id: "map-assets-1", url: URL_ })).rejects
      .toThrow("assets map-assets-1: the manifest names no files_sha256");
    expect(log).toEqual([]);
  });
});

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
const FX = JSON.parse(readFileSync(join(__dirname, "../../dev/assets-pack-parity.json"), "utf8")) as {
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
    ];
    for (const [said, edit] of cases) {
      const fs = memFs();
      fs.files.set(PACK, packWith(edit));
      await expect(unpackAssets(fs, PACK, fxBlock)).rejects.toThrow(said);
      expect(fs.files.has(`release/${URL_}assets.json`)).toBe(false);     // never whole
    }
  });
});
