/**
 * P11: THE MAP ASSETS on a phone (glyphs + sprites, `pipeline/deliver/tiles/assets.py`): the folder's `assets.json` is
 * held to the manifest's `files_sha256`, every file to its own sha256 in it, and each lands under `release/<url>`.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { assetsDigest, installAssets, type RawFetch } from "./assets";
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
