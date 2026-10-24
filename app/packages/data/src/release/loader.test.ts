/** P11: the loader opens lazily and checks every file; install/update download only what changed and swap atomically. */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { releaseLoader, ReleaseError, type DbName } from "./loader";
import { heldFrom, heldPath, install, POINTER, type ReleaseFs } from "./install";
import type { Db } from "../sql/db";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";

function manifest(files: Record<string, string>, release = "r-1"): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: release,
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(Object.entries(files).map(([k, body]) => [k, {
      url: `${release}/${k}-x.sqlite.gz`, bytes: enc(body).length, gz: 0, sha256: sha256Hex(enc(body)),
      format: k === "water" ? "water/1" : k.startsWith("regs-") ? "regs-region/1" : `${k}/1`, section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}

/** A fake Db whose meta says what the manifest says (or `lie`). */
const fakeOpen = (lie?: string) => async (name: DbName): Promise<Db> => ({
  all: async (sql: string) => sql.startsWith("SELECT k, v FROM meta")
    ? [{ k: "format", v: lie ?? (name === "water" ? "water/1" : name.startsWith("regs-") ? "regs-region/1" : `${name}/1`) },
       { k: "section_handles", v: H }]
    : sql.includes("akey_region") ? [{ akey: 7, region: "2" }, { akey: 9, region: "5" }, { akey: 9, region: "7a" }] : [],
  get: async () => undefined,
});

describe("releaseLoader", () => {
  const m = manifest({ core: "c", water: "w", "regs-2": "r2", "regs-5": "r5", "regs-7a": "r7" });
  const src = { bytes: async () => new Uint8Array() };
  it("opens nothing until asked, then only what was asked; a region file per region", async () => {
    const L = releaseLoader(m, src, fakeOpen());
    expect(L.opened()).toEqual([]);
    await L.db("core");
    expect(L.opened()).toEqual(["core"]);
    await L.regionDb(7);                                   // the Vedder's key: water + regs-2
    expect(L.opened()).toEqual(["core", "water", "regs-2"]);
    await L.regionDb(9);                                   // a key in 5 and 7a, neither open: the first, only
    expect(L.opened()).toEqual(["core", "water", "regs-2", "regs-5"]);
  });
  it("the lake facts: decoded once on first ask against the release's handles; null when the release has none", async () => {
    const lakes = Uint8Array.from([0x42, 0x43, 0x4c, 0x4b, 1, 0xd7, 0x68, 0xd6, 0xe1, 0x35, 0xe6, 0x2b, 0x99, 0, 1, 12, 2, 0]);
    const asked: string[] = [];
    const L = releaseLoader(manifest({ core: "c", lakes: "l" }), { bytes: async (n) => { asked.push(n); return lakes; } },
                            fakeOpen());
    const f = await L.lakes();
    expect([f?.survey(12), f?.survey(11), await L.lakes() === f, asked, L.opened()])
      .toEqual(["digitised", null, true, ["lakes"], ["lakes"]]);
    expect(await releaseLoader(m, src, fakeOpen()).lakes()).toBeNull();
    // another atlas's file is refused
    const other = lakes.slice(); other[5] = 0;
    const R = releaseLoader(manifest({ lakes: "l" }), { bytes: async () => other }, fakeOpen());
    await expect(R.lakes()).rejects.toThrow("lake facts: built for handles 0068d6e135e62b99, not d768d6e135e62b99");
  });
  it("refuses a file whose meta is not the manifest's, and does not cache the failure", async () => {
    const L = releaseLoader(m, src, fakeOpen("core/0"));
    await expect(L.db("core")).rejects.toBeInstanceOf(ReleaseError);
    await expect(L.db("near")).rejects.toThrow(/no file near/);
  });
});

function memFs(): ReleaseFs & { files: Map<string, Uint8Array>; failRename?: string } {
  const files = new Map<string, Uint8Array>();
  const fs: ReleaseFs & { files: Map<string, Uint8Array>; failRename?: string } = {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(p + "/")),
    read: async (p) => { const b = files.get(p); if (!b) throw new Error(`no ${p}`); return b; },
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => {
      if (fs.failRename && b === fs.failRename) throw new Error("crash");
      files.set(b, files.get(a)!); files.delete(a);
    },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(p + "/")) files.delete(k); },
  };
  return fs;
}
const server = (bodies: Record<string, string>, log: string[]) => async (url: string, e: { bytes: number; sha256: string }) => {
  log.push(url);
  const name = url.split("/").pop()!.split("-x")[0]!;
  const b = enc(bodies[name] ?? "");
  if (sha256Hex(b) !== e.sha256) throw new Error(`sha mismatch for ${url}`);
  return b;
};

describe("install / update (native, always offline)", () => {
  it("installs every file, then an update downloads only the changed ones and copies the rest", async () => {
    const fs = memFs(), log: string[] = [];
    const v1 = { core: "c1", water: "w1", paint: "p1" };
    const held = await install(manifest(v1), fs, server(v1, log), "https://r2/");
    expect(log.length).toBe(3);
    expect(await heldFrom(fs)).toEqual(held);
    const v2 = { core: "c2", water: "w1", paint: "p1" };
    log.length = 0;
    const next = await install(manifest(v2, "r-2"), fs, server(v2, log), "https://r2/", held);
    expect(log).toEqual(["https://r2/r-2/core-x.sqlite.gz"]);
    expect(next.release).toBe("r-2");
    expect(new TextDecoder().decode(await fs.read("release/r-2/water-x.sqlite"))).toBe("w1");
  });
  it("an unchanged file that no longer matches on disk is downloaded again, never copied", async () => {
    // a file opened in place can be touched by the engine (a journal mode written into its header): its bytes are no
    // longer the manifest's, and copying it would fail the re-verify and strand the update
    const fs = memFs(), log: string[] = [];
    const v1 = { core: "c1", water: "w1" };
    const held = await install(manifest(v1), fs, server(v1, log), "https://r2/");
    fs.files.set("release/r-1/water-x.sqlite", enc("w1 touched"));
    log.length = 0;
    const v2 = { core: "c2", water: "w1" };
    const next = await install(manifest(v2, "r-2"), fs, server(v2, log), "https://r2/", held);
    expect(log).toEqual(["https://r2/r-2/core-x.sqlite.gz", "https://r2/r-2/water-x.sqlite.gz"]);
    expect(new TextDecoder().decode(await fs.read(heldPath(next, "r-2/water-x.sqlite.gz")))).toBe("w1");
  });
  it("tiles and their mask stream to disk (never held whole), only when the tile version moves", async () => {
    const fs = memFs(), log: string[] = [], streamed: string[] = [];
    const tile = enc("PMTiles bytes"), mask = enc('{"type":"FeatureCollection"}');
    const tiles = (id: string) => ({
      compatible: [`${H}.*`], assets: null,
      current: { id, url: `tiles/${id}/atlas-${id}.pmtiles`, bytes: tile.length, sha256: sha256Hex(tile) },
      mask: { url: `tiles/${id}/bc_outside.geojson`, bytes: mask.length, sha256: sha256Hex(mask) },
    });
    const toDisk = async (url: string, path: string, e: { bytes: number; sha256: string }, onBytes?: (n: number) => void) => {
      streamed.push(`${url} -> ${path}`);
      const b = url.endsWith(".pmtiles") ? tile : mask;
      expect(sha256Hex(b)).toBe(e.sha256);
      onBytes?.(b.length);
      await fs.write(`${path}.part`, b);
      await fs.rename(`${path}.part`, path);
    };
    const m1 = { ...manifest({ core: "c1" }), tiles: tiles(`${H}.aaa`) } as Manifest;
    const progress: string[] = [];
    const held = await install(m1, fs, server({ core: "c1" }, log), "https://r2/", null,
                               (p) => progress.push(`${p.file} ${p.bytes}/${p.of}`), toDisk);
    expect(streamed).toEqual([
      `https://r2/tiles/${H}.aaa/atlas-${H}.aaa.pmtiles -> release/tiles/${H}.aaa/atlas-${H}.aaa.pmtiles`,
      `https://r2/tiles/${H}.aaa/bc_outside.geojson -> release/tiles/${H}.aaa/bc_outside.geojson`]);
    const T = 2 + tile.length + mask.length;
    // each streamed file reports while it streams (here: once, whole) and again when it is in place
    expect(progress).toEqual([`core 2/${T}`, `tiles 15/${T}`, `tiles 15/${T}`, `mask ${T}/${T}`, `mask ${T}/${T}`]);
    expect(held.tiles).toBe(`${H}.aaa`);
    streamed.length = 0;
    const m2 = { ...manifest({ core: "c2" }, "r-2"), tiles: tiles(`${H}.aaa`) } as Manifest;
    await install(m2, fs, server({ core: "c2" }, log), "https://r2/", held, () => {}, toDisk);
    expect(streamed).toEqual([]);                              // a data-only release never touches the tiles
  });
  it("a crash at the swap, or a failed sha, keeps the old set in force", async () => {
    const fs = memFs(), log: string[] = [];
    const v1 = { core: "c1" };
    const held = await install(manifest(v1), fs, server(v1, log), "https://r2/");
    fs.failRename = POINTER;
    await expect(install(manifest({ core: "c2" }, "r-2"), fs, server({ core: "c2" }, log), "https://r2/", held))
      .rejects.toThrow("crash");
    expect((await heldFrom(fs))!.release).toBe("r-1");
    fs.failRename = undefined;
    await expect(install(manifest({ core: "c3" }, "r-3"), fs, server({ core: "WRONG" }, log), "https://r2/", held))
      .rejects.toThrow(/sha mismatch/);
    expect((await heldFrom(fs))!.release).toBe("r-1");
  });
});
