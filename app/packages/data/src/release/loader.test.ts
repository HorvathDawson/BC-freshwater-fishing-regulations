/** P11: the loader opens lazily and checks every file; install/update download only what changed and swap atomically. */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { releaseLoader, ReleaseError, type DbName } from "./loader";
import { heldFrom, install, POINTER, type ReleaseFs } from "./install";
import type { Db } from "../bundle/db";

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
