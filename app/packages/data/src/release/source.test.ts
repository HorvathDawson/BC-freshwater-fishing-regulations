/**
 * P11: the reader's REGULATIONS read goes through the per-region file — the source the Shell is handed opens core, then
 * water, then the region file holding the water's answer keys (`loader.regionDb`), and a water in another region adds
 * only that region's file. Held over the loader's `opened()`, the same order the web fetches in.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import type { Db, Row } from "../bundle/db";
import type { ItemId } from "../index";
import { releaseLoader, type DbName } from "./loader";
import { CROSS } from "./queries";
import { makeReleaseSource } from "./source";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const fmt = (k: string) => (k === "water" ? "water/1" : k.startsWith("regs-") ? "regs-region/1" : `${k}/1`);

function manifest(names: string[]): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(names.map((k) => [k, {
      url: `r-1/${k}-x.sqlite.gz`, bytes: 1, gz: 0, sha256: sha256Hex(enc(k)), format: fmt(k), section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}

/** core knows two waters (ords 1 and 2); water gives ord 1 the key 7 (Region 2) and ord 2 the key 9 (Region 5). */
const open = async (name: DbName): Promise<Db> => {
  const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
    if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt(name) }, { k: "section_handles", v: H }];
    if (name === "water" && sql.includes("akey_region")) return [{ akey: 7, region: "2" }, { akey: 9, region: "5" }];
    if (name === "water" && sql === CROSS.AKEYS_FOR_ORD.sql) return p[0] === 1 ? [{ akey: 7 }] : p[0] === 2 ? [{ akey: 9 }] : [];
    return [];
  };
  return {
    all,
    get: async (sql: string, ...p: unknown[]) => (name === "core" && sql === CROSS.ITEM_ORD.sql
      ? ({ "gnis:3062": { ord: 1 }, "gnis:5": { ord: 2 } } as Record<string, Row>)[String(p[0])]
      : (await all(sql, ...p))[0]),
  };
};
const bytes = { bytes: async () => new Uint8Array() };

describe("makeReleaseSource.regulationKeys: the regulations read opens the water's region file, lazily", () => {
  it("the Vedder opens core + water + regs-2; a Region 5 water then adds regs-5 only", async () => {
    const loader = releaseLoader(manifest(["core", "water", "regs-2", "regs-5"]), bytes, open);
    const source = makeReleaseSource(loader);
    expect(loader.opened()).toEqual([]);
    expect(await source.regulationKeys("gnis:3062" as ItemId)).toEqual([7]);
    expect(loader.opened()).toEqual(["core", "water", "regs-2"]);
    expect(await source.regulationKeys("gnis:5" as ItemId)).toEqual([9]);
    expect(loader.opened()).toEqual(["core", "water", "regs-2", "regs-5"]);
  });
  it("a water core does not know opens nothing past core", async () => {
    const loader = releaseLoader(manifest(["core", "water", "regs-2"]), bytes, open);
    expect(await makeReleaseSource(loader).regulationKeys("gnis:0" as ItemId)).toEqual([]);
    expect(loader.opened()).toEqual(["core"]);
  });
  it("a release that ships no regulations (no water file) answers none and opens nothing", async () => {
    const loader = releaseLoader(manifest(["core"]), bytes, open);
    expect(await makeReleaseSource(loader).regulationKeys("gnis:3062" as ItemId)).toEqual([]);
    expect(loader.opened()).toEqual([]);
  });
});

/**
 * WHERE A WATER IS, from its parts' geometry (the water file's `part_geo`, integer microdegrees). The rows are release
 * r-6960fe8201e3's own: Kooryet Lake (ord 11837, one part, no gauge and no town ring) and Kootenay Lake (ord 11862),
 * which owns no section — its three closure areas (ords 11618-11620, `part_of` it) hold them all.
 */
const GEO: Record<number, Row[]> = {
  11837: [{ w: -130043061, s: 53333981, e: -129902673, n: 53361987, mid_lon: -129984551, mid_lat: 53344782 }],
  11618: [{ w: -117130272, s: 49246325, e: -116479453, n: 50180355, mid_lon: -116808854, mid_lat: 49640120 }],
  11619: [{ w: -117219097, s: 49551213, e: -116967467, n: 49639755, mid_lon: -117104970, mid_lat: 49601084 }],
  11620: [{ w: -117468881, s: 49447024, e: -117213168, n: 49592476, mid_lon: -117295876, mid_lat: 49504886 }],
};
const ORDS: Record<string, number[]> = {
  "wbk:328972518": [11837], "wbk:328974235": [11862, 11618, 11619, 11620],
};
const openGeo = async (name: DbName): Promise<Db> => {
  const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
    if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt(name) }, { k: "section_handles", v: H }];
    if (name === "core" && sql === CROSS.GEO_ORDS.sql) return (ORDS[String(p[0])] ?? []).map((ord) => ({ ord }));
    if (name === "water" && sql === CROSS.PART_GEO_FOR_ORDS(p.length).sql)
      return p.flatMap((o) => GEO[Number(o)] ?? []);
    throw new Error(`unexpected ${name}: ${sql}`);
  };
  return { all, get: async (sql: string, ...p: unknown[]) => (await all(sql, ...p))[0] };
};

describe("makeReleaseSource.waterBox: every water placed by its own geometry", () => {
  it("Kooryet Lake's box is its part_geo, in degrees — read from core, then water", async () => {
    const loader = releaseLoader(manifest(["core", "water"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterBox("wbk:328972518" as ItemId))
      .toEqual([-130.043061, 53.333981, -129.902673, 53.361987]);
    expect(loader.opened()).toEqual(["core", "water"]);
  });
  it("Kootenay Lake, which owns no part, is the union of its three closure areas", async () => {
    const loader = releaseLoader(manifest(["core", "water"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterBox("wbk:328974235" as ItemId))
      .toEqual([-117.468881, 49.246325, -116.479453, 50.180355]);
  });
  it("a water core does not know is no box, and opens nothing past core", async () => {
    const loader = releaseLoader(manifest(["core", "water"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterBox("gnis:0" as ItemId)).toBeNull();
    expect(loader.opened()).toEqual(["core"]);
  });
  it("a release with no water file has no geometry: null, and nothing opened", async () => {
    const loader = releaseLoader(manifest(["core"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterBox("wbk:328972518" as ItemId)).toBeNull();
    expect(loader.opened()).toEqual([]);
  });
});
