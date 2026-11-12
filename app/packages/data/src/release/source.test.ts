/**
 * P11: the reader's REGULATIONS read goes through the per-region file — the source the Shell is handed opens core, then
 * water, then the region file holding the water's answer keys (`loader.regionDb`), and a water in another region adds
 * only that region's file. Held over the loader's `opened()`, the same order the web fetches in.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import type { Db, Row } from "../sql/db";
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
    // (T555) item_alias: `wbk:1` was absorbed into Kooryet Lake; asked only for an id core does not hold
    if (name === "core" && sql === CROSS.HAS_ITEM_ALIAS.sql) return [{ x: 1 }];
    if (name === "core" && sql === CROSS.ITEM_CURRENT.sql) return p[0] === "wbk:1" ? [{ item_id: "wbk:328972518" }] : [];
    if ((name === "water" || name === "geo") && sql === CROSS.PART_GEO_FOR_ORDS(p.length).sql)
      return p.flatMap((o) => GEO[Number(o)] ?? []);
    throw new Error(`unexpected ${name}: ${sql}`);
  };
  return { all, get: async (sql: string, ...p: unknown[]) => (await all(sql, ...p))[0] };
};

describe("T182: a release with the split files reads each moved table from its own file", () => {
  it("show on the map: Kooryet Lake's box from core + geo — not the whole water file", async () => {
    const loader = releaseLoader(manifest(["core", "water", "geo"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterBox("wbk:328972518" as ItemId))
      .toEqual([-130.043061, 53.333981, -129.902673, 53.361987]);
    expect(loader.opened()).toEqual(["core", "geo"]);
  });

  /** Every SQL a file is asked, by file; `tables` says which tables each file holds (meta's are every file's). */
  const recording = (tables: Record<string, string[]>) => {
    const asked: [string, string][] = [];
    const open = async (name: DbName): Promise<Db> => {
      const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
        if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt(name) }, { k: "section_handles", v: H }];
        asked.push([name, sql]);
        const m = /name = '(\w+)'/.exec(sql);
        if (sql.includes("sqlite_master")) return (tables[name] ?? []).includes(m![1]!) ? [{ x: 1 }] : [];
        if (sql.includes("FROM section_down")) return p[0] === 5 ? [{ down_sid: 6 }] : [];
        if (sql.includes("FROM lake_section")) return [{ sid: 7, station: "08MH103" }];
        if (sql.includes("FROM item_section s JOIN item i")) return [{ sid: 7, item_id: "wbk:1" }];
        if (sql.includes("FROM lake_gauge")) return [{ item_id: "wbk:1", station: "08MH090" }];
        return [];
      };
      return { all, get: async (sql: string, ...p: unknown[]) => (await all(sql, ...p))[0] };
    };
    return { asked, open };
  };
  const files = (asked: [string, string][], what: RegExp) => [...new Set(asked.filter(([, s]) => what.test(s)).map(([f]) => f))];

  it("the downstream walk reads down; the climatology reads climate; the lake road is conditions' lake_section", async () => {
    const r = recording({ conditions: ["lake_section"] });
    const loader = releaseLoader(manifest(["core", "conditions", "down", "climate"]), bytes, r.open);
    const src = makeReleaseSource(loader);
    expect(await src.traceToGauge(5 as never)).toEqual([5, 6]);
    await src.gaugeParameters("08MH001" as never);
    expect(await src.lakeStationsFor([7 as never])).toEqual(new Map([[7, "08MH103"]]));
    expect([files(r.asked, /section_down/), files(r.asked, /gauge_clim/), files(r.asked, /lake_section|lake_gauge/)])
      .toEqual([["down"], ["climate"], ["conditions"]]);
    expect(loader.opened()).toEqual(["down", "climate", "conditions"]);          // no core for the lake road
  });

  it("a release cut before the split: the same reads from the old files (conditions; core + conditions for lakes)", async () => {
    const r = recording({});
    const loader = releaseLoader(manifest(["core", "conditions"]), bytes, r.open);
    const src = makeReleaseSource(loader);
    expect(await src.traceToGauge(5 as never)).toEqual([5, 6]);
    await src.gaugeParameters("08MH001" as never);
    expect(await src.lakeStationsFor([7 as never])).toEqual(new Map([[7, "08MH090"]]));
    expect([files(r.asked, /section_down/), files(r.asked, /gauge_clim/), files(r.asked, /FROM lake_gauge/)])
      .toEqual([["conditions"], ["conditions"], ["conditions"]]);
    expect(loader.opened()).toEqual(["conditions", "core"]);
  });
});

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
  it("(T555) a saved id a later build absorbed is placed by the water it became (item_alias): box and point", async () => {
    const src = makeReleaseSource(releaseLoader(manifest(["core", "water"]), bytes, openGeo));
    expect(await src.waterBox("wbk:1" as ItemId)).toEqual(await src.waterBox("wbk:328972518" as ItemId));
    expect(await src.waterBox("wbk:1" as ItemId)).not.toBeNull();
    expect(await src.waterPoint!("wbk:1" as ItemId)).toEqual(await src.waterPoint!("wbk:328972518" as ItemId));
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

describe("(T293) makeReleaseSource.waterPoint: a point on the water itself", () => {
  it("a one-part water: its part's midpoint, on its own line", async () => {
    const loader = releaseLoader(manifest(["core", "water"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterPoint!("wbk:328972518" as ItemId)).toEqual({ lon: -129.984551, lat: 53.344782 });
  });
  it("Kootenay Lake: of its areas' midpoints, the one nearest the middle of the whole box", async () => {
    const loader = releaseLoader(manifest(["core", "water"]), bytes, openGeo);
    expect(await makeReleaseSource(loader).waterPoint!("wbk:328974235" as ItemId)).toEqual({ lon: -116.808854, lat: 49.64012 });
  });
  it("no geometry: null", async () => {
    expect(await makeReleaseSource(releaseLoader(manifest(["core"]), bytes, openGeo)).waterPoint!("wbk:328972518" as ItemId)).toBeNull();
    expect(await makeReleaseSource(releaseLoader(manifest(["core", "water"]), bytes, openGeo)).waterPoint!("gnis:0" as ItemId)).toBeNull();
  });
});

describe("(T376) makeReleaseSource.basinAt: the watershed group a point is in, from conditions' grid", () => {
  const ROW = Uint8Array.from([2, 1, 2, 2, 2, 0]);
  const openBasins = (withGrid: boolean) => async (name: DbName): Promise<Db> => {
    const all = async (sql: string): Promise<Row[]> => {
      if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt(name) }, { k: "section_handles", v: H }];
      if (name !== "conditions") return [];
      if (sql === CROSS.HAS_BASIN_GRID.sql) return withGrid ? [{ x: 1 }] : [];
      if (sql === CROSS.BASIN_FRAME.sql) return [{ w: 0, s: 0, dlon: 0.5, dlat: 0.5, cols: 6, rows: 2 }];
      if (sql === CROSS.BASIN_GRID_ROWS.sql) return [{ row: 0, b: ROW }, { row: 1, b: ROW }];
      if (sql === CROSS.BASIN_NAMES.sql)
        return [{ ix: 1, basin_id: "CHWK", name: "Chilliwack River" }, { ix: 2, basin_id: "LFRA", name: "Lower Fraser" }];
      return [];
    };
    return { all, get: async (sql: string) => (await all(sql))[0] };
  };
  it("answers the group and its name as sourced, and null where no group is; conditions is the only file opened", async () => {
    const loader = releaseLoader(manifest(["core", "conditions"]), bytes, openBasins(true));
    const source = makeReleaseSource(loader);
    expect(await source.basinAt!({ lon: 0.2, lat: 0.7 })).toEqual({ id: "CHWK", name: "Chilliwack River" });
    expect(await source.basinAt!({ lon: 1.2, lat: 0.2 })).toEqual({ id: "LFRA", name: "Lower Fraser" });
    expect(await source.basinAt!({ lon: 2.5, lat: 0.2 })).toBeNull();
    expect(loader.opened()).toEqual(["conditions"]);
  });
  it("(T417) basinIn: the group with the largest share of a box, from the same grid", async () => {
    const source = makeReleaseSource(releaseLoader(manifest(["core", "conditions"]), bytes, openBasins(true)));
    // centred on the sea (lon 2.5) with Lower Fraser on screen: Lower Fraser, never none
    expect(await source.basinIn!([1.6, 0, 2.9, 0.9])).toEqual({ id: "LFRA", name: "Lower Fraser" });
    expect(await source.basinIn!([2.1, 0, 2.9, 0.9])).toBeNull();
    await expect(makeReleaseSource(releaseLoader(manifest(["core", "conditions"]), bytes, openBasins(false)))
      .basinIn!([0, 0, 1, 1])).rejects.toThrow("carries no watershed groups");
  });
  it("a release cut before the grid THROWS — never 'in no group'", async () => {
    const source = makeReleaseSource(releaseLoader(manifest(["core", "conditions"]), bytes, openBasins(false)));
    await expect(source.basinAt!({ lon: 0.2, lat: 0.7 })).rejects.toThrow("carries no watershed groups");
  });
});
