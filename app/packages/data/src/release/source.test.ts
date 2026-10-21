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
