/**
 * (T555) A saved item id follows `item_alias`: an id a later build absorbed opens the water it became (spots, alerts and
 * recent searches hold item ids); a missing id with no alias is still null, and a core cut before the table is unchanged.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import type { Db, Row } from "../sql/db";
import type { ItemId } from "../index";
import { releaseLoader, type DbName } from "./loader";
import { CROSS, RELEASE_QUERIES as R } from "./queries";
import { makeReleaseSource } from "./source";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const fmt = (k: string) => (k === "water" ? "water/1" : k.startsWith("regs-") ? "regs-region/1" : `${k}/1`);
const manifest = (): Manifest => parseManifest({
  format: "app-manifest/2", data_release: "r-1",
  tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
  files: Object.fromEntries(["core", "water", "regs-2"].map((k) => [k, {
    url: `r-1/${k}-x.sqlite.gz`, bytes: 1, gz: 0, sha256: sha256Hex(enc(k)), format: fmt(k), section_handles: H }])),
  regulations: { valid_until: null, versions: null } });

/** core holds gnis:17795 (ord 4); `wbk:328965685` was absorbed into it. `withTable: false` is a core cut before item_alias. */
const opener = (withTable: boolean) => async (name: DbName): Promise<Db> => {
  const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
    if (sql.startsWith("SELECT k, v FROM meta")) return [{ k: "format", v: fmt(name) }, { k: "section_handles", v: H }];
    if (name === "core" && sql === R.ITEM!.sql && p[0] === "gnis:17795") return [{ item_id: "gnis:17795", name: "Camp Slough", kind: "stream" }];
    if (name === "core" && sql === CROSS.HAS_ITEM_ALIAS.sql) return withTable ? [{ x: 1 }] : [];
    if (name === "core" && sql === CROSS.ITEM_CURRENT.sql && p[0] === "wbk:328965685") return [{ item_id: "gnis:17795" }];
    if (name === "core" && sql === CROSS.ITEM_ORD.sql && p[0] === "gnis:17795") return [{ ord: 4 }];
    if (name === "water" && sql === CROSS.AKEYS_FOR_ORD.sql) return p[0] === 4 ? [{ akey: 7 }] : [];
    if (name === "water" && sql.includes("akey_region")) return [{ akey: 7, region: "2" }];
    return [];
  };
  return { all, get: async (sql: string, ...p: unknown[]) => (await all(sql, ...p))[0] };
};
const bytes = { bytes: async () => new Uint8Array() };
const source = (withTable = true) => makeReleaseSource(releaseLoader(manifest(), bytes, opener(withTable)));

describe("saved ids follow item_alias", () => {
  it("an absorbed id opens the water it became, under the CURRENT id", async () => {
    const w = await source().water("wbk:328965685" as ItemId);
    expect(w).toMatchObject({ item: "gnis:17795", name: "Camp Slough" });
  });
  it("a current id is unchanged; an id with no alias is null", async () => {
    expect(await source().water("gnis:17795" as ItemId)).toMatchObject({ item: "gnis:17795" });
    expect(await source().water("wbk:0" as ItemId)).toBeNull();
  });
  it("a core cut before item_alias has no aliases: the absorbed id is null, as it always was", async () => {
    expect(await source(false).water("wbk:328965685" as ItemId)).toBeNull();
  });
  it("the regulations read follows it too", async () => {
    expect(await source().regulationKeys("wbk:328965685" as ItemId)).toEqual([7]);
  });
});
