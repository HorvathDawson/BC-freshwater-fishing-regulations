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
import { alertsWithCurrentItems, followSavedItems, savedItemRenames, spotWithCurrentItem } from "../savedIds";
import { NOTIFY_DEFAULT } from "../notify/settings";
import type { Spot } from "../spots/model";

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
const sectionsAskedFor: string[] = [];
const opener = (withTable: boolean) => async (name: DbName): Promise<Db> => {
  const all = async (sql: string, ...p: unknown[]): Promise<Row[]> => {
    if (sql === R.SECTIONS_FOR_ITEM!.sql) sectionsAskedFor.push(String(p[0]));
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
  it("currentItem: the id in use for an absorbed id, itself for a current one, null for one that is gone", async () => {
    const s = source();
    expect(await s.currentItem!("wbk:328965685" as ItemId)).toBe("gnis:17795");
    expect(await s.currentItem!("gnis:17795" as ItemId)).toBe("gnis:17795");
    expect(await s.currentItem!("wbk:0" as ItemId)).toBeNull();
  });
  it("locate follows it: the absorbed id holds no sections, so the water it became is asked", async () => {
    sectionsAskedFor.length = 0;
    await source().locate("wbk:328965685" as ItemId);
    expect(sectionsAskedFor).toEqual(["wbk:328965685", "gnis:17795"]);
    // a core cut before item_alias: the old id only, as it always was
    sectionsAskedFor.length = 0;
    await source(false).locate("wbk:328965685" as ItemId);
    expect(sectionsAskedFor).toEqual(["wbk:328965685"]);
  });
});

describe("what is kept by item id is rewritten once (T555 item 5)", () => {
  const spot = (item: string | null) => ({ id: "s1", item, waterName: "Camp Slough", lat: 49, lon: -122 }) as unknown as Spot;
  const rule = (id: string, item: string | null) => ({ id, station: "08MH001", stationName: "x", quantity: "discharge",
    direction: "above", threshold: { kind: "value", value: 1 }, on: true, water: { item, name: "Camp Slough" }, createdAt: 0 });

  it("only the absorbed ids are renames: a current id, a gone id and no id are left alone, each id asked once", async () => {
    const asked: string[] = [];
    const s = source();
    const spy = { currentItem: (i: ItemId) => { asked.push(i); return s.currentItem!(i); } };
    const got = await savedItemRenames(spy, ["wbk:328965685", "gnis:17795", "wbk:0", null, undefined, "wbk:328965685"]);
    expect([...got]).toEqual([["wbk:328965685", "gnis:17795"]]);
    expect(asked).toEqual(["wbk:328965685", "gnis:17795", "wbk:0"]);
  });
  it("a source with no aliases (a fixture) renames nothing", async () => {
    expect((await savedItemRenames({}, ["wbk:328965685"])).size).toBe(0);
  });
  it("a spot takes the id in use and keeps everything else; an untouched spot is the SAME object (no write)", async () => {
    const renames = await savedItemRenames(source(), ["wbk:328965685"]);
    const old = spot("wbk:328965685"), cur = spot("gnis:17795"), none = spot(null);
    expect(spotWithCurrentItem(old, renames)).toEqual({ ...old, item: "gnis:17795" });
    expect(spotWithCurrentItem(cur, renames)).toBe(cur);
    expect(spotWithCurrentItem(none, renames)).toBe(none);
  });
  it("an alert's water takes the id in use; settings with nothing absorbed are the SAME object (no write)", async () => {
    const renames = await savedItemRenames(source(), ["wbk:328965685"]);
    const s = { ...NOTIFY_DEFAULT, rules: [rule("a", "wbk:328965685"), rule("b", "gnis:17795"), rule("c", null)] } as typeof NOTIFY_DEFAULT;
    const out = alertsWithCurrentItems(renames)(s);
    expect(out.rules.map((r) => r.water?.item)).toEqual(["gnis:17795", "gnis:17795", null]);
    expect(out.rules[1]).toBe(s.rules[1]);
    const same = { ...NOTIFY_DEFAULT, rules: [rule("b", "gnis:17795")] } as typeof NOTIFY_DEFAULT;
    expect(alertsWithCurrentItems(renames)(same)).toBe(same);
  });
  it("followSavedItems: one pass rewrites the absorbed spot and alert, and a second pass writes nothing", async () => {
    let spots = [spot("wbk:328965685"), spot("gnis:17795")];
    let state = { ...NOTIFY_DEFAULT, rules: [rule("a", "wbk:328965685")] } as typeof NOTIFY_DEFAULT;
    let spotWrites = 0, alertWrites = 0;
    const kept = {
      spots: { list: async () => spots,
               rewrite: async (fill: (s: Spot) => Promise<Spot>) => { spotWrites++; spots = await Promise.all(spots.map(fill)); } },
      alerts: { read: () => state, update: (fn: (s: typeof state) => typeof state) => { alertWrites++; state = fn(state); } },
    };
    expect(await followSavedItems(source(), kept)).toBe(1);
    expect(spots.map((s) => s.item)).toEqual(["gnis:17795", "gnis:17795"]);
    expect(state.rules[0]!.water).toEqual({ item: "gnis:17795", name: "Camp Slough" });
    expect(await followSavedItems(source(), kept)).toBe(0);
    expect([spotWrites, alertWrites]).toEqual([1, 1]);
  });
  it("followSavedItems on the web (no alerts) and with nothing kept writes nothing", async () => {
    const kept = { spots: { list: async () => [], rewrite: async () => { throw new Error("no write expected"); } }, alerts: null };
    expect(await followSavedItems(source(), kept)).toBe(0);
  });
});
