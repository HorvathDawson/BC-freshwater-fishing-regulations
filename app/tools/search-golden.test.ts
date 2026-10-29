/**
 * (T247) SEARCH ON THE REAL RELEASE: golden queries (a typo, a management unit, a region, a town, a duplicate name) must
 * put the right water first, and a keystroke must stay inside its time budget on ~20,000 waters.
 *
 * DATA, so Tier 2: set SEARCH_CORE to an uncompressed `core` sqlite (a release's, gunzipped; to try management units and
 * regions it must hold `item_area`, which releases cut from T247 on do) — or RELEASE_DIR to a data/release/<r> folder.
 *   SEARCH_CORE=/path/core.sqlite pnpm -C app exec vitest run search-golden
 * Without either the suite says so and checks nothing (it cannot run in CI, which holds no data).
 */
import { describe, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { areaPhrase, buildSearchIndex, describeBy, queryIndex, type IndexWater, type NearPlace, type SearchIndexData } from "@app/core";
import { CROSS } from "../packages/data/src/release/queries";

const ROOT = resolve(__dirname, "../..");
function corePath(): string | null {
  if (process.env.SEARCH_CORE) return resolve(ROOT, process.env.SEARCH_CORE);
  if (!process.env.RELEASE_DIR) return null;
  const dir = resolve(ROOT, process.env.RELEASE_DIR);
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
  const p = join(mkdtempSync(join(tmpdir(), "search-golden-")), "core.sqlite");
  writeFileSync(p, gunzipSync(readFileSync(join(dir, "..", m.files.core.url))));
  return p;
}
const path = corePath();
const db = path ? new DatabaseSync(path, { readOnly: true }) : null;
type Row = Record<string, unknown>;
const all = (sql: string): Row[] => db!.prepare(sql).all() as Row[];
const has = (t: string) => !!db!.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(t);

/** The same reads `WaterSource.searchIndex` makes (release/source.ts), through node:sqlite. */
function data(): SearchIndexData {
  const near = new Map<number, NearPlace[]>();
  if (has("item_near")) for (const r of all(CROSS.INDEX_NEAR.sql)) {
    const l = near.get(Number(r.ord)) ?? [];
    l.push({ name: String(r.name), kind: String(r.kind), lat: Number(r.lat), lon: Number(r.lon), km: Number(r.ckm) / 100 });
    near.set(Number(r.ord), l);
  }
  const area = new Map(has("item_area") ? all(CROSS.INDEX_AREA.sql).map((r) => [Number(r.ord), r]) : []);
  const csv = (v: unknown) => (v ? String(v).split(",") : []);
  return {
    waters: all(CROSS.INDEX_ITEMS.sql).map((r): IndexWater => ({
      item: String(r.item_id), name: String(r.name), kind: String(r.kind), size: r.size == null ? null : Number(r.size),
      areaHa: r.area_ha == null ? null : Number(r.area_ha), pieces: Number(r.pieces),
      regions: csv(area.get(Number(r.ord))?.regions), mus: csv(area.get(Number(r.ord))?.mus),
      near: describeBy(near.get(Number(r.ord)) ?? []) })),
    aliases: all(CROSS.INDEX_ALIASES.sql).map((r) => ({ item: String(r.item_id), alias: String(r.alias), source: null })),
    places: all(CROSS.INDEX_PLACES.sql).map((r) => ({ place: String(r.place_id), name: String(r.name), kind: String(r.kind),
                                                     lat: Number(r.lat), lon: Number(r.lon), pop: r.pop == null ? null : Number(r.pop) })),
  };
}

describe.skipIf(!db)("search on the real release (T247)", () => {
  const d = db ? data() : null;
  const ix = d ? buildSearchIndex(d) : null;
  const top = (q: string, n = 5) => queryIndex(ix!, q, n).waters.map((w) => `${w.name} [${areaPhrase(w.regions, w.mus)}]${w.matchedAs ? ` (as ${w.matchedAs})` : ""}`);
  const hasArea = !!db && has("item_area");

  const first = (q: string) => queryIndex(ix!, q, 5).waters[0]?.name;
  it("a typo puts the right water first", () => {
    expect([first("chilliwack river"), first("chillwack"), first("chiliwack riv"), first("kootney"), first("fraser rivr"),
            first("harrisson"), first("vedder")])
      .toEqual(["Chilliwack River", "Chilliwack River", "Chilliwack River", "Kootenay River", "Fraser River", "Harrison Lake",
                "Vedder River"]);
  });
  it("an alias finds its water, and says it was the alias", () => {
    const r = queryIndex(ix!, "vedder river").waters[0]!;
    expect(r.name === "Vedder River" || r.matchedAs !== null).toBe(true);
  });
  it("two waters of one name sit together, each with its own region", () => {
    const elks = queryIndex(ix!, "elk river").waters.filter((w) => w.name === "Elk River");
    expect(elks.length).toBeGreaterThanOrEqual(2);
    if (hasArea) expect(new Set(elks.map((w) => w.regions.join(","))).size).toBe(elks.length);
  });
  it("a town: near Hope lists waters around Hope, with the town itself", () => {
    const r = queryIndex(ix!, "near hope");
    expect(r.places[0]?.name).toBe("Hope");
    expect(r.waters.length).toBeGreaterThan(3);
  });
  it.skipIf(!hasArea)("a management unit lists exactly the waters in it; a region lists the waters of it; both narrow a name", () => {
    const mu = queryIndex(ix!, "2-4", 5000);
    expect(mu.area).toEqual({ kind: "mu", code: "2-4" });
    expect(mu.waters.length).toBeGreaterThan(10);
    expect(mu.waters.every((w) => w.mus.includes("2-4"))).toBe(true);
    expect(mu.waters.map((w) => w.name)).toContain("Vedder River");
    const rg = queryIndex(ix!, "region 2", 5000);
    expect(rg.waters.every((w) => w.regions.includes("2"))).toBe(true);
    expect(queryIndex(ix!, "elk 4-2").waters.map((w) => w.name)).toEqual(["Elk River"]);
    expect(queryIndex(ix!, "elk region 1").waters.every((w) => w.regions.includes("1"))).toBe(true);
  });

  it("per-keystroke time on the real index", () => {
    const qs = ["ch", "chi", "chil", "chill", "chilli", "chilliw", "chilliwa", "chilliwack", "creek", "lake", "river", "2-4", "region 2", "near hope", "fraser rivr"];
    const times = qs.map((q) => { const t = performance.now(); queryIndex(ix!, q); return performance.now() - t; });
    console.log(`real index: ${ix!.waters.length} waters, ${ix!.entries} names, ${ix!.places.length} towns; ms per query: ` +
                qs.map((q, i) => `${q}=${times[i]!.toFixed(1)}`).join(" "));
    expect(Math.max(...times)).toBeLessThan(100);
  });
  void hasArea;
});
